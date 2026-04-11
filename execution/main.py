import os
import time
import logging
from typing import Optional, Dict, Any

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ENV LOADING — python-dotenv
# override=False → Render ENV პრიორიტეტულია .env-ზე
# ე.ი. Render-ზე დაყენებული ცვლადი ყოველთვის იმარჯვებს
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
try:
    from dotenv import load_dotenv
    load_dotenv("/opt/render/project/src/.env", override=False)
except ImportError:
    pass  # python-dotenv არ არის — Render ENV კმარა

from execution.regime_engine import MarketRegimeEngine

from execution.db.db import init_db
from execution.db.repository import (
    get_system_state,
    update_system_state,
    log_event,
    get_trade_stats,
    get_closed_trades,
    close_trade,
    get_open_trade_for_symbol,
    reset_consecutive_sl_per_symbol,
)
from execution.execution_engine import ExecutionEngine
from execution.signal_client import pop_next_signal
from execution.kill_switch import is_kill_switch_active
from execution.dca_position_manager import get_dca_manager
from execution.dca_tp_sl_manager import get_tp_sl_manager, DCATpSlManager
from execution.dca_risk_manager import get_risk_manager
from execution.telegram_notifier import (
    notify_performance_snapshot,
    build_daily_stats_from_closed_trades,
    notify_daily_close_summary,
    notify_dca_addon,
    notify_dca_closed,
    notify_dca_breakeven,
    _now_dt,
)

logger = logging.getLogger("gbm")

# SIGNAL_EXPIRATION_SECONDS — outbox-დან წამოღებული ძველი signal-ი → skip
_SIGNAL_EXPIRATION_SECONDS = 0  # DCA: disabled


def _bootstrap_state_if_needed() -> None:
    raw = get_system_state()
    if raw is None or len(raw) < 5:
        logger.warning("BOOTSTRAP_STATE | system_state row missing or invalid -> skip")
        return

    status = str(raw[1] or "").upper()
    startup_sync_ok = int(raw[2] or 0)
    kill_switch_db = int(raw[3] or 0)

    env_kill = os.getenv("KILL_SWITCH", "false").lower() == "true"

    logger.info(
        f"BOOTSTRAP_STATE | status={status} startup_sync_ok={startup_sync_ok} "
        f"kill_db={kill_switch_db} env_kill={env_kill}"
    )

    if env_kill or kill_switch_db == 1:
        logger.warning("BOOTSTRAP_STATE | kill switch ON -> skip overrides")
        return

    if status == "PAUSED" or startup_sync_ok == 0:
        logger.warning("BOOTSTRAP_STATE | applying self-heal -> status=RUNNING startup_sync_ok=1 kill_switch=0")
        update_system_state(status="RUNNING", startup_sync_ok=1, kill_switch=0)


def _try_import_generator():
    try:
        from execution.signal_generator import run_once as generate_once
        return generate_once
    except Exception as e:
        logger.error(f"GENERATOR_IMPORT_FAIL | err={e} -> generator disabled (consumer will still run)")
        try:
            log_event("GENERATOR_IMPORT_FAIL", f"err={e}")
        except Exception:
            pass
        return None


def _safe_pop_next_signal(outbox_path: str) -> Optional[Dict[str, Any]]:
    try:
        return pop_next_signal(outbox_path)
    except Exception as e:
        logger.exception(f"OUTBOX_POP_FAIL | path={outbox_path} err={e}")
        try:
            log_event("OUTBOX_POP_FAIL", f"path={outbox_path} err={e}")
        except Exception:
            pass
        return None


def _run_performance_report_safe(send_telegram: bool = False) -> None:
    try:
        s = get_trade_stats()
        logger.info(
            "PERF_REPORT | closed=%s wins=%s losses=%s winrate=%.2f%% roi=%.2f%% pnl=%.4f quote_in=%.4f pf=%.3f | open=%s open_quote_in=%.4f",
            s.get("closed_trades", 0),
            s.get("wins", 0),
            s.get("losses", 0),
            float(s.get("winrate_pct", 0.0)),
            float(s.get("roi_pct", 0.0)),
            float(s.get("pnl_quote_sum", 0.0)),
            float(s.get("quote_in_sum", 0.0)),
            float(s.get("profit_factor", 0.0)),
            s.get("open_trades", 0),
            float(s.get("open_quote_in_sum", 0.0)),
        )

        try:
            log_event(
                "PERF_REPORT",
                f"closed={s.get('closed_trades', 0)} "
                f"winrate={float(s.get('winrate_pct', 0.0)):.2f}% "
                f"roi={float(s.get('roi_pct', 0.0)):.2f}% "
                f"pnl={float(s.get('pnl_quote_sum', 0.0)):.4f} "
                f"open={s.get('open_trades', 0)} "
                f"open_quote_in={float(s.get('open_quote_in_sum', 0.0)):.4f}"
            )
        except Exception:
            pass

        if send_telegram:
            try:
                notify_performance_snapshot(s)
            except Exception as e:
                logger.warning(f"TG_NOTIFY_PERF_FAIL | err={e}")

    except Exception as e:
        logger.warning(f"PERF_REPORT_FAIL | err={e}")



def _run_dca_loop(engine, dca_mgr, tp_sl_mgr, risk_mgr) -> None:
    """
    DCA monitoring loop — ყოველ main loop iteration-ზე გამოიძახება.

    შეამოწმებს:
      1. TP hit → close position
      2. Breakeven → SL გადაადგილება
      3. Force close → max drawdown ან max add-ons + SL
      4. SL confirmed → close position
      5. Add-on → drawdown trigger + recovery signals
    """
    from execution.db.repository import (
        get_all_open_dca_positions,
        get_all_open_trades,
        close_dca_position,
        update_dca_position_after_addon,
        update_dca_sl_price,
        add_dca_order,
        open_dca_position,
    )
    from execution.dca_position_manager import recalculate_average, score_recovery_signals

    open_positions = get_all_open_dca_positions()
    if not open_positions:
        return

    for pos in open_positions:
        sym = pos["symbol"]
        pos_id = pos["id"]

        # Layer 2 symbol — "_L2" suffix ამოვიღოთ exchange call-ებისთვის
        # DB-ში "BTC/USDT_L2" ინახება, Binance-ს "BTC/USDT" სჭირდება
        exchange_sym = sym.replace("_L2", "") if sym.endswith("_L2") else sym
        is_layer2 = sym.endswith("_L2")

        try:
            # current price
            try:
                current_price = engine.exchange.fetch_last_price(exchange_sym)
                current_price = float(current_price) if current_price else 0.0
            except Exception as _pe:
                logger.warning(f"[DCA] PRICE_FETCH_ERR | {sym} err={_pe}")
                current_price = 0.0

            if current_price <= 0:
                logger.warning(f"[DCA] NO_PRICE | {sym}")
                continue

            avg_entry    = float(pos["avg_entry_price"] or 0)
            tp_price     = float(pos["current_tp_price"] or 0)
            sl_price     = 0.0  # DCA: SL გათიშულია — ყოველთვის 0
            total_qty    = float(pos["total_qty"] or 0)
            total_quote  = float(pos["total_quote_spent"] or 0)
            add_on_count = int(pos["add_on_count"] or 0)

            logger.info(
                f"[DCA] MONITOR | {sym} price={current_price:.4f} "
                f"avg={avg_entry:.4f} tp={tp_price:.4f} "
                f"qty={total_qty:.6f} add_ons={add_on_count}"
            )

            # ── 1. TP hit ────────────────────────────────────────────────
            if tp_price > 0 and current_price >= tp_price:
                logger.info(f"[DCA] TP_HIT | {sym} price={current_price:.4f} >= tp={tp_price:.4f}")
                try:
                    sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                    exit_price = float(sell.get("average") or sell.get("price") or current_price)
                    pnl_quote = (exit_price - avg_entry) * total_qty
                    pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0

                    # ── dca_positions დახურვა ──────────────────────────
                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "TP")

                    # ── FIX: trades ცხრილის დახურვა ───────────────────
                    _open_tr = get_open_trade_for_symbol(exchange_sym)
                    if _open_tr:
                        close_trade(_open_tr[0], exit_price, "TP", pnl_quote, pnl_pct)
                        logger.info(f"[DCA] TRADE_CLOSED_TP | {sym} signal_id={_open_tr[0]} pnl={pnl_quote:+.4f}")
                    else:
                        logger.warning(f"[DCA] TRADE_NOT_FOUND | {sym} — trades row missing on TP")

                    # ── SL cooldown reset (TP = recovery) ─────────────
                    try:
                        reset_consecutive_sl_per_symbol(sym)
                    except Exception as _e:
                        logger.warning(f"[DCA] SL_RESET_FAIL | {sym} err={_e}")

                    try:
                        log_event("DCA_CLOSED_TP", f"sym={sym} exit={exit_price:.4f} pnl={pnl_quote:+.4f} pct={pnl_pct:.3f}%")
                    except Exception:
                        pass

                    from execution.telegram_notifier import notify_dca_closed
                    from execution.db.repository import get_trade_stats
                    stats = get_trade_stats()
                    notify_dca_closed(
                        sym, avg_entry, exit_price, total_qty, total_quote,
                        pnl_quote, pnl_pct, "TP", add_on_count, stats
                    )
                    logger.info(f"[DCA] CLOSED_TP | {sym} pnl={pnl_quote:+.4f}")
                except Exception as e:
                    logger.error(f"[DCA] TP_SELL_FAIL | {sym} err={e}")
                continue

            # ── 2. Force close check ─────────────────────────────────────
            force_close, fc_reason = tp_sl_mgr.should_force_close(pos, current_price)
            if force_close:
                logger.warning(f"[DCA] FORCE_CLOSE | {sym} reason={fc_reason}")
                try:
                    sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                    exit_price = float(sell.get("average") or sell.get("price") or current_price)
                    pnl_quote = (exit_price - avg_entry) * total_qty
                    pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0

                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "FORCE_CLOSE")

                    # ── FIX: trades ცხრილის დახურვა ───────────────────
                    _open_tr = get_open_trade_for_symbol(exchange_sym)
                    if _open_tr:
                        close_trade(_open_tr[0], exit_price, "FORCE_CLOSE", pnl_quote, pnl_pct)
                        logger.info(f"[DCA] TRADE_CLOSED_FC | {sym} signal_id={_open_tr[0]} pnl={pnl_quote:+.4f}")
                    else:
                        logger.warning(f"[DCA] TRADE_NOT_FOUND | {sym} — trades row missing on FORCE_CLOSE")

                    try:
                        log_event("DCA_FORCE_CLOSE", f"sym={sym} reason={fc_reason} exit={exit_price:.4f} pnl={pnl_quote:+.4f}")
                    except Exception:
                        pass

                    from execution.telegram_notifier import notify_dca_closed
                    notify_dca_closed(
                        sym, avg_entry, exit_price, total_qty, total_quote,
                        pnl_quote, pnl_pct, "FORCE_CLOSE", add_on_count
                    )
                except Exception as e:
                    logger.error(f"[DCA] FORCE_CLOSE_FAIL | {sym} err={e}")
                continue

            # ── 3. Fetch ohlcv for signal analysis ───────────────────────
            try:
                from execution.signal_generator import _fetch_ohlcv_direct
                tf = os.getenv("BOT_TIMEFRAME", "15m")
                ohlcv = _fetch_ohlcv_direct(exchange_sym, tf, 60)
            except Exception as e:
                logger.warning(f"[DCA] OHLCV_FAIL | {sym} err={e}")
                continue

            if not ohlcv or len(ohlcv) < 30:
                continue

            # ── 4. SL confirmed → close ──────────────────────────────────
            if sl_price > 0 and current_price < sl_price:
                sl_confirmed, sl_reason = tp_sl_mgr.is_sl_confirmed(sl_price, ohlcv)
                if sl_confirmed:
                    logger.info(f"[DCA] SL_CONFIRMED | {sym} reason={sl_reason}")
                    try:
                        sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                        exit_price = float(sell.get("average") or sell.get("price") or current_price)
                        pnl_quote = (exit_price - avg_entry) * total_qty
                        pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0

                        close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "SL")

                        # ── FIX: trades ცხრილის დახურვა ───────────────
                        _open_tr = get_open_trade_for_symbol(exchange_sym)
                        if _open_tr:
                            close_trade(_open_tr[0], exit_price, "SL", pnl_quote, pnl_pct)
                            logger.info(f"[DCA] TRADE_CLOSED_SL | {sym} signal_id={_open_tr[0]} pnl={pnl_quote:+.4f}")
                        else:
                            logger.warning(f"[DCA] TRADE_NOT_FOUND | {sym} — trades row missing on SL")

                        try:
                            from execution.db.repository import increment_consecutive_sl_per_symbol
                            increment_consecutive_sl_per_symbol(sym)
                        except Exception as _e:
                            logger.warning(f"[DCA] SL_INCREMENT_FAIL | {sym} err={_e}")

                        try:
                            log_event("DCA_CLOSED_SL", f"sym={sym} reason={sl_reason} exit={exit_price:.4f} pnl={pnl_quote:+.4f}")
                        except Exception:
                            pass

                        from execution.telegram_notifier import notify_dca_closed
                        notify_dca_closed(
                            sym, avg_entry, exit_price, total_qty, total_quote,
                            pnl_quote, pnl_pct, "SL", add_on_count
                        )
                    except Exception as e:
                        logger.error(f"[DCA] SL_SELL_FAIL | {sym} err={e}")
                    continue
                else:
                    logger.info(f"[DCA] SL_NOT_CONFIRMED | {sym} reason={sl_reason}")

            # ── 5. Breakeven check ───────────────────────────────────────
            be_update, new_sl = tp_sl_mgr.check_breakeven(avg_entry, current_price, sl_price)
            if be_update:
                update_dca_sl_price(pos_id, new_sl)
                from execution.telegram_notifier import notify_dca_breakeven
                notify_dca_breakeven(sym, avg_entry, sl_price, new_sl)
                sl_price = new_sl

            # ── 6. Add-on check ──────────────────────────────────────────
            all_positions = get_all_open_dca_positions()
            addon_ok, addon_reason = dca_mgr.should_add_on(pos, current_price, ohlcv)

            if not addon_ok:
                logger.debug(f"[DCA] NO_ADDON | {sym} reason={addon_reason}")
                continue

            risk_ok, risk_reason = risk_mgr.can_add_on(pos, dca_mgr.get_addon_size(add_on_count), all_positions)
            if not risk_ok:
                logger.info(f"[DCA] ADDON_RISK_BLOCK | {sym} reason={risk_reason}")
                continue

            # place add-on order
            addon_size = dca_mgr.get_addon_size(add_on_count)
            drawdown_pct = (avg_entry - current_price) / avg_entry * 100.0
            score, score_details = score_recovery_signals(ohlcv)

            logger.info(
                f"[DCA] PLACING_ADDON | {sym} level={add_on_count+1} "
                f"size={addon_size} drawdown={drawdown_pct:.2f}% score={score}/5"
            )

            try:
                buy = engine.exchange.place_market_buy_by_quote(sym, addon_size)
                buy_price = float(buy.get("average") or buy.get("price") or current_price)
                buy_qty   = addon_size / buy_price

                avg_result = recalculate_average(total_qty, avg_entry, buy_qty, buy_price)
                new_avg    = avg_result["avg_entry_price"]
                new_qty    = avg_result["total_qty"]
                new_quote  = total_quote + addon_size

                tp_sl = tp_sl_mgr.calculate(new_avg)
                new_tp = tp_sl["tp_price"]
                new_sl = tp_sl["sl_price"]

                # DB update
                update_dca_position_after_addon(
                    pos_id,
                    new_avg_entry=new_avg,
                    new_total_qty=new_qty,
                    new_total_quote=new_quote,
                    new_add_on_count=add_on_count + 1,
                    new_tp_price=new_tp,
                    new_sl_price=new_sl,
                    last_add_on_ts=time.time(),
                )

                closes = [float(c[4]) for c in ohlcv]
                rsi_val = score_details.get("rsi", 0.0)
                atr_val = score_details.get("atr_pct", 0.0)

                add_dca_order(
                    position_id=pos_id,
                    symbol=sym,
                    order_type=f"ADD_ON_{add_on_count + 1}",
                    entry_price=buy_price,
                    qty=buy_qty,
                    quote_spent=addon_size,
                    avg_entry_after=new_avg,
                    tp_after=new_tp,
                    sl_after=new_sl,
                    trigger_drawdown_pct=drawdown_pct,
                    rsi_at_entry=rsi_val,
                    atr_pct_at_entry=atr_val,
                    recovery_score=score,
                    exchange_order_id=str(buy.get("id", "")),
                )

                notify_dca_addon(
                    symbol=sym,
                    addon_number=add_on_count + 1,
                    addon_price=buy_price,
                    addon_quote=addon_size,
                    new_avg_entry=new_avg,
                    total_quote_spent=new_quote,
                    new_tp_price=new_tp,
                    new_sl_price=new_sl,
                    drawdown_pct=drawdown_pct,
                    recovery_score=score,
                )

                logger.info(
                    f"[DCA] ADDON_PLACED | {sym} level={add_on_count+1} "
                    f"price={buy_price:.4f} new_avg={new_avg:.4f} "
                    f"tp={new_tp:.4f} sl={new_sl:.4f}"
                )

            except Exception as e:
                logger.error(f"[DCA] ADDON_PLACE_FAIL | {sym} err={e}")

        except Exception as e:
            logger.warning(f"[DCA] POSITION_LOOP_ERR | {sym} id={pos_id} err={e}")


def _check_and_open_layer2(engine, tp_sl_mgr) -> None:
    """
    Layer 2 — Crash Detection & Parallel Trading.

    ლოგიკა:
      1. თითო symbol-ისთვის 24h HIGH ამოიღე
      2. თუ current_price <= HIGH × (1 - LAYER2_DROP_PCT/100) → crash!
      3. Layer 2 პოზიცია უკვე ღიაა? → გამოტოვე
      4. USDT ბალანსი საკმარისია? → გახსენი Layer 2

    ENV:
      LAYER2_DROP_PCT=5.0      ← HIGH-დან რამდენი % ვარდნაზე გაიხსნოს
      LAYER2_ENABLED=true      ← ჩართვა/გამორთვა
      LAYER2_QUOTE=10.0        ← Layer 2-ის trade ზომა
      LAYER2_SYMBOLS=BTC/USDT,BNB/USDT,ETH/USDT
    """
    import os

    # Layer 2 ჩართულია?
    if not os.getenv("LAYER2_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return

    from execution.db.repository import (
        get_open_dca_position_for_symbol,
        open_dca_position,
        add_dca_order,
        open_trade,
        log_event,
    )

    drop_pct    = float(os.getenv("LAYER2_DROP_PCT",  "5.0"))
    quote       = float(os.getenv("LAYER2_QUOTE",     "10.0"))
    symbols_raw = os.getenv("LAYER2_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols     = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    tp_pct      = float(os.getenv("DCA_TP_PCT", "0.55"))
    buffer      = float(os.getenv("SMART_ADDON_BUFFER", "5.0"))

    # USDT ბალანსი
    try:
        free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
    except Exception as _e:
        logger.warning(f"[LAYER2] balance_fetch_fail | err={_e}")
        return

    for sym in symbols:
        exchange_sym = sym  # Layer2: symbols სუფთაა (_L2 suffix არ აქვს)
        try:
            # current price
            current_price = float(engine.exchange.fetch_last_price(exchange_sym) or 0.0)
            if current_price <= 0:
                continue

            # 24h HIGH — ohlcv 1d candle
            try:
                ticker = engine.price_feed.fetch_ticker(sym)
                high_24h = float(ticker.get("high") or ticker.get("info", {}).get("highPrice") or 0.0)
            except Exception:
                high_24h = 0.0

            if high_24h <= 0:
                logger.debug(f"[LAYER2] NO_HIGH | {sym} → skip")
                continue

            drop_from_high = (high_24h - current_price) / high_24h * 100.0

            logger.info(
                f"[LAYER2] CHECK | {sym} price={current_price:.4f} "
                f"high24h={high_24h:.4f} drop={drop_from_high:.2f}% "
                f"trigger={drop_pct:.1f}%"
            )

            # crash trigger?
            if drop_from_high < drop_pct:
                logger.debug(f"[LAYER2] NO_CRASH | {sym} drop={drop_from_high:.2f}% < {drop_pct:.1f}%")
                continue

            # Layer 2 უკვე ღიაა ამ symbol-ზე?
            # Layer 2 პოზიციები tag-ით განვასხვავებთ: symbol = "BTC/USDT_L2"
            sym_l2 = f"{sym}_L2"
            existing_l2 = get_open_dca_position_for_symbol(sym_l2)
            if existing_l2:
                logger.debug(f"[LAYER2] ALREADY_OPEN | {sym_l2}")
                continue

            # ბალანსი საკმარისია?
            required = quote + buffer
            if free_usdt < required:
                logger.warning(
                    f"[LAYER2] INSUFFICIENT_BALANCE | {sym} "
                    f"free={free_usdt:.2f} < required={required:.2f}"
                )
                continue

            # Layer 2 გახსნა!
            logger.warning(
                f"[LAYER2] CRASH_DETECTED | {sym} "
                f"drop={drop_from_high:.2f}% >= {drop_pct:.1f}% → opening Layer 2"
            )

            # ყიდვა
            buy = engine.exchange.place_market_buy_by_quote(sym, quote)
            buy_price = float(buy.get("average") or buy.get("price") or current_price)
            buy_qty   = quote / buy_price

            tp_price = round(buy_price * (1.0 + tp_pct / 100.0), 6)

            # dca_positions — sym_l2 tag-ით
            pos_id = open_dca_position(
                symbol=sym_l2,
                initial_entry_price=buy_price,
                initial_qty=buy_qty,
                initial_quote_spent=quote,
                tp_price=tp_price,
                sl_price=0.0,
                tp_pct=tp_pct,
                sl_pct=999.0,
                max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "1")),
                max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "20.0")),
                max_drawdown_pct=999.0,
            )

            add_dca_order(
                position_id=pos_id,
                symbol=sym_l2,
                order_type="LAYER2_INITIAL",
                entry_price=buy_price,
                qty=buy_qty,
                quote_spent=quote,
                avg_entry_after=buy_price,
                tp_after=tp_price,
                sl_after=0.0,
                trigger_drawdown_pct=drop_from_high,
                exchange_order_id=str(buy.get("id", "")),
            )

            # trades ცხრილი
            from execution.db.repository import mark_signal_id_executed
            import uuid
            l2_signal_id = f"L2-{sym.replace('/', '')}-{uuid.uuid4().hex[:8]}"
            open_trade(
                signal_id=l2_signal_id,
                symbol=sym_l2,
                qty=buy_qty,
                quote_in=quote,
                entry_price=buy_price,
            )

            free_usdt -= quote  # ბალანსი განახლება in-memory

            try:
                log_event(
                    "LAYER2_OPENED",
                    f"sym={sym_l2} entry={buy_price:.4f} "
                    f"tp={tp_price:.4f} drop={drop_from_high:.2f}% "
                    f"pos_id={pos_id}"
                )
            except Exception:
                pass

            # Telegram
            try:
                from execution.telegram_notifier import notify_signal_created
                notify_signal_created(
                    symbol=sym_l2,
                    entry_price=buy_price,
                    quote_amount=quote,
                    tp_price=tp_price,
                    sl_price=0.0,
                    verdict="LAYER2_BUY",
                    mode=engine.mode,
                )
            except Exception as _tg:
                logger.warning(f"[LAYER2] TG_FAIL | {sym} err={_tg}")

            logger.warning(
                f"[LAYER2] OPENED | {sym_l2} entry={buy_price:.4f} "
                f"tp={tp_price:.4f} qty={buy_qty:.6f}"
            )

        except Exception as e:
            logger.error(f"[LAYER2] ERR | {sym} err={e}")


def _check_cascade_exchange(engine, tp_sl_mgr) -> None:
    """
    Cascade DCA — "Rolling Exchange" სტრატეგია.

    ლოგიკა:
      - სულ გახსნილი Layer-ების რაოდენობა >= CASCADE_START_LAYER (default=7)?
      - ყოველ symbol-ზე: ყველაზე ძველი Layer გამოვავლინოთ
      - current_price <= oldest_layer_avg × (1 - CASCADE_DROP_PCT/100)?
      - Exchange: ძველი Layer დავხუროთ (market sell) → ახალი Layer გავხსნათ
        გამოთავისუფლებული თანხით (exchange_proceeds)
      - Layer რაოდენობა >= CASCADE_MAX_LAYERS (default=10)?
        → გაჩერება, CASCADE_RESUME_LAYER (default=16)-მდე ლოდინი

    ENV:
      CASCADE_ENABLED=true
      CASCADE_START_LAYER=7       ← მე-7 სვლიდან იწყება
      CASCADE_DROP_PCT=1.5        ← იგივე Layer2-ის trigger
      CASCADE_MAX_LAYERS=10       ← მე-10-ზე გაჩერება
      CASCADE_RESUME_LAYER=16     ← მე-16-ზე გახსნა
      CASCADE_SYMBOLS=BTC/USDT,BNB/USDT,ETH/USDT
    """
    import os

    if not os.getenv("CASCADE_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return

    from execution.db.repository import (
        get_all_open_dca_positions,
        close_dca_position,
        open_dca_position,
        add_dca_order,
        open_trade,
        get_open_trade_for_symbol,
        close_trade,
        log_event,
    )

    cascade_start  = int(os.getenv("CASCADE_START_LAYER",  "7"))
    drop_pct       = float(os.getenv("CASCADE_DROP_PCT",   "1.5"))
    max_layers     = int(os.getenv("CASCADE_MAX_LAYERS",   "10"))
    resume_layer   = int(os.getenv("CASCADE_RESUME_LAYER", "16"))
    symbols_raw    = os.getenv("CASCADE_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols        = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    tp_pct         = float(os.getenv("DCA_TP_PCT", "0.55"))
    buffer         = float(os.getenv("SMART_ADDON_BUFFER", "5.0"))

    # ყველა ღია პოზიცია
    all_positions = get_all_open_dca_positions()

    # სულ რამდენი Layer გვაქვს (Layer1 + Layer2 + _L2 + _L3 ...)
    total_layers = len(all_positions)

    logger.info(
        f"[CASCADE] CHECK | total_layers={total_layers} "
        f"start_at={cascade_start} max={max_layers} resume_at={resume_layer}"
    )

    # Cascade ჯერ არ დაწყებულა?
    if total_layers < cascade_start:
        logger.debug(f"[CASCADE] NOT_YET | {total_layers} < {cascade_start}")
        return

    # მე-10-ზე გაჩერება — resume_layer-ს ვეცდინოთ
    if total_layers >= max_layers:
        # resume_layer-ზე მიაღწია? — გახსენი
        if total_layers < resume_layer:
            logger.info(f"[CASCADE] PAUSED | {total_layers} >= {max_layers}, waiting for {resume_layer}")
            return
        else:
            logger.warning(f"[CASCADE] RESUMING | total_layers={total_layers} >= {resume_layer}")

    for sym in symbols:
        try:
            exchange_sym = sym

            # current price
            current_price = float(engine.exchange.fetch_last_price(exchange_sym) or 0.0)
            if current_price <= 0:
                continue

            # ამ symbol-ის ყველა Layer — გახსნის დროის მიხედვით დავალაგოთ
            sym_positions = [
                p for p in all_positions
                if str(p.get("symbol", "")).upper().replace("_L2", "").replace("_L3", "")
                   .replace("_L4", "").replace("_L5", "").replace("_L6", "")
                   .replace("_L7", "").replace("_L8", "").replace("_L9", "")
                   .replace("_L10", "") == sym.upper()
            ]

            if len(sym_positions) < 2:
                logger.debug(f"[CASCADE] {sym} | only {len(sym_positions)} layer(s) → skip")
                continue

            # ყველაზე ძველი Layer — opened_at მიხედვით
            oldest = sorted(sym_positions, key=lambda p: str(p.get("opened_at", "")))[0]
            oldest_avg   = float(oldest.get("avg_entry_price", 0.0))
            oldest_qty   = float(oldest.get("total_qty", 0.0))
            oldest_quote = float(oldest.get("total_quote_spent", 0.0))
            oldest_id    = oldest["id"]
            oldest_sym   = oldest["symbol"]

            if oldest_avg <= 0 or oldest_qty <= 0:
                continue

            # FIX: trigger ვზომავთ ყველაზე ახალი Layer-ის avg-დან
            # ანუ: Layer 2 გახსნიდან კიდევ -1.5% → CASCADE იწყება
            # (არა oldest-იდან — ის Layer 2-ის trigger-თან ემთხვეოდა)
            newest = sorted(sym_positions, key=lambda p: str(p.get("opened_at", "")))[-1]
            newest_avg = float(newest.get("avg_entry_price", 0.0))
            if newest_avg <= 0:
                newest_avg = oldest_avg

            drop_from_newest = (newest_avg - current_price) / newest_avg * 100.0

            logger.info(
                f"[CASCADE] {sym} | oldest={oldest_sym} avg={oldest_avg:.4f} "
                f"newest_avg={newest_avg:.4f} price={current_price:.4f} "
                f"drop_from_newest={drop_from_newest:.2f}% trigger={drop_pct:.1f}%"
            )

            if drop_from_newest < drop_pct:
                logger.debug(f"[CASCADE] {sym} | drop={drop_from_newest:.2f}% < {drop_pct:.1f}% → wait")
                continue

            # ბალანსი შემოწმება (buffer საჭიროა)
            free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
            if free_usdt < buffer:
                logger.warning(f"[CASCADE] {sym} | low_balance={free_usdt:.2f} < buffer={buffer:.1f}")
                continue

            # ── Exchange: ძველი Layer-ის გაყიდვა ──────────────────────
            logger.warning(
                f"[CASCADE] EXCHANGE | {oldest_sym} avg={oldest_avg:.4f} "
                f"qty={oldest_qty:.6f} drop={drop_from_newest:.2f}%"
            )

            try:
                sell = engine.exchange.place_market_sell(exchange_sym, oldest_qty)
                sell_price = float(sell.get("average") or sell.get("price") or current_price)
                proceeds = sell_price * oldest_qty
                fee = proceeds * 0.001  # 0.1% fee
                net_proceeds = round(proceeds - fee, 4)

                pnl_quote = (sell_price - oldest_avg) * oldest_qty
                pnl_pct   = (sell_price / oldest_avg - 1.0) * 100.0

                # dca_positions დახურვა
                close_dca_position(
                    oldest_id, sell_price, oldest_qty,
                    pnl_quote, pnl_pct, "CASCADE_EXCHANGE"
                )

                # trades დახურვა
                open_tr = get_open_trade_for_symbol(oldest_sym)
                if open_tr:
                    close_trade(open_tr[0], sell_price, "CASCADE_EXCHANGE", pnl_quote, pnl_pct)

                logger.warning(
                    f"[CASCADE] SOLD | {oldest_sym} price={sell_price:.4f} "
                    f"proceeds={net_proceeds:.4f} pnl={pnl_quote:+.4f}"
                )

            except Exception as _se:
                logger.error(f"[CASCADE] SELL_FAIL | {oldest_sym} err={_se}")
                continue

            # ── ახალი Layer გახსნა net_proceeds-ით ───────────────────
            if net_proceeds < 5.0:
                logger.warning(f"[CASCADE] LOW_PROCEEDS | {net_proceeds:.4f} < $5 → skip new layer")
                continue

            # Layer ნომერი განვსაზღვროთ
            layer_num = len(sym_positions)  # მიმდინარე + 1
            new_sym = f"{sym}_L{layer_num + 1}"

            try:
                buy_quote = max(net_proceeds, 10.0)  # მინიმუმ $10
                buy = engine.exchange.place_market_buy_by_quote(exchange_sym, buy_quote)
                buy_price = float(buy.get("average") or buy.get("price") or current_price)
                buy_qty   = buy_quote / buy_price
                tp_price  = round(buy_price * (1.0 + tp_pct / 100.0), 6)

                # dca_positions გახსნა
                pos_id = open_dca_position(
                    symbol=new_sym,
                    initial_entry_price=buy_price,
                    initial_qty=buy_qty,
                    initial_quote_spent=buy_quote,
                    tp_price=tp_price,
                    sl_price=0.0,
                    tp_pct=tp_pct,
                    sl_pct=999.0,
                    max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "1")),
                    max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "20.0")),
                    max_drawdown_pct=999.0,
                )

                add_dca_order(
                    position_id=pos_id,
                    symbol=new_sym,
                    order_type="CASCADE_LAYER",
                    entry_price=buy_price,
                    qty=buy_qty,
                    quote_spent=net_proceeds,
                    avg_entry_after=buy_price,
                    tp_after=tp_price,
                    sl_after=0.0,
                    trigger_drawdown_pct=drop_from_newest,
                    exchange_order_id=str(buy.get("id", "")),
                )

                # trades გახსნა
                import uuid
                cascade_signal_id = f"CAS-{sym.replace('/', '')}-{uuid.uuid4().hex[:8]}"
                open_trade(
                    signal_id=cascade_signal_id,
                    symbol=new_sym,
                    qty=buy_qty,
                    quote_in=net_proceeds,
                    entry_price=buy_price,
                )

                try:
                    log_event(
                        "CASCADE_LAYER_OPENED",
                        f"sym={new_sym} entry={buy_price:.4f} tp={tp_price:.4f} "
                        f"quote={net_proceeds:.4f} from={oldest_sym}"
                    )
                except Exception:
                    pass

                # Telegram
                try:
                    from execution.telegram_notifier import notify_signal_created
                    notify_signal_created(
                        symbol=new_sym,
                        entry_price=buy_price,
                        quote_amount=net_proceeds,
                        tp_price=tp_price,
                        sl_price=0.0,
                        verdict="CASCADE_BUY",
                        mode=engine.mode,
                    )
                except Exception as _tg:
                    logger.warning(f"[CASCADE] TG_FAIL | err={_tg}")

                logger.warning(
                    f"[CASCADE] NEW_LAYER | {new_sym} entry={buy_price:.4f} "
                    f"tp={tp_price:.4f} quote={net_proceeds:.4f}"
                )

            except Exception as _be:
                logger.error(f"[CASCADE] BUY_FAIL | {new_sym} err={_be}")

        except Exception as e:
            logger.error(f"[CASCADE] ERR | {sym} err={e}")


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode = os.getenv("MODE", "DEMO").upper()
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
    sleep_s = float(os.getenv("LOOP_SLEEP_SECONDS", "10"))

    report_every_s = int(os.getenv("REPORT_EVERY_SECONDS", "60"))
    telegram_report_every_s = int(os.getenv("TELEGRAM_REPORT_EVERY_SECONDS", "1800"))

    last_report_ts = 0.0
    last_tg_report_ts = 0.0
    last_daily_summary_date = None

    init_db()
    _bootstrap_state_if_needed()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # LIVE DASHBOARD — background thread, port 8080
    # URL: https://your-render-url.onrender.com/dashboard
    # DASHBOARD_ENABLED=true ENV-ით ჩართვა/გამორთვა
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if os.getenv("DASHBOARD_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        try:
            from execution.dashboard import start_dashboard
            _dash_port = int(os.getenv("DASHBOARD_PORT", "8080"))
            start_dashboard(port=_dash_port)
        except Exception as _de:
            logger.warning(f"DASHBOARD_START_FAIL | err={_de}")

    engine = ExecutionEngine()

    # DCA MODE: reconcile_oco გათიშულია — OCO არ გამოიყენება
    # try:
    #     engine.reconcile_oco()
    # except Exception as e:
    #     logger.warning(f"OCO_RECONCILE_START_WARN | err={e}")

    generate_once = _try_import_generator()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FIX #3: MarketRegimeEngine — loop-გარეთ, ᲔᲠᲗᲘ instance სამუდამოდ
    # ძველი კოდი ყოველ ტიკზე ახალ instance-ს ქმნიდა → state იკარგებოდა
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    regime_engine = MarketRegimeEngine()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FIX C-1: inject_regime_engine — execution_engine-ს regime_engine
    # გადაეცემა, რათა TP/SL close-ზე in-memory SL counter სწორად reset-ს.
    # გარეშე: _regime_engine=None → notify_outcome() არასოდეს გამოიძახება
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    engine.inject_regime_engine(regime_engine)

    # DCA managers init
    # DCA MODE: DCA_ENABLED ENV-დან — default true (DCA ბოტია)
    _dca_enabled = os.getenv("DCA_ENABLED", "true").strip().lower() in ("1", "true", "yes")
    dca_mgr   = get_dca_manager()   if _dca_enabled else None
    tp_sl_mgr = get_tp_sl_manager() if _dca_enabled else None
    risk_mgr  = get_risk_manager()  if _dca_enabled else None
    if _dca_enabled:
        logger.info(f"DCA_ENABLED | max_add_ons={os.getenv('DCA_MAX_ADD_ONS', '3')} max_capital={os.getenv('DCA_MAX_CAPITAL_USDT', '40')}")

    logger.info(f"GENIUS BOT MAN worker starting | MODE={mode}")
    logger.info(f"OUTBOX_PATH={outbox_path}")
    logger.info(f"LOOP_SLEEP_SECONDS={sleep_s}")
    logger.info(f"REPORT_EVERY_SECONDS={report_every_s}")
    logger.info(f"TELEGRAM_REPORT_EVERY_SECONDS={telegram_report_every_s}")

    while True:
        try:
            if is_kill_switch_active():
                logger.warning("KILL_SWITCH_ACTIVE | worker will not generate/pop/execute signals")
                try:
                    log_event("WORKER_KILL_SWITCH_ACTIVE", "blocked before loop actions")
                except Exception:
                    pass
                time.sleep(sleep_s)
                continue

            # DCA MODE: reconcile_oco გათიშულია loop-შიც
            # try:
            #     engine.reconcile_oco()
            # except Exception as e:
            #     logger.warning(f"OCO_RECONCILE_LOOP_WARN | err={e}")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # DCA LOOP — add-on check + TP/SL + breakeven + force close
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _dca_enabled:
                try:
                    _run_dca_loop(engine, dca_mgr, tp_sl_mgr, risk_mgr)
                except Exception as e:
                    logger.warning(f"DCA_LOOP_WARN | err={e}")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # LAYER 2 — Crash detection & parallel trading
            # HIGH-დან -5% ვარდნაზე ახალი 3 პოზიცია გაიხსნება
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _dca_enabled and engine.exchange is not None:
                try:
                    _check_and_open_layer2(engine, tp_sl_mgr)
                except Exception as e:
                    logger.warning(f"LAYER2_CHECK_WARN | err={e}")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # CASCADE DCA — Rolling Exchange სტრატეგია
            # მე-7 Layer-დან: ძველი → Exchange → ახალი Layer დაბლა
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _dca_enabled and engine.exchange is not None:
                try:
                    _check_cascade_exchange(engine, tp_sl_mgr)
                except Exception as e:
                    logger.warning(f"CASCADE_CHECK_WARN | err={e}")

            if generate_once is not None:
                try:
                    created = generate_once(outbox_path)
                    if created:
                        logger.info("SIGNAL_GENERATOR | signal created")
                except Exception as e:
                    logger.exception(f"SIGNAL_GENERATOR_FAIL | err={e}")
                    try:
                        log_event("SIGNAL_GENERATOR_FAIL", f"err={e}")
                    except Exception:
                        pass

                sig = _safe_pop_next_signal(outbox_path)

                if sig:
                    signal_id = sig.get("signal_id", "UNKNOWN")
                    verdict = str(sig.get("final_verdict", "")).upper()

                    logger.info(f"Signal received | id={signal_id} | verdict={verdict}")

                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # SIGNAL_EXPIRATION_SECONDS — ძველი signal-ი → skip
                    # signal_generator-ი წერს sig["ts_utc"] (UTC ISO)
                    # თუ signal-ი outbox-ში SIGNAL_EXPIRATION_SECONDS-ზე
                    # მეტია → გამოტოვება (stale signal)
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    if _SIGNAL_EXPIRATION_SECONDS > 0:
                        try:
                            from datetime import datetime, timezone
                            ts_raw = sig.get("ts_utc", "")
                            if ts_raw:
                                sig_dt = datetime.fromisoformat(
                                    str(ts_raw).replace("Z", "+00:00")
                                )
                                if sig_dt.tzinfo is None:
                                    sig_dt = sig_dt.replace(tzinfo=timezone.utc)
                                age_s = (datetime.now(timezone.utc) - sig_dt).total_seconds()
                                if age_s > _SIGNAL_EXPIRATION_SECONDS:
                                    logger.warning(
                                        f"[EXPIRED] signal skipped | id={signal_id} "
                                        f"age={age_s:.0f}s > limit={_SIGNAL_EXPIRATION_SECONDS}s"
                                    )
                                    try:
                                        log_event(
                                            "SIGNAL_EXPIRED",
                                            f"id={signal_id} age={age_s:.0f}s verdict={verdict}"
                                        )
                                    except Exception:
                                        pass
                                    continue
                        except Exception as e:
                            logger.warning(f"EXPIRY_CHECK_FAIL | id={signal_id} err={e} → skip check")

                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    # FIX #2: SELL signal — regime check bypass
                    # SELL (TREND_REVERSAL / PROTECTIVE_SELL) ყოველთვის
                    # სრულდება, SKIP_TRADING-ი მას ვერ ბლოკავს.
                    # ძველი კოდი SELL-საც ჩერდებოდა SIDEWAYS-ზე!
                    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                    if verdict == "SELL":
                        source = sig.get("meta", {}).get("source", "UNKNOWN")
                        if source == "PROTECTIVE_SELL":
                            # crash guard — ATR EXTREME + KILL risk → გაყიდვა
                            logger.warning(
                                f"[AUTO] PROTECTIVE_SELL → executing | "
                                f"id={signal_id} source={source}"
                            )
                            engine.execute_signal(sig)
                        else:
                            # TREND_REVERSAL / RSI_OVERBOUGHT — DCA-ში ბლოკი
                            # DCA TP-ს ელოდება, არ გაყიდის ვარდნაზე
                            logger.info(
                                f"[AUTO] SELL blocked (DCA holds) | "
                                f"id={signal_id} source={source}"
                            )

                    elif verdict == "TRADE":
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        # GAP-2 FIX: main.py-ი აღარ ახდენს TP/SL-ის recalc-ს.
                        # signal_generator-მა უკვე გაითვლა adaptive (TP/SL + MTF bonus)
                        # და sig["adaptive"]-ში ჩაწერა.
                        # main.py-ი მხოლოდ SKIP safety-net-ია:
                        # თუ სიგნალის emit-სა და execution-ს შორის (20 წამი)
                        # ბაზარი BEAR/VOLATILE/SIDEWAYS-ად გადაბრუნდა → ბლოკავს.
                        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                        trend   = float(sig.get("trend",     0) or 0)
                        atr_pct = float(sig.get("atr_pct",   0) or 0)
                        symbol  = str((sig.get("execution") or {}).get("symbol", ""))

                        regime  = regime_engine.detect_regime(trend=trend, atr_pct=atr_pct)

                        # DCA MODE: regime block გათიშულია — ვაჭრობა ყველა რეჟიმში
                        # if regime in ("BEAR", "VOLATILE", "SIDEWAYS"): → disabled
                        logger.info(f"[AUTO] regime={regime} trend={trend:.3f} atr={atr_pct:.3f} → DCA mode, no block")

                        logger.info(
                            f"[AUTO] Regime={regime} trend={trend:.3f} "
                            f"atr_pct={atr_pct:.3f} symbol={symbol} "
                            f"TP={sig.get('adaptive', {}).get('TP_PCT', 'n/a')}% "
                            f"SL={sig.get('adaptive', {}).get('SL_PCT', 'n/a')}% "
                            f"mtf={sig.get('meta', {}).get('mtf_alignment', 'N/A')} "
                            f"| id={signal_id}"
                        )

                        engine.execute_signal(sig)

                    else:
                        # HOLD ან სხვა — უბრალოდ log
                        logger.info(f"[AUTO] Unsupported verdict={verdict} | id={signal_id} → skip")

                else:
                    logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")

            now = time.time()

            if report_every_s > 0 and (now - last_report_ts) >= report_every_s:
                _run_performance_report_safe(send_telegram=False)
                last_report_ts = now

            if telegram_report_every_s > 0 and (now - last_tg_report_ts) >= telegram_report_every_s:
                _run_performance_report_safe(send_telegram=True)
                last_tg_report_ts = now

            try:
                now_local = _now_dt()
                today_str = now_local.date().isoformat()

                if (
                    now_local.hour == 23
                    and now_local.minute == 59
                    and last_daily_summary_date != today_str
                ):
                    closed_trades = get_closed_trades()
                    daily_stats = build_daily_stats_from_closed_trades(
                        closed_trades,
                        target_dt=now_local,
                    )
                    notify_daily_close_summary(daily_stats)
                    last_daily_summary_date = today_str

                    logger.info(
                        "DAILY_SUMMARY_SENT | date=%s closed=%s pnl=%.4f",
                        today_str,
                        daily_stats.get("closed_trades", 0),
                        float(daily_stats.get("pnl_quote_sum", 0.0)),
                    )

                    try:
                        log_event(
                            "DAILY_SUMMARY_SENT",
                            f"date={today_str} "
                            f"closed={daily_stats.get('closed_trades', 0)} "
                            f"wins={daily_stats.get('wins', 0)} "
                            f"losses={daily_stats.get('losses', 0)} "
                            f"pnl={float(daily_stats.get('pnl_quote_sum', 0.0)):.4f}"
                        )
                    except Exception:
                        pass

            except Exception as e:
                logger.warning(f"DAILY_SUMMARY_FAIL | err={e}")

        except Exception as e:
            logger.exception(f"WORKER_LOOP_ERROR | err={e}")
            try:
                log_event("WORKER_LOOP_ERROR", f"err={e}")
            except Exception:
                pass

        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
