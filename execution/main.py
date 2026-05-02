import os
import time
import logging
from typing import Optional, Dict, Any

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GENIUS-DCA-Bot — main.py
# CHANGELOG:
#
# FIX #1 — buy_qty slippage correction (3 ადგილი)
# FIX #2 — TP/SL/FC trades lookup Layer სიმბოლოებისთვის
# FIX #3 — CASCADE symbol suffix regex
# FIX #4 — ADD-ON exchange_sym
# FIX #9 — LAYER2 Cooldown 180s
# FIX #10 — CASCADE net_proceeds < $10 → skip
# FIX #11 — Layer პოზიციებზე ADD-ON skip
# FIX #13 — CASCADE buy_quote fixed $12
# FIX #14 — TP_FIX ყოველ loop-ზე
#
# FIX #24 — GENIUS MIRROR ENGINE (bilateral SHORT DCA hedge)
#   თეორია: LONG DCA ვარდნაზე + SHORT ვარდნაზე ერთდროულად
#   trigger: LONG L1-დან -8.59% (L2/L3 midpoint)
#   DOWN mode ADD-ONs: -1.0%, -2.2%, -3.5%, -5.0%, -6.5% → avg↓ → TP↓
#   UP   mode ADD-ONs: +1.0%, +2.2%, +3.5%, +5.0%, +6.5% → avg↑ → TP↑
#   TP: current_price <= avg × 0.9945 (0.55% ქვევით)
#   FC: position +15% avg-დან → CLOSE (max ზარალი)
#   FC time: გაუქმებულია (დრო = მოკავშირე)
#   სცენარი A (bounce): LONG TP + MIRROR TP = ორივე მოგება ✓
#   სცენარი B (ვარდნა გრძელდება): LONG FC + MIRROR TP = neutralized ✓
#   სცენარი C (sideways): MIRROR scalp ✓
#   ფაილი: futures_engine.py (სრული implementation)
#   ENV: MIRROR_ENGINE_ENABLED=true
#        FUTURES_MODE=LIVE
#        BINANCE_FUTURES_API_KEY=...
#        BINANCE_FUTURES_API_SECRET=...
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/render/project/src/.env", override=False)
except ImportError:
    pass

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
# FIX #24: MIRROR ENGINE — futures_engine import
from execution.futures_engine import get_futures_engine
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
            s.get("closed_trades", 0), s.get("wins", 0), s.get("losses", 0),
            float(s.get("winrate_pct", 0.0)), float(s.get("roi_pct", 0.0)),
            float(s.get("pnl_quote_sum", 0.0)), float(s.get("quote_in_sum", 0.0)),
            float(s.get("profit_factor", 0.0)), s.get("open_trades", 0),
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
    from execution.db.repository import (
        get_all_open_dca_positions, close_dca_position,
        update_dca_position_after_addon, update_dca_sl_price,
        add_dca_order, open_dca_position,
    )
    from execution.dca_position_manager import recalculate_average, score_recovery_signals

    open_positions = get_all_open_dca_positions()
    if not open_positions:
        return

    for pos in open_positions:
        sym = pos["symbol"]
        pos_id = pos["id"]
        import re as _re_sym
        exchange_sym = _re_sym.sub(r'_L\d+$', '', sym)
        is_layer2 = sym != exchange_sym

        try:
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
            sl_price     = 0.0
            total_qty    = float(pos["total_qty"] or 0)
            total_quote  = float(pos["total_quote_spent"] or 0)
            add_on_count = int(pos["add_on_count"] or 0)

            logger.info(
                f"[DCA] MONITOR | {sym} price={current_price:.4f} "
                f"avg={avg_entry:.4f} tp={tp_price:.4f} "
                f"qty={total_qty:.6f} add_ons={add_on_count}"
            )

            if tp_price > 0 and current_price >= tp_price:
                logger.info(f"[DCA] TP_HIT | {sym} price={current_price:.4f} >= tp={tp_price:.4f}")
                try:
                    sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                    exit_price = float(sell.get("average") or sell.get("price") or current_price)
                    pnl_quote = (exit_price - avg_entry) * total_qty
                    pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0
                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "TP")
                    _open_tr = get_open_trade_for_symbol(sym)
                    if not _open_tr:
                        _open_tr = get_open_trade_for_symbol(exchange_sym)
                    if _open_tr:
                        close_trade(_open_tr[0], exit_price, "TP", pnl_quote, pnl_pct)
                        logger.info(f"[DCA] TRADE_CLOSED_TP | {sym} signal_id={_open_tr[0]} pnl={pnl_quote:+.4f}")
                    else:
                        logger.warning(f"[DCA] TRADE_NOT_FOUND | {sym} — trades row missing on TP")
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

            force_close, fc_reason = tp_sl_mgr.should_force_close(pos, current_price)
            if force_close:
                logger.warning(f"[DCA] FORCE_CLOSE | {sym} reason={fc_reason}")
                try:
                    sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                    exit_price = float(sell.get("average") or sell.get("price") or current_price)
                    pnl_quote = (exit_price - avg_entry) * total_qty
                    pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0
                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "FORCE_CLOSE")
                    _open_tr = get_open_trade_for_symbol(sym)
                    if not _open_tr:
                        _open_tr = get_open_trade_for_symbol(exchange_sym)
                    if _open_tr:
                        close_trade(_open_tr[0], exit_price, "FORCE_CLOSE", pnl_quote, pnl_pct)
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

            try:
                from execution.signal_generator import _fetch_ohlcv_direct
                # BOT_TIMEFRAME — მხოლოდ ENV-იდან იკითხება (Render ENV)
                # default: 5m (scalp-DCA: სიგნალი ყოველ 5 წუთში)
                tf = os.getenv("BOT_TIMEFRAME", "5m")
                ohlcv = _fetch_ohlcv_direct(exchange_sym, tf, 60)
            except Exception as e:
                logger.warning(f"[DCA] OHLCV_FAIL | {sym} err={e}")
                continue

            if not ohlcv or len(ohlcv) < 30:
                continue

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
                        _open_tr = get_open_trade_for_symbol(sym)
                        if not _open_tr:
                            _open_tr = get_open_trade_for_symbol(exchange_sym)
                        if _open_tr:
                            close_trade(_open_tr[0], exit_price, "SL", pnl_quote, pnl_pct)
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

            be_update, new_sl = tp_sl_mgr.check_breakeven(avg_entry, current_price, sl_price)
            if be_update:
                update_dca_sl_price(pos_id, new_sl)
                from execution.telegram_notifier import notify_dca_breakeven
                notify_dca_breakeven(sym, avg_entry, sl_price, new_sl)
                sl_price = new_sl

            if is_layer2:
                logger.debug(f"[DCA] SKIP_ADDON | {sym} is Layer position → CASCADE manages")
                continue

            all_positions = get_all_open_dca_positions()
            addon_ok, addon_reason = dca_mgr.should_add_on(pos, current_price, ohlcv)
            if not addon_ok:
                logger.debug(f"[DCA] NO_ADDON | {sym} reason={addon_reason}")
                continue

            risk_ok, risk_reason = risk_mgr.can_add_on(pos, dca_mgr.get_addon_size(add_on_count), all_positions)
            if not risk_ok:
                logger.info(f"[DCA] ADDON_RISK_BLOCK | {sym} reason={risk_reason}")
                continue

            addon_size = dca_mgr.get_addon_size(add_on_count)
            drawdown_pct = (avg_entry - current_price) / avg_entry * 100.0
            score, score_details = score_recovery_signals(ohlcv)

            logger.info(
                f"[DCA] PLACING_ADDON | {sym} level={add_on_count+1} "
                f"size={addon_size} drawdown={drawdown_pct:.2f}% score={score}/5"
            )

            try:
                buy = engine.exchange.place_market_buy_by_quote(exchange_sym, addon_size)
                buy_price = float(buy.get("average") or buy.get("price") or current_price)
                buy_qty   = float(buy.get("filled") or buy.get("amount") or (addon_size / buy_price))
                avg_result = recalculate_average(total_qty, avg_entry, buy_qty, buy_price)
                new_avg    = avg_result["avg_entry_price"]
                new_qty    = avg_result["total_qty"]
                new_quote  = total_quote + addon_size
                tp_sl      = tp_sl_mgr.calculate(new_avg)
                new_tp     = tp_sl["tp_price"]
                new_sl     = tp_sl["sl_price"]
                update_dca_position_after_addon(
                    pos_id,
                    new_avg_entry=new_avg, new_total_qty=new_qty,
                    new_total_quote=new_quote, new_add_on_count=add_on_count + 1,
                    new_tp_price=new_tp, new_sl_price=new_sl,
                    last_add_on_ts=time.time(),
                )
                rsi_val = score_details.get("rsi", 0.0)
                atr_val = score_details.get("atr_pct", 0.0)
                add_dca_order(
                    position_id=pos_id, symbol=sym,
                    order_type=f"ADD_ON_{add_on_count + 1}",
                    entry_price=buy_price, qty=buy_qty, quote_spent=addon_size,
                    avg_entry_after=new_avg, tp_after=new_tp, sl_after=new_sl,
                    trigger_drawdown_pct=drawdown_pct,
                    rsi_at_entry=rsi_val, atr_pct_at_entry=atr_val,
                    recovery_score=score, exchange_order_id=str(buy.get("id", "")),
                )
                notify_dca_addon(
                    symbol=sym, addon_number=add_on_count + 1,
                    addon_price=buy_price, addon_quote=addon_size,
                    new_avg_entry=new_avg, total_quote_spent=new_quote,
                    new_tp_price=new_tp, new_sl_price=new_sl,
                    drawdown_pct=drawdown_pct, recovery_score=score,
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
    if not os.getenv("LAYER2_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return
    from execution.db.repository import (
        get_open_dca_position_for_symbol, open_dca_position,
        add_dca_order, open_trade, log_event,
    )
    drop_pct    = float(os.getenv("LAYER2_DROP_PCT",  "5.0"))
    quote       = float(os.getenv("LAYER2_QUOTE",     "10.0"))
    symbols_raw = os.getenv("LAYER2_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols     = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    tp_pct      = float(os.getenv("DCA_TP_PCT", "0.55"))
    buffer      = float(os.getenv("SMART_ADDON_BUFFER", "5.0"))
    try:
        free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
    except Exception as _e:
        logger.warning(f"[LAYER2] balance_fetch_fail | err={_e}")
        return

    for sym in symbols:
        try:
            current_price = float(engine.exchange.fetch_last_price(sym) or 0.0)
            if current_price <= 0:
                continue
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
                f"high24h={high_24h:.4f} drop={drop_from_high:.2f}% trigger={drop_pct:.1f}%"
            )
            if drop_from_high < drop_pct:
                logger.debug(f"[LAYER2] NO_CRASH | {sym} drop={drop_from_high:.2f}% < {drop_pct:.1f}%")
                continue
            sym_l2 = f"{sym}_L2"
            existing_l2 = get_open_dca_position_for_symbol(sym_l2)
            if existing_l2:
                logger.debug(f"[LAYER2] ALREADY_OPEN | {sym_l2}")
                continue
            required = quote + buffer
            if free_usdt < required:
                logger.warning(f"[LAYER2] INSUFFICIENT_BALANCE | {sym} free={free_usdt:.2f} < required={required:.2f}")
                continue
            logger.warning(f"[LAYER2] CRASH_DETECTED | {sym} drop={drop_from_high:.2f}% >= {drop_pct:.1f}% → opening Layer 2")
            buy = engine.exchange.place_market_buy_by_quote(sym, quote)
            buy_price = float(buy.get("average") or buy.get("price") or current_price)
            buy_qty   = float(buy.get("filled") or buy.get("amount") or (quote / buy_price))
            tp_price  = round(buy_price * (1.0 + tp_pct / 100.0), 6)
            pos_id = open_dca_position(
                symbol=sym_l2, initial_entry_price=buy_price,
                initial_qty=buy_qty, initial_quote_spent=quote,
                tp_price=tp_price, sl_price=0.0, tp_pct=tp_pct, sl_pct=999.0,
                max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "1")),
                max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "20.0")),
                max_drawdown_pct=999.0,
            )
            add_dca_order(
                position_id=pos_id, symbol=sym_l2, order_type="LAYER2_INITIAL",
                entry_price=buy_price, qty=buy_qty, quote_spent=quote,
                avg_entry_after=buy_price, tp_after=tp_price, sl_after=0.0,
                trigger_drawdown_pct=drop_from_high, exchange_order_id=str(buy.get("id", "")),
            )
            import uuid
            l2_signal_id = f"L2-{sym.replace('/', '')}-{uuid.uuid4().hex[:8]}"
            open_trade(signal_id=l2_signal_id, symbol=sym_l2, qty=buy_qty, quote_in=quote, entry_price=buy_price)
            free_usdt -= quote
            try:
                log_event("LAYER2_OPENED", f"sym={sym_l2} entry={buy_price:.4f} tp={tp_price:.4f} drop={drop_from_high:.2f}% pos_id={pos_id}")
            except Exception:
                pass
            try:
                from execution.telegram_notifier import notify_signal_created
                notify_signal_created(symbol=sym_l2, entry_price=buy_price, quote_amount=quote,
                    tp_price=tp_price, sl_price=0.0, verdict="LAYER2_BUY", mode=engine.mode)
            except Exception as _tg:
                logger.warning(f"[LAYER2] TG_FAIL | {sym} err={_tg}")
            logger.warning(f"[LAYER2] OPENED | {sym_l2} entry={buy_price:.4f} tp={tp_price:.4f} qty={buy_qty:.6f}")
        except Exception as e:
            logger.error(f"[LAYER2] ERR | {sym} err={e}")



def _check_cascade_exchange(engine, tp_sl_mgr) -> None:
    if not os.getenv("CASCADE_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return
    from execution.db.repository import (
        get_all_open_dca_positions, close_dca_position, open_dca_position,
        add_dca_order, open_trade, get_open_trade_for_symbol, close_trade, log_event,
    )
    cascade_start  = int(os.getenv("CASCADE_START_LAYER",  "2"))
    drop_pct_base  = float(os.getenv("CASCADE_DROP_PCT",    "1.5"))
    drop_pct_l4    = float(os.getenv("CASCADE_DROP_L4_PCT", "2.0"))
    drop_pct_l8    = float(os.getenv("CASCADE_DROP_L8_PCT", "5.0"))
    tp_pct_base    = float(os.getenv("DCA_TP_PCT",          "0.55"))
    tp_pct_l3      = float(os.getenv("CASCADE_TP_L3_PCT",   "0.65"))
    tp_pct_l8      = float(os.getenv("CASCADE_TP_L8_PCT",   "1.00"))
    max_layers     = int(os.getenv("CASCADE_MAX_LAYERS",    "10"))
    resume_layer   = int(os.getenv("CASCADE_RESUME_LAYER",  "10"))
    symbols_raw    = os.getenv("CASCADE_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols        = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    buffer         = float(os.getenv("SMART_ADDON_BUFFER", "5.0"))

    all_positions = get_all_open_dca_positions()
    total_layers  = len(all_positions)
    logger.info(f"[CASCADE] CHECK | total_layers={total_layers} start_at={cascade_start} max={max_layers} resume_at={resume_layer}")
    if total_layers < cascade_start:
        logger.debug(f"[CASCADE] NOT_YET | {total_layers} < {cascade_start}")
        return
    if total_layers >= max_layers:
        if total_layers < resume_layer:
            logger.info(f"[CASCADE] PAUSED | {total_layers} >= {max_layers}, waiting for {resume_layer}")
            return
        else:
            logger.warning(f"[CASCADE] RESUMING | total_layers={total_layers} >= {resume_layer}")

    for sym in symbols:
        try:
            exchange_sym = sym
            current_price = float(engine.exchange.fetch_last_price(exchange_sym) or 0.0)
            if current_price <= 0:
                continue
            import re as _re
            sym_positions = [
                p for p in all_positions
                if _re.sub(r'_L\d+$', '', str(p.get("symbol", "")).upper()) == sym.upper()
            ]
            if len(sym_positions) < 2:
                logger.debug(f"[CASCADE] {sym} | only {len(sym_positions)} layer(s) → skip")
                continue
            oldest = sorted(sym_positions, key=lambda p: str(p.get("opened_at", "")))[0]
            oldest_avg   = float(oldest.get("avg_entry_price", 0.0))
            oldest_qty   = float(oldest.get("total_qty", 0.0))
            oldest_quote = float(oldest.get("total_quote_spent", 0.0))
            oldest_id    = oldest["id"]
            oldest_sym   = oldest["symbol"]
            layer_num    = len(sym_positions)
            if layer_num >= 8:
                drop_pct = drop_pct_l8
            elif layer_num >= 4:
                drop_pct = drop_pct_l4
            else:
                drop_pct = drop_pct_base
            if layer_num >= 8:
                tp_pct = tp_pct_l8
            elif layer_num >= 3:
                tp_pct = tp_pct_l3
            else:
                tp_pct = tp_pct_base
            newest = sorted(sym_positions, key=lambda p: str(p.get("opened_at", "")))[-1]
            newest_avg = float(newest.get("avg_entry_price", 0.0))
            if newest_avg <= 0:
                newest_avg = oldest_avg
            drop_from_newest = (newest_avg - current_price) / newest_avg * 100.0
            logger.info(
                f"[CASCADE] {sym} | layer={layer_num} oldest={oldest_sym} "
                f"avg={oldest_avg:.4f} newest_avg={newest_avg:.4f} "
                f"price={current_price:.4f} drop_from_newest={drop_from_newest:.2f}% trigger={drop_pct:.1f}%"
            )
            if drop_from_newest < drop_pct:
                logger.debug(f"[CASCADE] {sym} | drop={drop_from_newest:.2f}% < {drop_pct:.1f}% → wait")
                continue
            free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
            if free_usdt < buffer:
                logger.warning(f"[CASCADE] {sym} | low_balance={free_usdt:.2f} < buffer={buffer:.1f}")
                continue
            logger.warning(f"[CASCADE] EXCHANGE | {oldest_sym} avg={oldest_avg:.4f} qty={oldest_qty:.6f} drop={drop_from_newest:.2f}%")
            try:
                sell = engine.exchange.place_market_sell(exchange_sym, oldest_qty)
                sell_price    = float(sell.get("average") or sell.get("price") or current_price)
                proceeds      = sell_price * oldest_qty
                fee           = proceeds * 0.001
                net_proceeds  = round(proceeds - fee, 4)
                pnl_quote     = (sell_price - oldest_avg) * oldest_qty
                pnl_pct       = (sell_price / oldest_avg - 1.0) * 100.0
                close_dca_position(oldest_id, sell_price, oldest_qty, pnl_quote, pnl_pct, "CASCADE_EXCHANGE")
                open_tr = get_open_trade_for_symbol(oldest_sym)
                if not open_tr:
                    open_tr = get_open_trade_for_symbol(exchange_sym)
                if not open_tr:
                    base = exchange_sym.replace("/USDT", "")
                    for suffix in ["", "_L2", "_L3", "_L4", "_L5", "_L6", "_L7", "_L8", "_L9", "_L10"]:
                        _tr = get_open_trade_for_symbol(f"{base}/USDT{suffix}")
                        if _tr:
                            open_tr = _tr
                            break
                if open_tr:
                    close_trade(open_tr[0], sell_price, "CASCADE_EXCHANGE", pnl_quote, pnl_pct)
                logger.warning(f"[CASCADE] SOLD | {oldest_sym} price={sell_price:.4f} proceeds={net_proceeds:.4f} pnl={pnl_quote:+.4f}")
            except Exception as _se:
                logger.error(f"[CASCADE] SELL_FAIL | {oldest_sym} err={_se}")
                continue
            if net_proceeds < 5.0:
                logger.warning(f"[CASCADE] LOW_PROCEEDS | {net_proceeds:.4f} < $5 → skip new layer")
                continue
            new_sym   = f"{sym}_L{layer_num + 1}"
            buy_quote = float(os.getenv("BOT_QUOTE_PER_TRADE", "12.0"))
            try:
                buy = engine.exchange.place_market_buy_by_quote(exchange_sym, buy_quote)
                buy_price = float(buy.get("average") or buy.get("price") or current_price)
                buy_qty   = float(buy.get("filled") or buy.get("amount") or (buy_quote / buy_price))
                tp_price  = round(buy_price * (1.0 + tp_pct / 100.0), 6)
                pos_id = open_dca_position(
                    symbol=new_sym, initial_entry_price=buy_price,
                    initial_qty=buy_qty, initial_quote_spent=buy_quote,
                    tp_price=tp_price, sl_price=0.0, tp_pct=tp_pct, sl_pct=999.0,
                    max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "1")),
                    max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "20.0")),
                    max_drawdown_pct=999.0,
                )
                add_dca_order(
                    position_id=pos_id, symbol=new_sym, order_type="CASCADE_LAYER",
                    entry_price=buy_price, qty=buy_qty, quote_spent=buy_quote,
                    avg_entry_after=buy_price, tp_after=tp_price, sl_after=0.0,
                    trigger_drawdown_pct=drop_from_newest, exchange_order_id=str(buy.get("id", "")),
                )
                import uuid
                cascade_signal_id = f"CAS-{sym.replace('/', '')}-{uuid.uuid4().hex[:8]}"
                open_trade(signal_id=cascade_signal_id, symbol=new_sym, qty=buy_qty, quote_in=buy_quote, entry_price=buy_price)
                try:
                    log_event("CASCADE_LAYER_OPENED", f"sym={new_sym} entry={buy_price:.4f} tp={tp_price:.4f} quote={buy_quote:.4f} from={oldest_sym}")
                except Exception:
                    pass
                logger.warning(f"[CASCADE] NEW_LAYER | {new_sym} entry={buy_price:.4f} tp={tp_price:.4f} quote={buy_quote:.4f}")
            except Exception as _be:
                logger.error(f"[CASCADE] BUY_FAIL | {new_sym} err={_be}")
        except Exception as e:
            logger.error(f"[CASCADE] ERR | {sym} err={e}")



def _start_bot_api_server() -> None:
    if not os.getenv("BOT_API_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return
    try:
        from flask import Flask as _Flask, jsonify as _jsonify
    except ImportError:
        logger.warning("[BOT_API] Flask not installed → API disabled")
        return
    import threading as _threading
    from datetime import datetime as _dt, timezone as _tz
    api_app = _Flask("bot_api")

    @api_app.route("/api/stats")
    def bot_api_stats():
        try:
            from execution.db.repository import (
                get_trade_stats, get_all_open_dca_positions, get_closed_trades,
            )
            stats     = get_trade_stats()
            positions = get_all_open_dca_positions()
            trades    = get_closed_trades()
            recent = sorted(
                [t for t in trades if t.get("outcome")],
                key=lambda x: str(x.get("closed_at", "")), reverse=True,
            )[:20]
            return _jsonify({"stats": stats, "positions": positions, "recent_trades": recent,
                "timestamp": _dt.now(_tz.utc).isoformat()})
        except Exception as e:
            logger.error(f"[BOT_API] stats error: {e}")
            return _jsonify({"error": str(e)}), 500

    @api_app.route("/health")
    def bot_api_health():
        return _jsonify({"status": "ok", "service": "GENIUS-DCA-Bot"})

    def _run():
        port = int(os.getenv("BOT_API_PORT", "5001"))
        logger.info(f"[BOT_API] Starting on port {port} → /api/stats")
        api_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)

    t = _threading.Thread(target=_run, daemon=True, name="bot_api")
    t.start()
    logger.info("[BOT_API] API server thread started")



def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

    mode        = os.getenv("MODE", "DEMO").upper()
    outbox_path = os.getenv("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")
    sleep_s     = float(os.getenv("LOOP_SLEEP_SECONDS", "10"))

    report_every_s          = int(os.getenv("REPORT_EVERY_SECONDS", "60"))
    telegram_report_every_s = int(os.getenv("TELEGRAM_REPORT_EVERY_SECONDS", "1800"))

    last_report_ts         = 0.0
    last_tg_report_ts      = 0.0
    last_daily_summary_date = None

    heartbeat_every_s = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "600"))
    last_heartbeat_ts = 0.0

    daily_max_loss  = float(os.getenv("DAILY_MAX_LOSS_USDT", "5.0"))
    _daily_loss_date  = ""
    _daily_loss_total = 0.0

    init_db()
    _bootstrap_state_if_needed()

    if os.getenv("TP_FIX_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        try:
            import threading as _tp_thread
            import time as _tp_time
            def _run_tp_fix_delayed():
                _tp_time.sleep(10)
                try:
                    from execution.tp_fix import run_tp_fix
                    _r = run_tp_fix()
                    logger.info(f"TP_FIX | checked={_r.get('checked',0)} fixed={_r.get('fixed',0)} skipped={_r.get('skipped',0)}")
                except Exception as _tpe2:
                    logger.warning(f"TP_FIX_FAIL | err={_tpe2}")
            _tp_thread.Thread(target=_run_tp_fix_delayed, daemon=True, name="tp_fix").start()
            logger.info("TP_FIX | scheduled in 10s (background thread)")
        except Exception as _tpe:
            logger.warning(f"TP_FIX_THREAD_FAIL | err={_tpe}")

    if os.getenv("QTY_SYNC_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        try:
            import threading as _qty_thread
            def _run_qty_sync_delayed():
                import time as _t
                _delay = int(os.getenv("QTY_SYNC_DELAY", "20"))
                _t.sleep(_delay)
                try:
                    from execution.qty_sync import run_qty_sync
                    _r = run_qty_sync()
                    logger.info(f"QTY_SYNC | checked={_r.get('checked',0)} fixed={_r.get('fixed',0)} skipped={_r.get('skipped',0)}")
                except Exception as _qe2:
                    logger.warning(f"QTY_SYNC_FAIL | err={_qe2}")
            _qty_thread.Thread(target=_run_qty_sync_delayed, daemon=True, name="qty_sync").start()
            logger.info("QTY_SYNC | scheduled in 20s (background thread)")
        except Exception as _qe:
            logger.warning(f"QTY_SYNC_THREAD_FAIL | err={_qe}")

    try:
        _start_bot_api_server()
    except Exception as _ae:
        logger.warning(f"BOT_API_START_FAIL | err={_ae}")

    if os.getenv("DASHBOARD_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        try:
            from execution.dashboard import start_dashboard
            _dash_port = int(os.getenv("DASHBOARD_PORT", "8080"))
            start_dashboard(port=_dash_port)
        except Exception as _de:
            logger.warning(f"DASHBOARD_START_FAIL | err={_de}")

    engine = ExecutionEngine()
    regime_engine = MarketRegimeEngine()
    engine.inject_regime_engine(regime_engine)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FIX #24: MIRROR ENGINE init — singleton, LIVE-ზე მუშაობს
    # MIRROR_ENGINE_ENABLED=true + FUTURES_MODE=LIVE + API keys
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    futures_engine = get_futures_engine()
    logger.info(
        f"[MIRROR] FUTURES_ENGINE | enabled={futures_engine.enabled} "
        f"mirror={futures_engine.mirror_enabled} "
        f"mode={futures_engine.mode}"
    )

    generate_once = _try_import_generator()

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

            _price_cache: dict = {}
            if engine.exchange is not None:
                _symbols_to_cache = [s.strip() for s in os.getenv(
                    "BOT_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT"
                ).split(",") if s.strip()]
                for _sym in _symbols_to_cache:
                    try:
                        _price_cache[_sym] = float(engine.exchange.fetch_last_price(_sym) or 0.0)
                    except Exception as _pe:
                        logger.warning(f"PRICE_CACHE_FAIL | {_sym} err={_pe}")
                        _price_cache[_sym] = 0.0

            _today = _now_dt().date().isoformat()
            if _today != _daily_loss_date:
                _daily_loss_date  = _today
                _daily_loss_total = 0.0
                logger.info(f"DAILY_LOSS_RESET | date={_today} limit={daily_max_loss}")

            if daily_max_loss > 0 and _daily_loss_total <= -daily_max_loss:
                logger.warning(f"DAILY_LOSS_LIMIT | loss={_daily_loss_total:.4f} >= limit={daily_max_loss} → skip")
                try:
                    from execution.telegram_notifier import send_telegram_message
                    send_telegram_message(
                        f"⛔ <b>DAILY LOSS LIMIT</b>\n\n"
                        f"📉 დღის ზარალი: <code>{_daily_loss_total:.4f} USDT</code>\n"
                        f"🛡 Limit: <code>{daily_max_loss} USDT</code>\n"
                        f"⏸ ვაჭრობა შეჩერებულია დღეს\n"
                        f"🕒 <code>{_now_dt().strftime('%Y-%m-%d %H:%M')}</code>"
                    )
                except Exception:
                    pass
                time.sleep(sleep_s)
                continue

            if _dca_enabled:
                try:
                    from execution.tp_fix import run_tp_fix
                    _tp_r = run_tp_fix()
                    if _tp_r.get("fixed", 0) > 0:
                        logger.warning(f"TP_FIX_LOOP | fixed={_tp_r['fixed']} checked={_tp_r['checked']}")
                except Exception as _tfe:
                    logger.warning(f"TP_FIX_LOOP_WARN | err={_tfe}")

            if _dca_enabled:
                try:
                    _run_dca_loop(engine, dca_mgr, tp_sl_mgr, risk_mgr)
                except Exception as e:
                    logger.warning(f"DCA_LOOP_WARN | err={e}")

            if _dca_enabled and engine.exchange is not None:
                try:
                    _check_and_open_layer2(engine, tp_sl_mgr)
                except Exception as e:
                    logger.warning(f"LAYER2_CHECK_WARN | err={e}")

            if _dca_enabled and engine.exchange is not None:
                try:
                    _check_cascade_exchange(engine, tp_sl_mgr)
                except Exception as e:
                    logger.warning(f"CASCADE_CHECK_WARN | err={e}")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # FIX #24: MIRROR ENGINE — bilateral SHORT DCA hedge
            # trigger: LONG L1-დან -8.59% (L2/L3 midpoint)
            # DOWN mode: ვარდნა → ADD-ONs SHORT → avg↓ → TP↓
            # UP   mode: bounce → ADD-ONs SHORT → avg↑ → TP↑
            # სცენარი A (bounce): LONG TP + MIRROR TP ✓
            # სცენარი B (crash):  LONG FC + MIRROR TP → neutralized ✓
            # სცენარი C (sideways): MIRROR scalp ✓
            # ENV: MIRROR_ENGINE_ENABLED=true (default: false)
            #      FUTURES_MODE=LIVE
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _dca_enabled and futures_engine.enabled and futures_engine.mirror_enabled:
                try:
                    futures_engine.check_mirror_tp_sl()
                    futures_engine.check_mirror_engine_open()
                    futures_engine.check_mirror_addons()
                except Exception as _me:
                    logger.warning(f"MIRROR_ENGINE_LOOP_WARN | err={_me}")

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
                    verdict   = str(sig.get("final_verdict", "")).upper()
                    logger.info(f"Signal received | id={signal_id} | verdict={verdict}")

                    if _SIGNAL_EXPIRATION_SECONDS > 0:
                        try:
                            from datetime import datetime, timezone
                            ts_raw = sig.get("ts_utc", "")
                            if ts_raw:
                                sig_dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                                if sig_dt.tzinfo is None:
                                    sig_dt = sig_dt.replace(tzinfo=timezone.utc)
                                age_s = (datetime.now(timezone.utc) - sig_dt).total_seconds()
                                if age_s > _SIGNAL_EXPIRATION_SECONDS:
                                    logger.warning(f"[EXPIRED] signal skipped | id={signal_id} age={age_s:.0f}s > limit={_SIGNAL_EXPIRATION_SECONDS}s")
                                    try:
                                        log_event("SIGNAL_EXPIRED", f"id={signal_id} age={age_s:.0f}s verdict={verdict}")
                                    except Exception:
                                        pass
                                    continue
                        except Exception as e:
                            logger.warning(f"EXPIRY_CHECK_FAIL | id={signal_id} err={e} → skip check")

                    if verdict == "SELL":
                        source = sig.get("meta", {}).get("source", "UNKNOWN")
                        if source == "PROTECTIVE_SELL":
                            logger.warning(f"[AUTO] PROTECTIVE_SELL → executing | id={signal_id} source={source}")
                            engine.execute_signal(sig)
                        else:
                            logger.info(f"[AUTO] SELL blocked (DCA holds) | id={signal_id} source={source}")

                    elif verdict == "TRADE":
                        trend   = float(sig.get("trend",   0) or 0)
                        atr_pct = float(sig.get("atr_pct", 0) or 0)
                        symbol  = str((sig.get("execution") or {}).get("symbol", ""))
                        regime  = regime_engine.detect_regime(trend=trend, atr_pct=atr_pct)
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

                        # ── DEMO: DCA position გახსნა ──────────────────────────────
                        # LIVE-ზე: execution_engine.execute_signal() → place_market_buy()
                        #          → open_dca_position() შიგნით იხსნება ✅
                        # DEMO-ზე: buy dict ცარიელია → buy_avg=None → DCA ვერ იხსნება
                        #          → _run_dca_loop() dca_positions-ს ვერ ხედავს
                        #          → TP/ADD-ON არ მუშაობს
                        # FIX: DEMO-ზე main.py-ი ხსნის price_cache-ის ფასით
                        # LIVE-ზე: get_open_dca_position_for_symbol() = exists → skip ✅
                        if engine.exchange is None and _dca_enabled:
                            try:
                                from execution.db.repository import (
                                    open_dca_position, add_dca_order,
                                    get_open_dca_position_for_symbol,
                                )
                                _sym = str((sig.get("execution") or {}).get("symbol", "BTC/USDT"))
                                _quote = float(os.getenv("BOT_QUOTE_PER_TRADE", "20"))
                                _tp_pct = float(os.getenv("DCA_TP_PCT", "0.55"))

                                # double-open guard — LIVE-ზე execution_engine უკვე გახსნა
                                if not get_open_dca_position_for_symbol(_sym):
                                    _price = _price_cache.get(_sym, 0.0)
                                    if _price <= 0:
                                        try:
                                            _t = engine.price_feed.fetch_ticker(_sym)
                                            _price = float(_t.get("last") or 0.0)
                                        except Exception:
                                            pass
                                    if _price > 0:
                                        _qty = _quote / _price
                                        _tp  = round(_price * (1.0 + _tp_pct / 100.0), 6)
                                        _sizes_str = os.getenv("DCA_ADDON_SIZES", "15,20,22,20,12")
                                        try:
                                            _addon_sum = sum(float(x) for x in _sizes_str.split(",") if x.strip())
                                        except Exception:
                                            _addon_sum = 89.0
                                        _max_cap = float(os.getenv("DCA_MAX_CAPITAL_USDT") or (_quote + _addon_sum))
                                        _pos_id = open_dca_position(
                                            symbol=_sym,
                                            initial_entry_price=_price,
                                            initial_qty=_qty,
                                            initial_quote_spent=_quote,
                                            tp_price=_tp,
                                            sl_price=0.0,
                                            tp_pct=_tp_pct,
                                            sl_pct=999.0,
                                            max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "5")),
                                            max_capital=_max_cap,
                                            max_drawdown_pct=999.0,
                                        )
                                        add_dca_order(
                                            position_id=_pos_id,
                                            symbol=_sym,
                                            order_type="INITIAL",
                                            entry_price=_price,
                                            qty=_qty,
                                            quote_spent=_quote,
                                            avg_entry_after=_price,
                                            tp_after=_tp,
                                            sl_after=0.0,
                                            trigger_drawdown_pct=0.0,
                                            exchange_order_id=signal_id,
                                        )
                                        logger.info(
                                            f"[DEMO] DCA_OPENED | {_sym} "
                                            f"price={_price:.4f} qty={_qty:.6f} "
                                            f"tp={_tp:.4f} quote={_quote}"
                                        )
                                        try:
                                            from execution.telegram_notifier import notify_signal_created
                                            notify_signal_created(
                                                symbol=_sym,
                                                entry_price=_price,
                                                quote_amount=_quote,
                                                tp_price=_tp,
                                                sl_price=0.0,
                                                verdict=str(sig.get("final_verdict", "BUY")),
                                                mode="DEMO",
                                            )
                                        except Exception as _tg:
                                            logger.warning(f"[DEMO] TG_FAIL | {_sym} err={_tg}")
                            except Exception as _de:
                                logger.warning(f"[DEMO] DCA_OPEN_FAIL | err={_de}")
                    else:
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
                _hb_now    = _now_dt()
                _hb_hour   = _hb_now.hour
                _hb_minute = _hb_now.minute
                _hb_silent = (0 < _hb_hour < 8) or (_hb_hour == 0 and _hb_minute >= 30)
                _hb_night_ok = (_hb_hour == 0 and 29 <= _hb_minute <= 31)
                _hb_day_ok   = not _hb_silent and (now - last_heartbeat_ts) >= 1800
                if not _hb_silent and (_hb_night_ok or _hb_day_ok):
                    from execution.db.repository import get_all_open_dca_positions, get_trade_stats
                    from execution.telegram_notifier import notify_heartbeat
                    import resource as _res
                    _hb_positions = get_all_open_dca_positions()
                    _hb_capital   = sum(float(p.get("total_quote_spent", 0)) for p in _hb_positions)
                    _hb_stats     = get_trade_stats()
                    _hb_pnl_today = float(_hb_stats.get("pnl_quote_sum", 0.0))
                    _hb_mem       = _res.getrusage(_res.RUSAGE_SELF).ru_maxrss / 1024
                    notify_heartbeat(
                        open_count=len(_hb_positions), open_capital=_hb_capital,
                        prices=_price_cache, memory_mb=_hb_mem,
                        pnl_today=_hb_pnl_today, positions=_hb_positions,
                    )
                    last_heartbeat_ts = now
            except Exception as _hbe:
                logger.warning(f"HEARTBEAT_FAIL | err={_hbe}")

            try:
                now_local = _now_dt()
                today_str = now_local.date().isoformat()
                if (now_local.hour == 23 and now_local.minute == 59
                        and last_daily_summary_date != today_str):
                    closed_trades = get_closed_trades()
                    daily_stats = build_daily_stats_from_closed_trades(closed_trades, target_dt=now_local)
                    notify_daily_close_summary(daily_stats)
                    last_daily_summary_date = today_str
                    logger.info(
                        "DAILY_SUMMARY_SENT | date=%s closed=%s pnl=%.4f",
                        today_str, daily_stats.get("closed_trades", 0),
                        float(daily_stats.get("pnl_quote_sum", 0.0)),
                    )
                    try:
                        log_event(
                            "DAILY_SUMMARY_SENT",
                            f"date={today_str} closed={daily_stats.get('closed_trades', 0)} "
                            f"wins={daily_stats.get('wins', 0)} losses={daily_stats.get('losses', 0)} "
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
