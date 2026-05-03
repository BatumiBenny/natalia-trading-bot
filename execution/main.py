import os
import time
import logging
from typing import Optional, Dict, Any

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GENIUS-DCA-Bot — main.py
# CHANGELOG:
#
# FIX #1 — buy_qty slippage correction (3 ადგილი)
#   პრობლემა: buy_qty = quote / buy_price  → slippage იგნორირებული
#   გამოსწორება: buy.get("filled") → Binance-ის რეალური filled qty
#
# FIX #2 — TP/SL/FC trades lookup Layer სიმბოლოებისთვის
#   პრობლემა: BTC/USDT_L2 trade ვერ იპოვებოდა DB-ში
#   გამოსწორება: sym პირველი, exchange_sym fallback
#
# FIX #14 — TP_FIX ყოველ loop-ზე (2026-04-12)
#   პრობლემა: ADD-ON-ის შემდეგ TP < avg → პოზიცია არასოდეს იყიდება
#   გამოსწორება: run_tp_fix() ყოველ loop-ზე
#
# FIX #17 — DCA TP/FORCE_CLOSE → Hedge SHORT auto-close (BUG-1)
#   პრობლემა: DCA TP hit-ის შემდეგ hedge SHORT რჩებოდა ღია →
#             BTC ამაღლდებოდა → SHORT ზარალი (missing link)
#   გამოსწორება: close_dca_hedge_for_position(pos_id) TP და FC block-ებში
#   ფაილი: main.py (2 ადგილი) + futures_engine.py (ახალი მეთოდი)
#
# FIX #18 — MAX_OPEN_TRADES hardcoded fallback "8" → "6" (BUG-2)
#   პრობლემა: main.py os.getenv("MAX_OPEN_TRADES","8") — ENV=6, config=2
#             triple conflict: 3 სხვადასხვა მნიშვნელობა სამ ადგილში
#   გამოსწორება: fallback "8" → "6" (ENV-ს ემთხვევა)
#
# FIX #19 — INDEPENDENT SHORT DCA სისტემა
#   სარკე სტრატეგია: LONG L1 გახსნის შემდეგ SHORT იხსნება -1.6%-ზე
#   ADD-ONs ვარდნაზე (LONG-ის სარკე): -1.0%, -2.2%, -3.5%
#   TP: avg × 0.9945 — სავალდებულო დახურვა
#   FC: 10 დღე / +15% drawdown — ღია ტრეიდი არ რჩება
#   ENV: SHORT_DCA_ENABLED=true + SHORT_* params
#
# FIX #22 — LAYER2 trigger: 24h HIGH-based → L1-based
#   პრობლემა: 24h HIGH spike (ღამის pump/შეცდომა) → HIGH=8k, L1=5k
#             L2 trigger = 8k × 0.985 = 6.5k → L1-ზე მაღლა!
#             L2 არასოდეს გაიხსნება (ბაზარი ვარდნისასაც კი)
#   გამოსწორება: L2 trigger = L1 avg_entry_price × (1 - LAYER2_DROP_PCT/100)
#     L1 open AND price < L1 × 0.985 → L2 გახსნა
#     L1 არ არის? → L2 skip (L2 L1-ის გარეშე აზრი არ აქვს)
#     drop_from_high → drop_from_l1 (4 ადგილი LAYER2 ფუნქციაში)
#
# FIX #24 — PHANTOM WINGS სისტემა
#   L1 გახსნისთანავე 6 Wings + 2 LP orders Binance-ზე
#   LW1/LW2: limit UP  → _open_wings_level() (ჯავშანი, $15 each)
#   LW3-LW6: market buy → _open_wings_market() ($30 each, qty×2)
#   LP1/LP2: market buy → _open_wings_market() ($30 each, qty×2)
#   ADD-ONs: ყველა ღია _LW_/_LP_ position DCA add-on სისტემაში
#   Duplicate guard: _pending_wings_orders (LW1/LW2) + DB check
#   ENV: WINGS_ENABLED, WINGS_LEVELS_UP, WINGS_LEVELS_DOWN,
#        WINGS_LP_LEVELS, WINGS_QUOTES, WINGS_LP_QUOTES,
#        WINGS_TIMEOUT_SECONDS
#
# FIX #25 — PHANTOM WINGS კაპიტალის ოპტიმიზაცია ($700 ბალანსი)
#   პრობლემა: LW3-LW6 + LP1-LP2 limit-ი → Binance-ზე frozen capital
#             3 symbols × $225 = $675 + ADD-ONs → $700-ს გადააჭარბებდა
#   გამოსწორება:
#     1. LW3-LW6 + LP1-LP2 → market buy (frozen capital $0)
#     2. LP2 drop = -0.10% (LP1=-0.05%, LP2=-0.10%)
#     3. DCA_MAX_ADD_ONS=3 (9 pos × 3 × $15 = $405 + $225 = $630 ✅)
#     4. BOT_SYMBOLS=BTC/USDT (1 symbol)
#   კაპიტალი: $225 entry + $405 add-on reserve + $70 buffer = $700 ✅
#
# FIX #26 — PHANTOM WINGS per-level symbol (WINGS_SYMBOLS_UP/DOWN/LP)
#   პრობლემა: ყველა LW/LP ერთი symbol-ით იხსნებოდა (BOT_SYMBOLS[0])
#             "2-2 ცალი სხვადასხვა კოინი" — LW3=BTC, LW4=ETH (არა BTC,BTC)
#   გამოსწორება: WINGS_SYMBOLS_UP/DOWN/LP ENV — per-level symbol mapping
#     LW1 → WINGS_SYMBOLS_UP[0],  LW2 → WINGS_SYMBOLS_UP[1]
#     LW3 → WINGS_SYMBOLS_DOWN[0], LW4 → WINGS_SYMBOLS_DOWN[1]
#     LW5 → WINGS_SYMBOLS_DOWN[2], LW6 → WINGS_SYMBOLS_DOWN[3]
#     LP1 → WINGS_SYMBOLS_LP[0],  LP2 → WINGS_SYMBOLS_LP[1]
#   default (ENV გარეშე): BOT_SYMBOLS round-robin
#   L1 reference: თითო symbol-ის L1 avg_entry_price (ან პირველი L1)
#   DB symbol: {wing_base}_LW{n} → "ETH/USDT_LW3", "BNB/USDT_LW6"
#   Binance call: base_sym = wing_base (whitelist-ში უკვე არის)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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
from execution.dca_tp_sl_manager import get_tp_sl_manager
from execution.dca_risk_manager import get_risk_manager
from execution.futures_engine import get_futures_engine
from execution.exchange_client import ExchangeClientError
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FIX #24: PHANTOM WINGS — pending limit orders tracker
# _pending_wings_orders: {order_id: {exchange_sym, wings_sym,
#   target_price, wings_quote, tp_pct, max_add_ons, max_capital,
#   signal_id, opened_at, trigger_drop_pct}}
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


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


def _run_dca_loop(engine, dca_mgr, tp_sl_mgr, risk_mgr,
                  market_regime: str = "NEUTRAL",
                  futures_engine=None) -> None:
    """
    DCA monitoring loop — ყოველ main loop iteration-ზე გამოიძახება.

    შეამოწმებს:
      1. TP hit → close position
      2. Breakeven → SL გადაადგილება
      3. Force close → max drawdown ან max add-ons + SL
      4. SL confirmed → close position
      5. Add-on → drawdown trigger + recovery signals
         BEAR MODE: L2+L3 ADD-ON+rotation blocked
      6. DCA Hedge SHORT trigger → add_on_count == max_add_ons

    FIX #20: exchange_sym regex გაფართოვდა (_L[0-9]+|_LP)$ —
    LP positions-ი სწორად მუშაობს DCA loop-ში.
    """
    from execution.db.repository import (
        get_all_open_dca_positions,
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

        # FIX #20: (_L\d+|_LP)$ — LP suffix-ის სწორი strip
        import re as _re_sym
        exchange_sym = _re_sym.sub(r'(_L\d+|_LP\d*|_LW\d*)$', '', sym)  # FIX #24: _LW added

        try:
            # current price — DEMO: price_feed, LIVE: exchange
            try:
                if engine.exchange is not None:
                    current_price = engine.exchange.fetch_last_price(exchange_sym)
                else:
                    # DEMO mode — public API, key არ სჭირდება
                    _ticker = engine.price_feed.fetch_ticker(exchange_sym)
                    current_price = float(_ticker.get("last") or 0.0)
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
                    # DEMO: ვირტუალური გაყიდვა
                    if engine.exchange is None:
                        exit_price = current_price
                        pnl_quote = (exit_price - avg_entry) * total_qty
                        pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0
                        sell = {"average": exit_price, "price": exit_price}
                    else:
                        sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                        exit_price = float(sell.get("average") or sell.get("price") or current_price)
                        pnl_quote = (exit_price - avg_entry) * total_qty
                        pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0

                    # ── dca_positions დახურვა ──────────────────────────
                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "TP")

                    # ── DCA hedge SHORT auto-close (BUG-1 FIX) ─────────
                    if futures_engine is not None:
                        try:
                            futures_engine.close_dca_hedge_for_position(
                                pos_id, reason="DCA_TP_HIT"
                            )
                        except Exception as _hce:
                            logger.warning(f"[DCA] HEDGE_CLOSE_TP_FAIL | {sym} err={_hce}")

                    # trades ცხრილის დახურვა
                    _open_tr = get_open_trade_for_symbol(sym)
                    if not _open_tr:
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
                    if engine.exchange is None:
                        exit_price = current_price
                        sell = {"average": exit_price, "price": exit_price}
                    else:
                        sell = engine.exchange.place_market_sell(exchange_sym, total_qty)
                        exit_price = float(sell.get("average") or sell.get("price") or current_price)
                    pnl_quote = (exit_price - avg_entry) * total_qty
                    pnl_pct   = (exit_price / avg_entry - 1.0) * 100.0

                    close_dca_position(pos_id, exit_price, total_qty, pnl_quote, pnl_pct, "FORCE_CLOSE")

                    # ── DCA hedge SHORT auto-close (BUG-1 FIX) ─────────
                    if futures_engine is not None:
                        try:
                            futures_engine.close_dca_hedge_for_position(
                                pos_id, reason="DCA_FORCE_CLOSE"
                            )
                        except Exception as _hce:
                            logger.warning(f"[DCA] HEDGE_CLOSE_FC_FAIL | {sym} err={_hce}")

                    # trades ცხრილის დახურვა
                    _open_tr = get_open_trade_for_symbol(sym)
                    if not _open_tr:
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
                        if engine.exchange is None:
                            exit_price = current_price
                            sell = {"average": exit_price, "price": exit_price}
                        else:
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

            # BEAR MODE: L2 ADD-ON ბლოკილია (SHORT-ს ეწინააღმდეგება)
            if market_regime == "BEAR":
                logger.info(f"[DCA] BEAR_BLOCK | {sym} BEAR market → L2+L3 ADD-ON+rotation blocked")
                continue

            if not addon_ok:
                logger.debug(f"[DCA] NO_ADDON | {sym} reason={addon_reason}")

                # ── L3 ZONE ──────────────────────────────────────────────
                n     = int(pos.get("add_on_count", 0))
                max_n = dca_mgr.max_add_ons

                if n >= max_n:
                    l3_done = int(pos.get("l3_addon_done", 0) or 0)

                    if not l3_done:
                        last_ap = float(pos.get("last_addon_price") or avg_entry)
                        if last_ap > 0:
                            drop_from_last = (last_ap - current_price) / last_ap * 100.0
                            rot_trigger    = dca_mgr.rotation_trigger_pct  # 1.5%
                            if drop_from_last >= rot_trigger:
                                logger.warning(
                                    f"[DCA] L3_ADDON_TRIGGER | {sym} "
                                    f"drop={drop_from_last:.2f}% >= {rot_trigger:.1f}% → L3 ADD-ON"
                                )
                                _execute_l3_addon(engine, pos, current_price, tp_sl_mgr)
                            else:
                                logger.debug(
                                    f"[DCA] L3_ADDON_WAIT | {sym} "
                                    f"drop={drop_from_last:.2f}% < {rot_trigger:.1f}%"
                                )
                    else:
                        rotate_ok, rotate_reason = dca_mgr.should_rotate(pos, current_price)
                        if rotate_ok:
                            logger.warning(f"[DCA] L3_ROTATION_TRIGGER | {sym} → LIFO")
                            _execute_l3_rotation(engine, pos, current_price, tp_sl_mgr, dca_mgr)
                        else:
                            logger.debug(f"[DCA] NO_ROTATION | {sym} reason={rotate_reason}")

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
                if engine.exchange is None:
                    buy_price = current_price
                    buy_qty   = addon_size / buy_price
                    buy = {"average": buy_price, "price": buy_price, "filled": buy_qty}
                else:
                    buy = engine.exchange.place_market_buy_by_quote(exchange_sym, addon_size)
                    buy_price = float(buy.get("average") or buy.get("price") or current_price)
                    buy_qty   = float(buy.get("filled") or buy.get("amount") or (addon_size / buy_price))

                avg_result = recalculate_average(total_qty, avg_entry, buy_qty, buy_price)
                new_avg    = avg_result["avg_entry_price"]
                new_qty    = avg_result["total_qty"]
                new_quote  = total_quote + addon_size

                _pos_after_addon = dict(pos)
                _pos_after_addon["add_on_count"] = add_on_count + 1
                tp_sl = tp_sl_mgr.calculate(new_avg, position=_pos_after_addon)
                new_tp = tp_sl["tp_price"]
                new_sl = tp_sl["sl_price"]

                update_dca_position_after_addon(
                    pos_id,
                    new_avg_entry=new_avg,
                    new_total_qty=new_qty,
                    new_total_quote=new_quote,
                    new_add_on_count=add_on_count + 1,
                    new_tp_price=new_tp,
                    new_sl_price=new_sl,
                    last_add_on_ts=time.time(),
                    last_addon_price=buy_price,
                )

                # ── DCA HEDGE SHORT trigger ───────────────────────────
                _new_add_on_count = add_on_count + 1
                _max_add_ons = dca_mgr.max_add_ons
                if _new_add_on_count == _max_add_ons and futures_engine is not None:
                    try:
                        _triggered = futures_engine.open_dca_hedge_short(
                            symbol=exchange_sym,
                            current_price=buy_price,
                            dca_pos_id=pos_id,
                            market_regime=market_regime,
                        )
                        if _triggered:
                            logger.warning(
                                f"[DCA] HEDGE_SHORT_TRIGGERED | {sym} "
                                f"add_on={_new_add_on_count}/{_max_add_ons} "
                                f"@ {buy_price:.4f}"
                            )
                    except Exception as _he:
                        logger.warning(f"[DCA] HEDGE_TRIGGER_FAIL | {sym} err={_he}")

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


def _execute_l3_addon(engine, pos: dict, current_price: float, tp_sl_mgr) -> None:
    """
    L3 ADD-ON — L2 resource ($10) გადადის L3-ზე.

    FIX #20: exchange_sym regex (_L[0-9]+|_LP)$ — LP positions-ი სწორად
    """
    from execution.db.repository import (
        add_dca_order,
        update_dca_position_after_l3_addon,
        log_event,
    )
    from execution.dca_position_manager import recalculate_average

    sym         = pos["symbol"]
    pos_id      = pos["id"]
    avg_entry   = float(pos["avg_entry_price"] or 0)
    total_qty   = float(pos["total_qty"] or 0)
    total_quote = float(pos["total_quote_spent"] or 0)

    # FIX #20: (_L\d+|_LP)$ regex
    import re as _re_l3
    exchange_sym = _re_l3.sub(r'(_L\d+|_LP\d*|_LW\d*)$', '', sym)  # FIX #24: _LW added

    try:
        l3_addon_quote = float(os.getenv("BOT_QUOTE_PER_TRADE", "15.0"))

        from execution.dca_risk_manager import get_risk_manager as _get_rm
        _l3_risk_ok, _l3_risk_reason = _get_rm().can_l3_operation(l3_addon_quote)
        if not _l3_risk_ok:
            logger.warning(f"[L3_ADDON] RISK_BLOCK | {sym} reason={_l3_risk_reason}")
            return

        if engine.exchange is None:
            buy_price = current_price
            buy_qty   = l3_addon_quote / buy_price
            buy = {"average": buy_price, "filled": buy_qty}
        else:
            buy = engine.exchange.place_market_buy_by_quote(exchange_sym, l3_addon_quote)
            buy_price = float(buy.get("average") or buy.get("price") or current_price)
            buy_qty   = float(buy.get("filled") or buy.get("amount") or (l3_addon_quote / buy_price))

        avg_result = recalculate_average(total_qty, avg_entry, buy_qty, buy_price)
        new_avg    = avg_result["avg_entry_price"]
        new_qty    = avg_result["total_qty"]
        new_quote  = total_quote + l3_addon_quote

        new_tp = tp_sl_mgr.calculate_rotation_tp(new_avg)
        drawdown_pct = (avg_entry - current_price) / avg_entry * 100.0 if avg_entry > 0 else 0.0

        logger.warning(
            f"[L3_ADDON] OPENED | {sym} @ {buy_price:.4f} "
            f"qty={buy_qty:.6f} quote={l3_addon_quote} | "
            f"old_avg={avg_entry:.4f} → new_avg={new_avg:.4f} "
            f"new_tp={new_tp:.4f} drawdown={drawdown_pct:.2f}%"
        )

        update_dca_position_after_l3_addon(
            position_id=pos_id,
            new_avg_entry=new_avg,
            new_total_qty=new_qty,
            new_total_quote=new_quote,
            new_tp_price=new_tp,
            last_addon_price=buy_price,
        )

        add_dca_order(
            position_id=pos_id,
            symbol=sym,
            order_type="L3_ADDON",
            entry_price=buy_price,
            qty=buy_qty,
            quote_spent=l3_addon_quote,
            avg_entry_after=new_avg,
            tp_after=new_tp,
            sl_after=0.0,
            trigger_drawdown_pct=drawdown_pct,
            exchange_order_id=str(buy.get("id", "")),
        )

        try:
            log_event(
                "L3_ADDON_OPENED",
                f"sym={sym} price={buy_price:.4f} quote={l3_addon_quote} "
                f"old_avg={avg_entry:.4f} new_avg={new_avg:.4f} "
                f"new_tp={new_tp:.4f} drawdown={drawdown_pct:.2f}%"
            )
        except Exception:
            pass

        try:
            from execution.telegram_notifier import send_telegram_message
            send_telegram_message(
                f"📥 <b>L3 ADD-ON გახსნა</b>\n\n"
                f"🪙 <b>Symbol:</b> <code>{sym}</code>\n"
                f"💰 <b>Entry:</b> <code>{buy_price:.2f}</code> "
                f"(<code>-{drawdown_pct:.2f}%</code>)\n"
                f"📊 <b>avg:</b> <code>{avg_entry:.2f} → {new_avg:.2f}</code> ↓\n"
                f"🎯 <b>TP:</b> <code>{new_tp:.2f}</code> (L3: 0.35%)\n"
                f"⚠️ <b>შემდეგი:</b> LIFO rotation კიდევ -1.5%-ზე\n"
                f"🕒 <code>{__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )
        except Exception as _tg:
            logger.warning(f"[L3_ADDON] TG_FAIL | err={_tg}")

    except Exception as e:
        logger.error(f"[L3_ADDON] FAIL | {sym} err={e}")


def _execute_l3_rotation(engine, pos: dict, current_price: float, tp_sl_mgr, dca_mgr) -> None:
    """
    LIFO Rotation — L3 zone: ADD-ONs exhausted + price კვლავ ეცემა.

    FIX #20: exchange_sym regex (_L[0-9]+|_LP)$ — LP positions-ი სწორად
    """
    from execution.db.repository import (
        get_dca_orders,
        add_dca_order,
        update_dca_position_after_rotation,
        log_event,
    )
    from execution.dca_position_manager import recalculate_average

    sym      = pos["symbol"]
    pos_id   = pos["id"]
    avg_entry = float(pos["avg_entry_price"] or 0)
    total_qty = float(pos["total_qty"] or 0)
    total_quote = float(pos["total_quote_spent"] or 0)

    # FIX #20: (_L\d+|_LP)$ regex
    import re as _re_rot
    exchange_sym = _re_rot.sub(r'(_L\d+|_LP\d*|_LW\d*)$', '', sym)  # FIX #24: _LW added

    try:
        dca_orders = get_dca_orders(pos_id)
        if not dca_orders:
            logger.warning(f"[L3_ROT] NO_ORDERS | {sym} → skip rotation")
            return

        lifo_unit = dca_mgr.get_lifo_unit(dca_orders)
        if not lifo_unit:
            logger.warning(f"[L3_ROT] NO_LIFO_UNIT | {sym} → skip rotation")
            return

        lifo_price = float(lifo_unit.get("entry_price", 0.0))
        lifo_qty   = float(lifo_unit.get("qty", 0.0))
        lifo_quote = float(lifo_unit.get("quote_spent", 0.0))

        if lifo_price <= 0 or lifo_qty <= 0:
            logger.warning(f"[L3_ROT] INVALID_LIFO | {sym} price={lifo_price} qty={lifo_qty} → skip")
            return

        if engine.exchange is None:
            sell_price = current_price
        else:
            sell_result = engine.exchange.place_market_sell(exchange_sym, lifo_qty)
            sell_price  = float(sell_result.get("average") or sell_result.get("price") or current_price)

        proceeds     = sell_price * lifo_qty
        fee          = proceeds * 0.001
        net_proceeds = proceeds - fee

        from execution.dca_risk_manager import get_risk_manager as _get_rm
        _rot_risk_ok, _rot_risk_reason = _get_rm().can_l3_operation(net_proceeds)
        if not _rot_risk_ok:
            logger.warning(f"[L3_ROT] RISK_BLOCK | {sym} net_proceeds={net_proceeds:.2f} reason={_rot_risk_reason}")
            return

        realized_pnl = (sell_price - lifo_price) * lifo_qty - fee

        logger.warning(
            f"[L3_ROT] LIFO_SELL | {sym} "
            f"lifo_price={lifo_price:.4f} sell={sell_price:.4f} "
            f"qty={lifo_qty:.6f} pnl={realized_pnl:+.4f}"
        )

        if engine.exchange is None:
            reinvest_price = current_price
            reinvest_qty   = net_proceeds / reinvest_price
        else:
            reinvest_result = engine.exchange.place_market_buy_by_quote(exchange_sym, net_proceeds)
            reinvest_price  = float(reinvest_result.get("average") or reinvest_result.get("price") or current_price)
            reinvest_qty    = float(reinvest_result.get("filled") or reinvest_result.get("amount") or (net_proceeds / reinvest_price))

        remaining_qty   = total_qty - lifo_qty
        total_value     = total_qty * avg_entry
        remaining_value = total_value - lifo_qty * lifo_price

        new_qty   = remaining_qty + reinvest_qty
        new_value = remaining_value + reinvest_qty * reinvest_price
        new_avg   = round(new_value / new_qty, 8) if new_qty > 0 else avg_entry

        new_tp = tp_sl_mgr.calculate_rotation_tp(new_avg)
        new_total_quote = total_quote - lifo_quote + net_proceeds

        logger.warning(
            f"[L3_ROT] REINVEST | {sym} "
            f"@ {reinvest_price:.4f} qty={reinvest_qty:.6f} | "
            f"old_avg={avg_entry:.4f} → new_avg={new_avg:.4f} "
            f"new_tp={new_tp:.4f} (L3 zone 0.35%)"
        )

        update_dca_position_after_rotation(
            position_id=pos_id,
            new_avg_entry=new_avg,
            new_total_qty=new_qty,
            new_total_quote=new_total_quote,
            new_tp_price=new_tp,
            last_rotation_ts=time.time(),
            rotation_pnl=realized_pnl,
        )

        add_dca_order(
            position_id=pos_id,
            symbol=sym,
            order_type="ROTATION_REINVEST",
            entry_price=reinvest_price,
            qty=reinvest_qty,
            quote_spent=net_proceeds,
            avg_entry_after=new_avg,
            tp_after=new_tp,
            sl_after=0.0,
            trigger_drawdown_pct=(avg_entry - current_price) / avg_entry * 100.0 if avg_entry > 0 else 0.0,
            exchange_order_id="",
        )

        try:
            log_event(
                "L3_ROTATION",
                f"sym={sym} lifo_price={lifo_price:.4f} sell={sell_price:.4f} "
                f"reinvest={reinvest_price:.4f} old_avg={avg_entry:.4f} "
                f"new_avg={new_avg:.4f} new_tp={new_tp:.4f} "
                f"pnl={realized_pnl:+.4f}"
            )
        except Exception:
            pass

        try:
            from execution.telegram_notifier import send_telegram_message
            send_telegram_message(
                f"🔄 <b>L3 LIFO ROTATION</b>\n\n"
                f"🪙 <b>Symbol:</b> <code>{sym}</code>\n"
                f"💸 <b>LIFO sell:</b> <code>{lifo_price:.2f} → {sell_price:.2f}</code>\n"
                f"♻️ <b>Reinvest:</b> <code>{reinvest_price:.2f}</code>\n"
                f"📊 <b>avg:</b> <code>{avg_entry:.2f} → {new_avg:.2f}</code> ↓\n"
                f"🎯 <b>TP:</b> <code>{new_tp:.2f}</code> (L3: 0.35%)\n"
                f"💰 <b>Rotation PnL:</b> <code>{realized_pnl:+.4f} USDT</code>\n"
                f"🕒 <code>{__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )
        except Exception as _tg:
            logger.warning(f"[L3_ROT] TG_FAIL | err={_tg}")

    except Exception as e:
        logger.error(f"[L3_ROT] ROTATION_FAIL | {sym} err={e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHANTOM WINGS — FIX #24
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# სტრუქტურა (BTC @ $77,000 მაგალითზე):
#   LW1/LW2 @ L1 × (1 + WINGS_LEVELS_UP[n]/100)   ← ზემოთ limit
#   LW3/LW4 @ L1 × (1 - WINGS_LEVELS_DOWN[n]/100)  ← ქვემოთ limit
#   LW5/LW6 @ L1 × (1 - WINGS_LEVELS_DOWN[n]/100)  ← ღრმა ვარდნა
#   LP1/LP2 @ L1 × (1 - WINGS_LP_LEVELS[n]/100)    ← phantom ქვემოთ
#
# DB symbols: BTC/USDT_LW1 ... BTC/USDT_LW6, BTC/USDT_LP1, BTC/USDT_LP2
# exchange_sym: _re_sym.sub(r'(_L\d+|_LP\d*|_LW\d*)$', '', sym)
#
# ADD-ON: ყველა ღია _LW_ position ავტომატურად DCA add-on-ში შედის
#   _run_dca_loop() ამუშავებს — regex სტრიპი ემთხვევა exchange_sym-ს
#
# Timeout: WINGS_TIMEOUT_SECONDS (default 300s)
#   expired → cancel → loop-ზე retry (WINGS_ENABLED=true)
#
# Duplicate guard: _pending_wings_orders dict (order_id → meta)
#   ყოველ loop-ზე pending-ი ამოწმდება → duplicate Binance order-ი ბლოკდება
#
# ENV:
#   WINGS_ENABLED=true
#   WINGS_LEVELS_UP=0.05,0.05                    ← LW1,LW2 limit ზემოთ %
#   WINGS_LEVELS_DOWN=0.05,0.05,0.10,0.10        ← LW3,LW4,LW5,LW6 market %
#   WINGS_LP_LEVELS=0.05,0.10                    ← LP1=-0.05%, LP2=-0.10%
#   WINGS_QUOTES=15,15,30,30,30,30               ← LW1/LW2=$15, LW3-6=$30
#   WINGS_LP_QUOTES=30,30                        ← LP1/LP2=$30
#   WINGS_TIMEOUT_SECONDS=300                    ← LW1/LW2 limit timeout only
#   WINGS_SYMBOLS_UP=BTC/USDT,ETH/USDT          ← per-level symbol LW1,LW2
#   WINGS_SYMBOLS_DOWN=BTC/USDT,ETH/USDT,BTC/USDT,BNB/USDT  ← LW3,LW4,LW5,LW6
#   WINGS_SYMBOLS_LP=BTC/USDT,ETH/USDT          ← LP1,LP2
#   (default: BOT_SYMBOLS-დან round-robin თუ WINGS_SYMBOLS_* არ არის)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# WINGS LIMIT ORDER TRACKER — pending orders dict (order_id → metadata)
# Shared between _open_wings_level() and _lp_wings_limit_tracker_thread()
_pending_wings_orders: dict = {}
_pending_wings_lock = None  # threading.Lock — main()-ში init


def _wings_limit_tracker_thread(engine_ref_getter, lock) -> None:
    """
    PHANTOM WINGS limit order tracker — background daemon thread.

    ყოველ 30 წამში:
      1. pending wings orders → Binance-ზე status check
      2. filled → open_dca_position + add_dca_order + open_trade + TG
      3. timeout (WINGS_TIMEOUT_SECONDS) → cancel_order + log
      4. zero fill (cancelled externally) → remove from pending

    engine_ref_getter: callable → engine instance (late binding)
    """
    timeout_s = int(os.getenv("WINGS_TIMEOUT_SECONDS", "300"))

    while True:
        try:
            time.sleep(30)
            engine = engine_ref_getter()
            if engine is None or engine.exchange is None:
                continue  # DEMO mode — tracker skip

            with lock:
                orders_snapshot = dict(_pending_wings_orders)

            for order_id, meta in orders_snapshot.items():
                try:
                    elapsed      = time.time() - meta["opened_at"]
                    exchange_sym = meta["exchange_sym"]
                    wings_sym    = meta["wings_sym"]
                    # wings_quote removed — actual_quote = filled_qty * avg_fill (tracker)
                    target_price = meta["target_price"]
                    tp_pct       = meta["tp_pct"]
                    signal_id    = meta["signal_id"]

                    try:
                        order_status = engine.exchange.fetch_order(order_id, exchange_sym)
                    except Exception as _fe:
                        logger.warning(f"[WINGS_TRACKER] FETCH_FAIL | {wings_sym} order={order_id} err={_fe}")
                        continue

                    status     = str(order_status.get("status", "")).lower()
                    filled_qty = float(order_status.get("filled") or 0.0)
                    avg_fill   = float(
                        order_status.get("average") or
                        order_status.get("price") or
                        target_price
                    )

                    # ── FILLED ────────────────────────────────────────
                    if status in ("closed", "filled") or filled_qty > 0:
                        if filled_qty <= 0:
                            with lock:
                                _pending_wings_orders.pop(order_id, None)
                            logger.info(f"[WINGS_TRACKER] ZERO_FILL | {wings_sym} → removed")
                            continue

                        actual_quote = filled_qty * avg_fill
                        tp_price     = round(avg_fill * (1.0 + tp_pct / 100.0), 6)

                        from execution.db.repository import (
                            open_dca_position, add_dca_order, open_trade,
                            get_open_dca_position_for_symbol,
                        )

                        if get_open_dca_position_for_symbol(wings_sym):
                            with lock:
                                _pending_wings_orders.pop(order_id, None)
                            logger.info(f"[WINGS_TRACKER] ALREADY_OPEN | {wings_sym} → skip")
                            continue

                        pos_id = open_dca_position(
                            symbol=wings_sym,
                            initial_entry_price=avg_fill,
                            initial_qty=filled_qty,
                            initial_quote_spent=actual_quote,
                            tp_price=tp_price,
                            sl_price=0.0,
                            tp_pct=tp_pct,
                            sl_pct=999.0,
                            max_add_ons=meta["max_add_ons"],
                            max_capital=meta["max_capital"],
                            max_drawdown_pct=999.0,
                        )
                        add_dca_order(
                            position_id=pos_id,
                            symbol=wings_sym,
                            order_type="WINGS_INITIAL",
                            entry_price=avg_fill,
                            qty=filled_qty,
                            quote_spent=actual_quote,
                            avg_entry_after=avg_fill,
                            tp_after=tp_price,
                            sl_after=0.0,
                            trigger_drawdown_pct=meta.get("trigger_drop_pct", 0.0),
                            exchange_order_id=order_id,
                        )
                        open_trade(
                            signal_id=signal_id,
                            symbol=wings_sym,
                            qty=filled_qty,
                            quote_in=actual_quote,
                            entry_price=avg_fill,
                        )

                        with lock:
                            _pending_wings_orders.pop(order_id, None)

                        logger.warning(
                            f"[WINGS_TRACKER] FILLED | {wings_sym} "
                            f"@ {avg_fill:.4f} qty={filled_qty:.6f} "
                            f"tp={tp_price:.4f} elapsed={elapsed:.0f}s"
                        )
                        try:
                            log_event(
                                "WINGS_LIMIT_FILLED",
                                f"sym={wings_sym} price={avg_fill:.4f} "
                                f"qty={filled_qty:.6f} tp={tp_price:.4f} "
                                f"elapsed={elapsed:.0f}s"
                            )
                        except Exception:
                            pass
                        try:
                            from execution.telegram_notifier import notify_signal_created
                            notify_signal_created(
                                symbol=wings_sym,
                                entry_price=avg_fill,
                                quote_amount=actual_quote,
                                tp_price=tp_price,
                                sl_price=0.0,
                                verdict="WINGS_FILLED",
                                mode="LIVE",
                            )
                        except Exception as _tg:
                            logger.warning(f"[WINGS_TRACKER] TG_FAIL | err={_tg}")

                    # ── TIMEOUT → CANCEL ───────────────────────────────
                    elif elapsed >= timeout_s:
                        try:
                            engine.exchange.cancel_order(order_id, exchange_sym)
                            logger.warning(
                                f"[WINGS_TRACKER] TIMEOUT_CANCEL | {wings_sym} "
                                f"order={order_id} elapsed={elapsed:.0f}s"
                            )
                        except Exception as _ce:
                            logger.warning(f"[WINGS_TRACKER] CANCEL_FAIL | {wings_sym} err={_ce}")

                        with lock:
                            _pending_wings_orders.pop(order_id, None)

                        try:
                            log_event(
                                "WINGS_LIMIT_EXPIRED",
                                f"sym={wings_sym} order={order_id} "
                                f"target={target_price:.4f} elapsed={elapsed:.0f}s"
                            )
                        except Exception:
                            pass
                        try:
                            from execution.telegram_notifier import send_telegram_message
                            send_telegram_message(
                                f"⏱ <b>WINGS Limit Expired</b>\n\n"
                                f"🪙 <b>Symbol:</b> <code>{wings_sym}</code>\n"
                                f"🎯 <b>Target:</b> <code>{target_price:.4f}</code>\n"
                                f"⏸ Unfilled after <code>{timeout_s}s</code> → cancelled\n"
                                f"🕒 <code>{_now_dt().strftime('%Y-%m-%d %H:%M:%S')}</code>"
                            )
                        except Exception:
                            pass

                    else:
                        logger.debug(
                            f"[WINGS_TRACKER] PENDING | {wings_sym} "
                            f"order={order_id} elapsed={elapsed:.0f}s/{timeout_s}s"
                        )

                except Exception as _oe:
                    logger.warning(f"[WINGS_TRACKER] ORDER_ERR | order={order_id} err={_oe}")

        except Exception as _te:
            logger.warning(f"[WINGS_TRACKER] THREAD_ERR | err={_te}")


def _open_wings_level(
    engine,
    base_sym: str,
    l1_price: float,
    wings_sym: str,
    target_price: float,
    quote: float,
    signal_id: str,
    tp_pct: float,
    max_add_ons: int,
    max_capital: float,
    trigger_drop_pct: float = 0.0,
) -> None:
    """
    ერთი PHANTOM WINGS level-ის გახსნა.

    DEMO: ვირტუალური entry @ target_price
    LIVE: GTC limit order @ target_price → Binance
          order_id → _pending_wings_orders → tracker thread

    Args:
      engine:           ExecutionEngine instance
      base_sym:         "BTC/USDT" (without suffix)
      l1_price:         L1 avg_entry_price (reference)
      wings_sym:        full symbol, e.g. "BTC/USDT_LW1"
      target_price:     Binance limit order price
      quote:            USDT to spend
      signal_id:        parent signal ID
      tp_pct:           TP percent (DCA_TP_PCT)
      max_add_ons:      DCA_MAX_ADD_ONS
      max_capital:      DCA_MAX_CAPITAL_USDT
      trigger_drop_pct: pct distance from L1 (for DB logging only)
                        positive = ABOVE L1 (UP wings: LW1/LW2)
                        negative = BELOW L1 (DOWN wings: LW3-LW6, LP1-LP2)
    """
    from execution.db.repository import (
        open_dca_position, add_dca_order, open_trade,
        get_open_dca_position_for_symbol, log_event,
    )

    # double-open guard (DB)
    if get_open_dca_position_for_symbol(wings_sym):
        logger.debug(f"[WINGS] ALREADY_OPEN | {wings_sym} → skip")
        return

    # duplicate pending guard (Binance)
    if _pending_wings_lock is not None:
        with _pending_wings_lock:
            _already_pending = any(
                m.get("wings_sym") == wings_sym
                for m in _pending_wings_orders.values()
            )
        if _already_pending:
            logger.debug(f"[WINGS] ALREADY_PENDING | {wings_sym} → skip")
            return

    is_live = engine.exchange is not None

    # ── DEMO ──────────────────────────────────────────────────────
    if not is_live:
        buy_price = target_price
        buy_qty   = quote / buy_price if buy_price > 0 else 0.0
        if buy_qty <= 0:
            return
        tp_price = round(buy_price * (1.0 + tp_pct / 100.0), 6)

        pos_id = open_dca_position(
            symbol=wings_sym,
            initial_entry_price=buy_price,
            initial_qty=buy_qty,
            initial_quote_spent=quote,
            tp_price=tp_price,
            sl_price=0.0,
            tp_pct=tp_pct,
            sl_pct=999.0,
            max_add_ons=max_add_ons,
            max_capital=max_capital,
            max_drawdown_pct=999.0,
        )
        add_dca_order(
            position_id=pos_id,
            symbol=wings_sym,
            order_type="WINGS_INITIAL",
            entry_price=buy_price,
            qty=buy_qty,
            quote_spent=quote,
            avg_entry_after=buy_price,
            tp_after=tp_price,
            sl_after=0.0,
            trigger_drawdown_pct=abs(trigger_drop_pct),
            exchange_order_id=f"DEMO-{signal_id}",
        )
        open_trade(
            signal_id=signal_id,
            symbol=wings_sym,
            qty=buy_qty,
            quote_in=quote,
            entry_price=buy_price,
        )
        logger.warning(
            f"[WINGS] DEMO_OPENED | {wings_sym} "
            f"entry={buy_price:.4f} (L1={l1_price:.4f} "
            f"{('+' if trigger_drop_pct >= 0 else '')}{trigger_drop_pct:.4f}%) "
            f"tp={tp_price:.4f} quote={quote}"
        )
        try:
            log_event(
                "WINGS_OPENED_DEMO",
                f"sym={wings_sym} entry={buy_price:.4f} tp={tp_price:.4f} "
                f"l1={l1_price:.4f} trigger_pct={trigger_drop_pct:.4f}"
            )
        except Exception:
            pass
        try:
            from execution.telegram_notifier import notify_signal_created
            notify_signal_created(
                symbol=wings_sym,
                entry_price=buy_price,
                quote_amount=quote,
                tp_price=tp_price,
                sl_price=0.0,
                verdict="WINGS_BUY",
                mode="DEMO",
            )
        except Exception as _tg:
            logger.warning(f"[WINGS] TG_FAIL | {wings_sym} err={_tg}")
        return

    # ── LIVE: GTC limit order ─────────────────────────────────────
    try:
        # LOT_SIZE floor: Binance stepSize compliance
        limit_qty = engine.exchange.floor_amount(base_sym, quote / target_price)
        if limit_qty <= 0:
            logger.warning(f"[WINGS] ZERO_QTY | {wings_sym} quote={quote} price={target_price:.4f}")
            return

        order    = engine.exchange.place_limit_buy(base_sym, limit_qty, target_price)
        order_id = str(order.get("id", ""))
        if not order_id:
            raise ValueError(f"no order_id from Binance for {wings_sym}")

        # register to pending tracker
        with _pending_wings_lock:
            _pending_wings_orders[order_id] = {
                "exchange_sym":    base_sym,
                "wings_sym":       wings_sym,
                "target_price":    target_price,
                "wings_quote":     quote,
                "tp_pct":          tp_pct,
                "max_add_ons":     max_add_ons,
                "max_capital":     max_capital,
                "signal_id":       signal_id,
                "opened_at":       time.time(),
                "trigger_drop_pct": trigger_drop_pct,
            }

        logger.warning(
            f"[WINGS] LIVE_LIMIT_PLACED | {wings_sym} "
            f"target={target_price:.4f} qty={limit_qty:.6f} "
            f"order_id={order_id} "
            f"timeout={os.getenv('WINGS_TIMEOUT_SECONDS', '300')}s"
        )
        try:
            log_event(
                "WINGS_LIMIT_PLACED",
                f"sym={wings_sym} target={target_price:.4f} "
                f"qty={limit_qty:.6f} order_id={order_id}"
            )
        except Exception:
            pass
        try:
            from execution.telegram_notifier import send_telegram_message
            _dir = "UP" if trigger_drop_pct > 0 else "DOWN"
            send_telegram_message(
                f"📋 <b>WINGS Limit განთავსდა</b>\n\n"
                f"🪙 <b>Symbol:</b> <code>{wings_sym}</code>\n"
                f"🎯 <b>Target:</b> <code>{target_price:.4f}</code> "
                f"(<code>{('+ ' if trigger_drop_pct>=0 else '')}{trigger_drop_pct:.4f}%</code> from L1)\n"
                f"📍 <b>Direction:</b> <code>{_dir}</code>\n"
                f"💵 <b>Quote:</b> <code>{quote} USDT</code>\n"
                f"⏱ <b>Timeout:</b> <code>{os.getenv('WINGS_TIMEOUT_SECONDS', '300')}s</code>\n"
                f"🕒 <code>{_now_dt().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )
        except Exception:
            pass

    except ExchangeClientError:
        raise
    except Exception as _le:
        logger.error(f"[WINGS] LIMIT_FAIL | {wings_sym} err={_le}")
        raise ExchangeClientError(f"[WINGS] Limit order failed for {wings_sym}: {_le}")



def _open_wings_market(
    engine,
    base_sym: str,
    l1_price: float,
    wings_sym: str,
    quote: float,
    signal_id: str,
    tp_pct: float,
    max_add_ons: int,
    max_capital: float,
    trigger_drop_pct: float = 0.0,
) -> None:
    """
    PHANTOM WINGS market buy — LW3-LW6 + LP1-LP2.

    DEMO: ვირტუალური entry @ current L1 × (1 - |trigger_drop_pct|/100)
    LIVE: market buy @ Binance → immediate fill
          open_dca_position + add_dca_order + open_trade

    Args:
      engine:           ExecutionEngine instance
      base_sym:         "BTC/USDT" (without suffix)
      l1_price:         L1 avg_entry_price (target price reference)
      wings_sym:        full symbol, e.g. "BTC/USDT_LW3"
      quote:            USDT to spend ($30 for LW3-LW6, LP1-LP2)
      signal_id:        parent signal ID
      tp_pct:           TP percent (DCA_TP_PCT = 0.55%)
      max_add_ons:      DCA_MAX_ADD_ONS
      max_capital:      DCA_MAX_CAPITAL_USDT
      trigger_drop_pct: pct below L1 (negative = below L1)
    """
    from execution.db.repository import (
        open_dca_position, add_dca_order, open_trade,
        get_open_dca_position_for_symbol, log_event,
    )

    # double-open guard (DB)
    if get_open_dca_position_for_symbol(wings_sym):
        logger.debug(f"[WINGS_MKT] ALREADY_OPEN | {wings_sym} → skip")
        return

    is_live = engine.exchange is not None

    # ── DEMO ──────────────────────────────────────────────────────
    if not is_live:
        buy_price = l1_price  # market fill ≈ current L1 price (DEMO)
        buy_qty   = quote / buy_price if buy_price > 0 else 0.0
        if buy_qty <= 0:
            return
        tp_price = round(buy_price * (1.0 + tp_pct / 100.0), 6)

        pos_id = open_dca_position(
            symbol=wings_sym,
            initial_entry_price=buy_price,
            initial_qty=buy_qty,
            initial_quote_spent=quote,
            tp_price=tp_price,
            sl_price=0.0,
            tp_pct=tp_pct,
            sl_pct=999.0,
            max_add_ons=max_add_ons,
            max_capital=max_capital,
            max_drawdown_pct=999.0,
        )
        add_dca_order(
            position_id=pos_id,
            symbol=wings_sym,
            order_type="WINGS_MKT_INITIAL",
            entry_price=buy_price,
            qty=buy_qty,
            quote_spent=quote,
            avg_entry_after=buy_price,
            tp_after=tp_price,
            sl_after=0.0,
            trigger_drawdown_pct=abs(trigger_drop_pct),
            exchange_order_id=f"DEMO-{signal_id}",
        )
        open_trade(
            signal_id=signal_id,
            symbol=wings_sym,
            qty=buy_qty,
            quote_in=quote,
            entry_price=buy_price,
        )
        logger.warning(
            f"[WINGS_MKT] DEMO_OPENED | {wings_sym} "
            f"entry={buy_price:.4f} (L1={l1_price:.4f} "
            f"{trigger_drop_pct:.4f}%) "
            f"tp={tp_price:.4f} quote={quote}"
        )
        try:
            log_event(
                "WINGS_MKT_OPENED_DEMO",
                f"sym={wings_sym} entry={buy_price:.4f} tp={tp_price:.4f} "
                f"l1={l1_price:.4f} trigger_pct={trigger_drop_pct:.4f}"
            )
        except Exception:
            pass
        try:
            from execution.telegram_notifier import notify_signal_created
            notify_signal_created(
                symbol=wings_sym,
                entry_price=buy_price,
                quote_amount=quote,
                tp_price=tp_price,
                sl_price=0.0,
                verdict="WINGS_MKT_BUY",
                mode="DEMO",
            )
        except Exception as _tg:
            logger.warning(f"[WINGS_MKT] TG_FAIL | {wings_sym} err={_tg}")
        return

    # ── LIVE: market buy ──────────────────────────────────────────
    try:
        buy      = engine.exchange.place_market_buy_by_quote(base_sym, quote)
        buy_price = float(buy.get("average") or buy.get("price") or l1_price)
        buy_qty   = float(buy.get("filled") or buy.get("amount") or (quote / buy_price))

        if buy_qty <= 0 or buy_price <= 0:
            logger.warning(f"[WINGS_MKT] ZERO_FILL | {wings_sym} qty={buy_qty} price={buy_price}")
            return

        actual_quote = buy_qty * buy_price
        tp_price     = round(buy_price * (1.0 + tp_pct / 100.0), 6)

        pos_id = open_dca_position(
            symbol=wings_sym,
            initial_entry_price=buy_price,
            initial_qty=buy_qty,
            initial_quote_spent=actual_quote,
            tp_price=tp_price,
            sl_price=0.0,
            tp_pct=tp_pct,
            sl_pct=999.0,
            max_add_ons=max_add_ons,
            max_capital=max_capital,
            max_drawdown_pct=999.0,
        )
        add_dca_order(
            position_id=pos_id,
            symbol=wings_sym,
            order_type="WINGS_MKT_INITIAL",
            entry_price=buy_price,
            qty=buy_qty,
            quote_spent=actual_quote,
            avg_entry_after=buy_price,
            tp_after=tp_price,
            sl_after=0.0,
            trigger_drawdown_pct=abs(trigger_drop_pct),
            exchange_order_id=str(buy.get("id", "")),
        )
        open_trade(
            signal_id=signal_id,
            symbol=wings_sym,
            qty=buy_qty,
            quote_in=actual_quote,
            entry_price=buy_price,
        )

        logger.warning(
            f"[WINGS_MKT] LIVE_OPENED | {wings_sym} "
            f"@ {buy_price:.4f} qty={buy_qty:.6f} "
            f"actual_quote={actual_quote:.2f} tp={tp_price:.4f}"
        )
        try:
            log_event(
                "WINGS_MKT_OPENED",
                f"sym={wings_sym} entry={buy_price:.4f} qty={buy_qty:.6f} "
                f"quote={actual_quote:.2f} tp={tp_price:.4f} "
                f"l1={l1_price:.4f} trigger_pct={trigger_drop_pct:.4f}"
            )
        except Exception:
            pass
        try:
            from execution.telegram_notifier import notify_signal_created
            notify_signal_created(
                symbol=wings_sym,
                entry_price=buy_price,
                quote_amount=actual_quote,
                tp_price=tp_price,
                sl_price=0.0,
                verdict="WINGS_MKT_BUY",
                mode="LIVE",
            )
        except Exception as _tg:
            logger.warning(f"[WINGS_MKT] TG_FAIL | {wings_sym} err={_tg}")

    except ExchangeClientError:
        raise
    except Exception as _me:
        logger.error(f"[WINGS_MKT] FAIL | {wings_sym} err={_me}")
        raise ExchangeClientError(f"[WINGS_MKT] Market buy failed for {wings_sym}: {_me}")


def _check_and_open_wings(engine, tp_pct: float, max_add_ons: int, max_capital: float) -> None:
    """
    PHANTOM WINGS loop check — ყოველ main loop-ზე (120s).

    L1 ღიაა → ყველა LW + LP level-ისთვის:
      LW1/LW2 (UP):   limit order → _open_wings_level() (ჯავშანი)
      LW3-LW6 (DOWN): market buy  → _open_wings_market() ($30, qty×2)
      LP1/LP2:        market buy  → _open_wings_market() ($30, qty×2)

    Symbol-ები per-level: WINGS_SYMBOLS_UP/DOWN/LP ENV-ით
      LW1 → WINGS_SYMBOLS_UP[0], LW2 → WINGS_SYMBOLS_UP[1]
      LW3 → WINGS_SYMBOLS_DOWN[0], ... LW6 → WINGS_SYMBOLS_DOWN[3]
      LP1 → WINGS_SYMBOLS_LP[0], LP2 → WINGS_SYMBOLS_LP[1]
      default (თუ ENV არ არის): BOT_SYMBOLS round-robin

    ENV:
      WINGS_ENABLED=true
      WINGS_LEVELS_UP=0.05,0.05
      WINGS_LEVELS_DOWN=0.05,0.05,0.10,0.10
      WINGS_LP_LEVELS=0.05,0.10
      WINGS_QUOTES=15,15,30,30,30,30
      WINGS_LP_QUOTES=30,30
      WINGS_SYMBOLS_UP=BTC/USDT,ETH/USDT
      WINGS_SYMBOLS_DOWN=BTC/USDT,ETH/USDT,BTC/USDT,BNB/USDT
      WINGS_SYMBOLS_LP=BTC/USDT,ETH/USDT
    """
    if os.getenv("WINGS_ENABLED", "false").strip().lower() not in ("1", "true", "yes"):
        return

    from execution.db.repository import (
        get_all_open_dca_positions, get_open_dca_position_for_symbol,
    )
    import uuid as _uuid_w

    # ── parse ENV ─────────────────────────────────────────────────
    try:
        levels_up = [float(x.strip()) for x in
                     os.getenv("WINGS_LEVELS_UP", "0.05,0.05").split(",") if x.strip()]
    except Exception:
        levels_up = [0.05, 0.05]

    try:
        levels_dn = [float(x.strip()) for x in
                     os.getenv("WINGS_LEVELS_DOWN", "0.05,0.05,0.10,0.10").split(",") if x.strip()]
    except Exception:
        levels_dn = [0.05, 0.05, 0.10, 0.10]

    try:
        lp_levels = [float(x.strip()) for x in
                     os.getenv("WINGS_LP_LEVELS", "0.05,0.10").split(",") if x.strip()]
    except Exception:
        lp_levels = [0.05, 0.10]

    n_wings = len(levels_up) + len(levels_dn)

    try:
        wings_quotes = [float(x.strip()) for x in
                        os.getenv("WINGS_QUOTES", ",".join(["15"] * n_wings)).split(",") if x.strip()]
    except Exception:
        wings_quotes = [15.0] * n_wings
    while len(wings_quotes) < n_wings:
        wings_quotes.append(wings_quotes[-1] if wings_quotes else 15.0)

    try:
        lp_quotes = [float(x.strip()) for x in
                     os.getenv("WINGS_LP_QUOTES", ",".join(["30"] * len(lp_levels))).split(",") if x.strip()]
    except Exception:
        lp_quotes = [30.0] * len(lp_levels)
    while len(lp_quotes) < len(lp_levels):
        lp_quotes.append(lp_quotes[-1] if lp_quotes else 30.0)

    # ── per-level symbol lists (FIX #26) ──────────────────────────
    # WINGS_SYMBOLS_UP/DOWN/LP: comma-separated, one symbol per level
    # default: BOT_SYMBOLS round-robin (cycle through available symbols)
    _bot_syms = [s.strip() for s in
                 os.getenv("BOT_SYMBOLS", "BTC/USDT,ETH/USDT,BNB/USDT").split(",") if s.strip()]

    def _parse_sym_list(env_key: str, count: int) -> list:
        """Parse symbol list from ENV, fallback to BOT_SYMBOLS round-robin."""
        raw = os.getenv(env_key, "").strip()
        if raw:
            syms = [s.strip() for s in raw.split(",") if s.strip()]
        else:
            syms = []
        # fill missing slots with round-robin from _bot_syms
        result = []
        for i in range(count):
            if i < len(syms):
                result.append(syms[i])
            else:
                result.append(_bot_syms[i % len(_bot_syms)] if _bot_syms else "BTC/USDT")
        return result

    syms_up = _parse_sym_list("WINGS_SYMBOLS_UP", len(levels_up))
    syms_dn = _parse_sym_list("WINGS_SYMBOLS_DOWN", len(levels_dn))
    syms_lp = _parse_sym_list("WINGS_SYMBOLS_LP", len(lp_levels))

    # L1 lookup: BTC/USDT signal drives wings — L1 reference from BOT_SYMBOLS[0]
    # (L1 avg_entry_price used for TP calculation per-position)
    try:
        all_open = get_all_open_dca_positions() or []
    except Exception as _e:
        logger.warning(f"[WINGS] DB_FAIL | err={_e}")
        return

    # Build L1 price map: {base_sym: avg_entry_price}
    _l1_price_map = {}
    for _p in all_open:
        _psym = str(_p.get("symbol", ""))
        # Only exact L1 (no suffix)
        import re as _re_l1chk
        if not _re_l1chk.search(r'(_L\d+|_LP\d*|_LW\d*)$', _psym):
            _l1_price_map[_psym] = float(_p.get("avg_entry_price") or 0.0)

    if not _l1_price_map:
        logger.debug("[WINGS] NO_L1_ANY | no L1 positions open → skip all wings")
        return

    # Use first available L1 as reference price anchor
    # (each wing uses its own symbol's L1 price if available, else first L1)
    _first_l1_sym = next(iter(_l1_price_map))
    _first_l1_avg = _l1_price_map[_first_l1_sym]

    w_signal_id = f"W-{_first_l1_sym.replace('/', '')}-{_uuid_w.uuid4().hex[:8]}"

    def _get_l1_price(sym: str) -> float:
        """Get L1 avg_entry_price for sym, fallback to first available L1."""
        return _l1_price_map.get(sym, _first_l1_avg)

    # ── LW ზემოთ (UP): LW1, LW2, ... ─────────────────────────────
    q_idx = 0
    for idx, pct in enumerate(levels_up, start=1):
        wing_base = syms_up[idx - 1]          # e.g. "BTC/USDT"
        sym_lw    = f"{wing_base}_LW{idx}"    # e.g. "BTC/USDT_LW1"
        l1_ref    = _get_l1_price(wing_base)
        target    = round(l1_ref * (1.0 + pct / 100.0), 8)
        quote     = wings_quotes[q_idx] if q_idx < len(wings_quotes) else 15.0
        q_idx    += 1

        try:
            _open_wings_level(
                engine=engine,
                base_sym=wing_base,
                l1_price=l1_ref,
                wings_sym=sym_lw,
                target_price=target,
                quote=quote,
                signal_id=f"LW{idx}-{w_signal_id}",
                tp_pct=tp_pct,
                max_add_ons=max_add_ons,
                max_capital=max_capital,
                trigger_drop_pct=pct,          # positive = above L1
            )
        except ExchangeClientError as _ece:
            logger.warning(f"[WINGS] LW_UP_FAIL | {sym_lw} err={_ece}")
        except Exception as _e:
            logger.warning(f"[WINGS] LW_UP_ERR | {sym_lw} err={_e}")

    # ── LW ქვემოთ (DOWN): LW3, LW4, ... → market buy ─────────────
    for jdx, pct in enumerate(levels_dn, start=len(levels_up) + 1):
        dn_idx    = jdx - len(levels_up) - 1  # 0-based index into syms_dn
        wing_base = syms_dn[dn_idx]            # e.g. "ETH/USDT"
        sym_lw    = f"{wing_base}_LW{jdx}"    # e.g. "ETH/USDT_LW3"
        l1_ref    = _get_l1_price(wing_base)
        quote     = wings_quotes[q_idx] if q_idx < len(wings_quotes) else 30.0
        q_idx    += 1

        try:
            _open_wings_market(
                engine=engine,
                base_sym=wing_base,
                l1_price=l1_ref,
                wings_sym=sym_lw,
                quote=quote,
                signal_id=f"LW{jdx}-{w_signal_id}",
                tp_pct=tp_pct,
                max_add_ons=max_add_ons,
                max_capital=max_capital,
                trigger_drop_pct=-pct,         # negative = below L1
            )
        except ExchangeClientError as _ece:
            logger.warning(f"[WINGS] LW_DN_FAIL | {sym_lw} err={_ece}")
        except Exception as _e:
            logger.warning(f"[WINGS] LW_DN_ERR | {sym_lw} err={_e}")

    # ── LP phantom → market buy ────────────────────────────────────
    for kdx, pct in enumerate(lp_levels, start=1):
        wing_base = syms_lp[kdx - 1]          # e.g. "ETH/USDT"
        sym_lp    = f"{wing_base}_LP{kdx}"    # e.g. "ETH/USDT_LP1"
        l1_ref    = _get_l1_price(wing_base)
        quote     = lp_quotes[kdx - 1] if (kdx - 1) < len(lp_quotes) else 30.0

        try:
            _open_wings_market(
                engine=engine,
                base_sym=wing_base,
                l1_price=l1_ref,
                wings_sym=sym_lp,
                quote=quote,
                signal_id=f"LP{kdx}-{w_signal_id}",
                tp_pct=tp_pct,
                max_add_ons=max_add_ons,
                max_capital=max_capital,
                trigger_drop_pct=-pct,
            )
        except ExchangeClientError as _ece:
            logger.warning(f"[WINGS] LP_FAIL | {sym_lp} err={_ece}")
        except Exception as _e:
            logger.warning(f"[WINGS] LP_ERR | {sym_lp} err={_e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LAYER2 — Crash Detection & Parallel Trading
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _check_and_open_layer2(engine, tp_sl_mgr) -> None:
    """
    Layer 2 — Crash Detection & Parallel Trading.

    ლოგიკა:
      1. თითო symbol-ისთვის 24h HIGH ამოიღე
      2. თუ current_price <= HIGH × (1 - LAYER2_DROP_PCT/100) → crash!
      3. Layer 2 პოზიცია უკვე ღიაა? → გამოტოვე
      4. ბალანსი საკმარისია? → გახსენი Layer 2

    ENV:
      LAYER2_ENABLED=true
      LAYER2_DROP_PCT=1.5
      LAYER2_QUOTE=12.0
      LAYER2_SYMBOLS=BTC/USDT,...
      LAYER2_DEMO_ENABLED=false
      SMART_ADDON_BUFFER=12
    """
    if not os.getenv("LAYER2_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return

    mode = os.getenv("MODE", "DEMO").upper()
    is_live = engine.exchange is not None

    demo_enabled = os.getenv("LAYER2_DEMO_ENABLED", "false").strip().lower() in ("1", "true", "yes")
    if not is_live and not demo_enabled:
        logger.debug("[LAYER2] DEMO mode → skipped (set LAYER2_DEMO_ENABLED=true to enable)")
        return

    from execution.db.repository import (
        get_open_dca_position_for_symbol,
        open_dca_position,
        add_dca_order,
        open_trade,
        log_event,
    )
    import uuid as _uuid_l2

    drop_pct    = float(os.getenv("LAYER2_DROP_PCT",  "1.5"))
    quote       = float(os.getenv("LAYER2_QUOTE",     "12.0"))
    symbols_raw = os.getenv("LAYER2_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols     = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    tp_pct      = float(os.getenv("DCA_TP_PCT", "0.55"))
    buffer      = float(os.getenv("SMART_ADDON_BUFFER", "12.0"))

    if is_live:
        try:
            free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
        except Exception as _e:
            logger.warning(f"[LAYER2] balance_fetch_fail | err={_e}")
            return
    else:
        try:
            from execution.db.repository import get_all_open_dca_positions
            _initial  = float(os.getenv("DEMO_INITIAL_BALANCE", "3200.0"))
            _open_pos = get_all_open_dca_positions() or []
            _invested = sum(float(p.get("total_quote_spent", 0.0)) for p in _open_pos)
            free_usdt = max(_initial - _invested, 0.0)
        except Exception as _e:
            free_usdt = float(os.getenv("DEMO_INITIAL_BALANCE", "3200.0"))

    for sym in symbols:
        try:
            if is_live:
                current_price = float(engine.exchange.fetch_last_price(sym) or 0.0)
            else:
                _ticker = engine.price_feed.fetch_ticker(sym)
                current_price = float(_ticker.get("last") or 0.0)

            if current_price <= 0:
                continue

            # FIX #22: L1-based trigger (ნაცვლად 24h HIGH-based)
            # პრობლემა: 24h HIGH spike → L2 trigger L1-ზე მაღლა →
            #           L2 არასოდეს trigger-ს (edge case bug)
            # გამოსწორება: L1 avg_entry_price-დან LAYER2_DROP_PCT% ქვემოთ
            # L1 არ არის? → skip (L2 L1-ის გარეშე აზრი არ აქვს)
            try:
                from execution.db.repository import (
                    get_open_dca_position_for_symbol as _get_l1_for_l2,
                )
                _l1_pos = _get_l1_for_l2(sym)
            except Exception:
                _l1_pos = None

            if not _l1_pos:
                logger.debug(f"[LAYER2] NO_L1 | {sym} → L2 skip (no L1 open)")
                continue

            l1_entry = float(_l1_pos.get("avg_entry_price") or 0.0)
            if l1_entry <= 0:
                logger.debug(f"[LAYER2] INVALID_L1_ENTRY | {sym} l1_entry={l1_entry} → skip")
                continue

            drop_from_l1 = (l1_entry - current_price) / l1_entry * 100.0

            logger.info(
                f"[LAYER2] CHECK | {sym} price={current_price:.4f} "
                f"l1_entry={l1_entry:.4f} drop_from_l1={drop_from_l1:.2f}% "
                f"trigger={drop_pct:.1f}% mode={mode}"
            )

            if drop_from_l1 < drop_pct:
                logger.debug(f"[LAYER2] NO_CRASH | {sym} drop={drop_from_l1:.2f}% < {drop_pct:.1f}%")
                continue

            sym_l2 = f"{sym}_L2"
            existing_l2 = get_open_dca_position_for_symbol(sym_l2)
            if existing_l2:
                logger.debug(f"[LAYER2] ALREADY_OPEN | {sym_l2}")
                continue

            required = quote + buffer
            if free_usdt < required:
                logger.warning(
                    f"[LAYER2] INSUFFICIENT_BALANCE | {sym} "
                    f"free={free_usdt:.2f} < required={required:.2f}"
                )
                continue

            logger.warning(
                f"[LAYER2] CRASH_DETECTED | {sym} "
                f"drop_from_l1={drop_from_l1:.2f}% >= {drop_pct:.1f}% → opening Layer 2 [{mode}]"
            )

            if is_live:
                buy = engine.exchange.place_market_buy_by_quote(sym, quote)
                buy_price = float(buy.get("average") or buy.get("price") or current_price)
                buy_qty   = float(buy.get("filled") or buy.get("amount") or (quote / buy_price))
            else:
                buy_price = current_price
                buy_qty   = quote / buy_price
                buy       = {"average": buy_price, "filled": buy_qty, "id": ""}

            tp_price = round(buy_price * (1.0 + tp_pct / 100.0), 6)

            pos_id = open_dca_position(
                symbol=sym_l2,
                initial_entry_price=buy_price,
                initial_qty=buy_qty,
                initial_quote_spent=quote,
                tp_price=tp_price,
                sl_price=0.0,
                tp_pct=tp_pct,
                sl_pct=999.0,
                max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "5")),
                max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "350.0")),
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
                trigger_drawdown_pct=drop_from_l1,
                exchange_order_id=str(buy.get("id", "")),
            )

            l2_signal_id = f"L2-{sym.replace('/', '')}-{_uuid_l2.uuid4().hex[:8]}"
            open_trade(
                signal_id=l2_signal_id,
                symbol=sym_l2,
                qty=buy_qty,
                quote_in=quote,
                entry_price=buy_price,
            )

            free_usdt -= quote

            try:
                log_event(
                    "LAYER2_OPENED",
                    f"sym={sym_l2} entry={buy_price:.4f} "
                    f"tp={tp_price:.4f} drop_from_l1={drop_from_l1:.2f}% "
                    f"pos_id={pos_id} mode={mode}"
                )
            except Exception:
                pass

            try:
                from execution.telegram_notifier import notify_signal_created
                notify_signal_created(
                    symbol=sym_l2,
                    entry_price=buy_price,
                    quote_amount=quote,
                    tp_price=tp_price,
                    sl_price=0.0,
                    verdict="LAYER2_BUY",
                    mode=mode,
                )
            except Exception as _tg:
                logger.warning(f"[LAYER2] TG_FAIL | err={_tg}")

            logger.warning(
                f"[LAYER2] OPENED | {sym_l2} entry={buy_price:.4f} "
                f"tp={tp_price:.4f} quote={quote} [{mode}]"
            )

        except Exception as e:
            logger.error(f"[LAYER2] ERR | {sym} err={e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CASCADE — Rolling Exchange სტრატეგია
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _check_cascade_exchange(engine, tp_sl_mgr) -> None:
    """
    Cascade DCA — Rolling Exchange სტრატეგია.

    FIX #20: sym_positions filter-ში _LP გამორიცხულია.
    LP-ს საკუთარი lifecycle აქვს — CASCADE-მა არ უნდა გაყიდოს.
    exchange_sym extraction regex: (_L[0-9]+|_LP)$ ნაცვლად _L[0-9]+$
    """
    if not os.getenv("CASCADE_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        return

    mode    = os.getenv("MODE", "DEMO").upper()
    is_live = engine.exchange is not None

    demo_enabled = os.getenv("CASCADE_DEMO_ENABLED", "false").strip().lower() in ("1", "true", "yes")
    if not is_live and not demo_enabled:
        logger.debug("[CASCADE] DEMO mode → skipped (set CASCADE_DEMO_ENABLED=true to enable)")
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
    import uuid as _uuid_cas
    import re as _re_cas

    cascade_start = int(os.getenv("CASCADE_START_LAYER",  "2"))
    drop_pct_base = float(os.getenv("CASCADE_DROP_PCT",    "1.5"))
    drop_pct_l4   = float(os.getenv("CASCADE_DROP_L4_PCT", "2.0"))
    drop_pct_l8   = float(os.getenv("CASCADE_DROP_L8_PCT", "5.0"))
    tp_pct_base   = float(os.getenv("DCA_TP_PCT",          "0.55"))
    tp_pct_l3     = float(os.getenv("CASCADE_TP_L3_PCT",   "0.65"))
    tp_pct_l8     = float(os.getenv("CASCADE_TP_L8_PCT",   "1.00"))
    max_layers    = int(os.getenv("CASCADE_MAX_LAYERS",    "10"))
    resume_layer  = int(os.getenv("CASCADE_RESUME_LAYER",  "10"))
    symbols_raw   = os.getenv("CASCADE_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT")
    symbols       = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    buffer        = float(os.getenv("SMART_ADDON_BUFFER", "12.0"))

    all_positions = get_all_open_dca_positions()
    total_layers  = len(all_positions)

    logger.info(
        f"[CASCADE] CHECK | total_layers={total_layers} "
        f"start_at={cascade_start} max={max_layers} resume_at={resume_layer} mode={mode}"
    )

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

            if is_live:
                current_price = float(engine.exchange.fetch_last_price(exchange_sym) or 0.0)
            else:
                _ticker = engine.price_feed.fetch_ticker(exchange_sym)
                current_price = float(_ticker.get("last") or 0.0)

            if current_price <= 0:
                continue

            # FIX #20: _LP suffix-ი CASCADE-ის sym_positions-დან გამორიცხულია.
            # LP-ს საკუთარი TP/FC lifecycle აქვს _run_dca_loop-ში.
            # CASCADE-მა LP არ უნდა გაყიდოს "oldest" სახით.
            sym_positions = [
                p for p in all_positions
                if (
                    _re_cas.sub(r'(_L\d+|_LP\d*|_LW\d*)$', '', str(p.get("symbol", "")).upper()) == sym.upper()
                    and not bool(__import__("re").search(r"(_LP\d*|_LW\d*)$", str(p.get("symbol", "")).upper()))
                )
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

            layer_num = len(sym_positions)

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
                f"price={current_price:.4f} drop={drop_from_newest:.2f}% "
                f"trigger={drop_pct:.1f}% tp={tp_pct:.2f}% mode={mode}"
            )

            if drop_from_newest < drop_pct:
                logger.debug(f"[CASCADE] {sym} | drop={drop_from_newest:.2f}% < {drop_pct:.1f}% → wait")
                continue

            if is_live:
                try:
                    free_usdt = float(engine.exchange.fetch_balance_free("USDT") or 0.0)
                except Exception:
                    free_usdt = 0.0
            else:
                try:
                    _initial  = float(os.getenv("DEMO_INITIAL_BALANCE", "3200.0"))
                    _invested = sum(float(p.get("total_quote_spent", 0.0)) for p in all_positions)
                    free_usdt = max(_initial - _invested, 0.0)
                except Exception:
                    free_usdt = float(os.getenv("DEMO_INITIAL_BALANCE", "3200.0"))

            if free_usdt < buffer:
                logger.warning(f"[CASCADE] {sym} | low_balance={free_usdt:.2f} < buffer={buffer:.1f}")
                continue

            logger.warning(
                f"[CASCADE] EXCHANGE | {oldest_sym} avg={oldest_avg:.4f} "
                f"qty={oldest_qty:.6f} drop={drop_from_newest:.2f}% [{mode}]"
            )

            # ── ძველი Layer-ის გაყიდვა ──────────────────────────────
            try:
                if is_live:
                    sell = engine.exchange.place_market_sell(exchange_sym, oldest_qty)
                    sell_price = float(sell.get("average") or sell.get("price") or current_price)
                else:
                    sell_price = current_price
                    sell = {"average": sell_price, "price": sell_price}

                proceeds     = sell_price * oldest_qty
                fee          = proceeds * 0.001
                net_proceeds = round(proceeds - fee, 4)

                pnl_quote = (sell_price - oldest_avg) * oldest_qty
                pnl_pct   = (sell_price / oldest_avg - 1.0) * 100.0

                close_dca_position(
                    oldest_id, sell_price, oldest_qty,
                    pnl_quote, pnl_pct, "CASCADE_EXCHANGE"
                )

                open_tr = get_open_trade_for_symbol(oldest_sym)
                if not open_tr:
                    open_tr = get_open_trade_for_symbol(exchange_sym)
                if not open_tr:
                    base = exchange_sym.replace("/USDT", "")
                    for suffix in ["", "_L2", "_L3", "_L4", "_L5",
                                   "_L6", "_L7", "_L8", "_L9", "_L10"]:
                        _tr = get_open_trade_for_symbol(f"{base}/USDT{suffix}")
                        if _tr:
                            open_tr = _tr
                            break
                if open_tr:
                    close_trade(open_tr[0], sell_price, "CASCADE_EXCHANGE", pnl_quote, pnl_pct)
                    logger.info(f"[CASCADE] TRADE_CLOSED | {oldest_sym} signal_id={open_tr[0]}")
                else:
                    logger.warning(f"[CASCADE] TRADE_NOT_FOUND | {oldest_sym}")

                logger.warning(
                    f"[CASCADE] SOLD | {oldest_sym} price={sell_price:.4f} "
                    f"proceeds={net_proceeds:.4f} pnl={pnl_quote:+.4f} [{mode}]"
                )

                try:
                    from execution.telegram_notifier import notify_cascade_exchange
                    _new_layer_name = f"{sym}_L{layer_num + 1}"
                    notify_cascade_exchange(
                        symbol=sym,
                        old_avg=oldest_avg,
                        old_layer=oldest_sym,
                        new_avg=current_price,
                        new_layer=_new_layer_name,
                        sell_price=sell_price,
                        pnl_quote=pnl_quote,
                        drop_pct=drop_from_newest,
                        new_tp=round(current_price * (1.0 + tp_pct / 100.0), 6),
                    )
                except Exception as _tg_sell:
                    logger.warning(f"[CASCADE] TG_SELL_FAIL | err={_tg_sell}")

            except Exception as _se:
                logger.error(f"[CASCADE] SELL_FAIL | {oldest_sym} err={_se}")
                continue

            if net_proceeds < 5.0:
                logger.warning(f"[CASCADE] LOW_PROCEEDS | {net_proceeds:.4f} < $5 → skip new layer")
                continue

            new_sym   = f"{sym}_L{layer_num + 1}"
            buy_quote = float(os.getenv("BOT_QUOTE_PER_TRADE", "15.0"))

            try:
                if is_live:
                    buy = engine.exchange.place_market_buy_by_quote(exchange_sym, buy_quote)
                    buy_price = float(buy.get("average") or buy.get("price") or current_price)
                    buy_qty   = float(buy.get("filled") or buy.get("amount") or (buy_quote / buy_price))
                else:
                    buy_price = current_price
                    buy_qty   = buy_quote / buy_price
                    buy       = {"average": buy_price, "filled": buy_qty, "id": ""}

                tp_price = round(buy_price * (1.0 + tp_pct / 100.0), 6)

                pos_id = open_dca_position(
                    symbol=new_sym,
                    initial_entry_price=buy_price,
                    initial_qty=buy_qty,
                    initial_quote_spent=buy_quote,
                    tp_price=tp_price,
                    sl_price=0.0,
                    tp_pct=tp_pct,
                    sl_pct=999.0,
                    max_add_ons=int(os.getenv("DCA_MAX_ADD_ONS", "5")),
                    max_capital=float(os.getenv("DCA_MAX_CAPITAL_USDT", "350.0")),
                    max_drawdown_pct=999.0,
                )

                add_dca_order(
                    position_id=pos_id,
                    symbol=new_sym,
                    order_type="CASCADE_LAYER",
                    entry_price=buy_price,
                    qty=buy_qty,
                    quote_spent=buy_quote,
                    avg_entry_after=buy_price,
                    tp_after=tp_price,
                    sl_after=0.0,
                    trigger_drawdown_pct=drop_from_newest,
                    exchange_order_id=str(buy.get("id", "")),
                )

                cascade_signal_id = f"CAS-{sym.replace('/', '')}-{_uuid_cas.uuid4().hex[:8]}"
                open_trade(
                    signal_id=cascade_signal_id,
                    symbol=new_sym,
                    qty=buy_qty,
                    quote_in=buy_quote,
                    entry_price=buy_price,
                )

                try:
                    log_event(
                        "CASCADE_LAYER_OPENED",
                        f"sym={new_sym} entry={buy_price:.4f} tp={tp_price:.4f} "
                        f"quote={buy_quote:.4f} from={oldest_sym} mode={mode}"
                    )
                except Exception:
                    pass

                try:
                    from execution.telegram_notifier import notify_signal_created
                    notify_signal_created(
                        symbol=new_sym,
                        entry_price=buy_price,
                        quote_amount=buy_quote,
                        tp_price=tp_price,
                        sl_price=0.0,
                        verdict="CASCADE_BUY",
                        mode=mode,
                    )
                except Exception as _tg:
                    logger.warning(f"[CASCADE] TG_FAIL | err={_tg}")

                logger.warning(
                    f"[CASCADE] NEW_LAYER | {new_sym} entry={buy_price:.4f} "
                    f"tp={tp_price:.4f} quote={buy_quote:.4f} [{mode}]"
                )

                new_layer_num = layer_num + 1
                _warn_from = int(os.getenv("CASCADE_WARN_FROM_LAYER", "7"))
                if new_layer_num >= _warn_from:
                    try:
                        from execution.telegram_notifier import notify_cascade_depth
                        if len(sym_positions) >= 2:
                            _sorted = sorted(sym_positions, key=lambda p: str(p.get("opened_at", "")))
                            _first_avg = float(_sorted[0].get("avg_entry_price", 0))
                            _last_avg  = float(_sorted[-1].get("avg_entry_price", 0))
                            if _last_avg < _first_avg * 0.998:
                                _trend = "down"
                            elif _last_avg > _first_avg * 1.002:
                                _trend = "up"
                            else:
                                _trend = "sideways"
                        else:
                            _trend = "unknown"

                        try:
                            _ticker_h = engine.price_feed.fetch_ticker(sym)
                            _high24   = float(_ticker_h.get("high") or 0.0)
                            _drop_h   = ((_high24 - buy_price) / _high24 * 100.0) if _high24 > 0 else 0.0
                        except Exception:
                            _drop_h = drop_from_newest

                        notify_cascade_depth(
                            symbol=sym,
                            layer_num=new_layer_num,
                            max_layers=max_layers,
                            drop_from_high_pct=_drop_h,
                            current_price=buy_price,
                            avg_entry=buy_price,
                            price_trend=_trend,
                        )
                    except Exception as _cwe:
                        logger.warning(f"[CASCADE] DEPTH_WARN_FAIL | err={_cwe}")

            except Exception as _be:
                logger.error(f"[CASCADE] BUY_FAIL | {new_sym} err={_be}")

        except Exception as e:
            logger.error(f"[CASCADE] ERR | {sym} err={e}")


def _start_bot_api_server() -> None:
    """
    Bot API Server — Dashboard-ისთვის DB data-ს აბრუნებს.
    GET /api/stats  → positions + trades + stats JSON
    GET /health     → liveness check
    """
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
                get_trade_stats,
                get_all_open_dca_positions,
                get_closed_trades,
            )
            stats     = get_trade_stats()
            positions = get_all_open_dca_positions()
            trades    = get_closed_trades()
            recent = sorted(
                [t for t in trades if t.get("outcome")],
                key=lambda x: str(x.get("closed_at", "")),
                reverse=True,
            )[:20]
            return _jsonify({
                "stats":         stats,
                "positions":     positions,
                "recent_trades": recent,
                "timestamp":     _dt.now(_tz.utc).isoformat(),
            })
        except Exception as e:
            logger.error(f"[BOT_API] stats error: {e}")
            return _jsonify({"error": str(e)}), 500

    @api_app.route("/health")
    def bot_api_health():
        return _jsonify({"status": "ok", "service": "GENIUS-DCA-Bot"})

    def _run():
        port = int(os.getenv("BOT_API_PORT", "5001"))
        logger.info(f"[BOT_API] Starting on port {port} → /api/stats")
        api_app.run(host="0.0.0.0", port=port, debug=False,
                    use_reloader=False, threaded=True)

    t = _threading.Thread(target=_run, daemon=True, name="bot_api")
    t.start()
    logger.info("[BOT_API] API server thread started")


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

    heartbeat_every_s = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "600"))
    last_heartbeat_ts = 0.0

    daily_max_loss = float(os.getenv("DAILY_MAX_LOSS_USDT", "5.0"))
    _daily_loss_date = ""
    _daily_loss_total = 0.0

    init_db()
    _bootstrap_state_if_needed()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FIX #24: PHANTOM WINGS TRACKER — threading.Lock + thread start
    # engine late-binding: tracker thread მიიღებს სწორ instance-ს
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import threading as _main_threading
    global _pending_wings_lock
    _pending_wings_lock = _main_threading.Lock()

    _wings_engine_holder = [None]  # mutable container for late binding

    def _get_wings_engine():
        return _wings_engine_holder[0]

    _wings_tracker = _main_threading.Thread(
        target=_wings_limit_tracker_thread,
        args=(_get_wings_engine, _pending_wings_lock),
        daemon=True,
        name="wings_limit_tracker",
    )
    _wings_tracker.start()
    logger.info("WINGS_LIMIT_TRACKER | background thread started")

    # TP FIX — startup
    if os.getenv("TP_FIX_ENABLED", "true").strip().lower() in ("1", "true", "yes"):
        try:
            import threading as _tp_thread
            import time as _tp_time

            def _run_tp_fix_delayed():
                _tp_time.sleep(10)
                try:
                    from execution.tp_fix import run_tp_fix
                    _r = run_tp_fix()
                    logger.info(
                        f"TP_FIX | checked={_r.get('checked',0)} "
                        f"fixed={_r.get('fixed',0)} "
                        f"skipped={_r.get('skipped',0)}"
                    )
                except Exception as _tpe2:
                    logger.warning(f"TP_FIX_FAIL | err={_tpe2}")

            _tp_thread.Thread(
                target=_run_tp_fix_delayed,
                daemon=True,
                name="tp_fix"
            ).start()
            logger.info("TP_FIX | scheduled in 10s (background thread)")
        except Exception as _tpe:
            logger.warning(f"TP_FIX_THREAD_FAIL | err={_tpe}")

    # QTY SYNC
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
                    logger.info(
                        f"QTY_SYNC | checked={_r.get('checked',0)} "
                        f"fixed={_r.get('fixed',0)} "
                        f"skipped={_r.get('skipped',0)}"
                    )
                except Exception as _qe2:
                    logger.warning(f"QTY_SYNC_FAIL | err={_qe2}")

            _qty_thread.Thread(
                target=_run_qty_sync_delayed,
                daemon=True,
                name="qty_sync"
            ).start()
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

    # FIX #24: PHANTOM WINGS engine late-binding
    _wings_engine_holder[0] = engine

    generate_once = _try_import_generator()

    regime_engine = MarketRegimeEngine()
    engine.inject_regime_engine(regime_engine)

    _dca_enabled = os.getenv("DCA_ENABLED", "true").strip().lower() in ("1", "true", "yes")
    dca_mgr   = get_dca_manager()   if _dca_enabled else None
    tp_sl_mgr = get_tp_sl_manager() if _dca_enabled else None
    risk_mgr  = get_risk_manager()  if _dca_enabled else None
    if _dca_enabled:
        logger.info(f"DCA_ENABLED | max_add_ons={os.getenv('DCA_MAX_ADD_ONS', '5')} max_capital={os.getenv('DCA_MAX_CAPITAL_USDT', 'AUTO')}")

    futures_engine = get_futures_engine()
    logger.info(
        f"FUTURES_ENGINE | enabled={futures_engine.enabled} "
        f"mode={futures_engine.mode} lev={futures_engine.leverage}x"
    )

    # FIX #24: PHANTOM WINGS system startup log
    _wings_enabled_flag = os.getenv("WINGS_ENABLED", "false").strip().lower() in ("1", "true", "yes")
    if _wings_enabled_flag:
        logger.info(
            f"WINGS_SYSTEM | enabled=True "
            f"up={os.getenv('WINGS_LEVELS_UP', '0.05,0.05')} "
            f"down={os.getenv('WINGS_LEVELS_DOWN', '0.05,0.05,0.10,0.10')} "
            f"lp={os.getenv('WINGS_LP_LEVELS', '0.05,0.10')} "
            f"quotes={os.getenv('WINGS_QUOTES', '15,15,30,30,30,30')} "
            f"lp_quotes={os.getenv('WINGS_LP_QUOTES', '30,30')} "
            f"timeout={os.getenv('WINGS_TIMEOUT_SECONDS', '300')}s "
            f"[LW1/LW2=limit, LW3-LW6+LP=market]"
        )

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

            _market_regime: str = "NEUTRAL"
            _price_cache: dict = {}
            _symbols_to_cache = [s.strip() for s in os.getenv(
                "BOT_SYMBOLS", "BTC/USDT,BNB/USDT,ETH/USDT"
            ).split(",") if s.strip()]
            for _sym in _symbols_to_cache:
                try:
                    if engine.exchange is not None:
                        _price_cache[_sym] = float(
                            engine.exchange.fetch_last_price(_sym) or 0.0
                        )
                    else:
                        _t = engine.price_feed.fetch_ticker(_sym)
                        _price_cache[_sym] = float(_t.get("last") or 0.0)
                except Exception as _pe:
                    logger.warning(f"PRICE_CACHE_FAIL | {_sym} err={_pe}")
                    _price_cache[_sym] = 0.0

            _today = _now_dt().date().isoformat()
            if _today != _daily_loss_date:
                _daily_loss_date = _today
                _daily_loss_total = 0.0
                logger.info(f"DAILY_LOSS_RESET | date={_today} limit={daily_max_loss}")

            try:
                from execution.db.repository import _fetchall
                import re as _re_loss
                _loss_rows = _fetchall(
                    "SELECT message FROM audit_log "
                    "WHERE event_type IN ('L3_ROTATION','DCA_FORCE_CLOSE') "
                    "AND created_at >= date('now') ORDER BY id DESC LIMIT 100"
                )
                _loss_total = 0.0
                for _row in (_loss_rows or []):
                    try:
                        _m = _re_loss.search(r'pnl=([+-]?\d+\.\d+)', str(_row[0]))
                        if _m:
                            _loss_total += float(_m.group(1))
                    except Exception:
                        pass
                if _loss_total < 0:
                    _daily_loss_total = _loss_total
            except Exception:
                pass

            if daily_max_loss > 0 and _daily_loss_total <= -daily_max_loss:
                logger.warning(
                    f"DAILY_LOSS_LIMIT | loss={_daily_loss_total:.4f} >= limit={daily_max_loss} → skip"
                )
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
                        logger.warning(
                            f"TP_FIX_LOOP | fixed={_tp_r['fixed']} checked={_tp_r['checked']}"
                        )
                except Exception as _tfe:
                    logger.warning(f"TP_FIX_LOOP_WARN | err={_tfe}")

            try:
                from execution.signal_generator import _detect_market_regime_24h
                _prev_regime = _market_regime
                _market_regime = _detect_market_regime_24h()
                futures_engine.check_tp_sl()

                if _market_regime != _prev_regime:
                    try:
                        log_event(
                            "MARKET_REGIME_CHANGE",
                            f"regime={_market_regime} prev={_prev_regime}"
                        )
                        logger.info(
                            f"MARKET_REGIME_CHANGE | {_prev_regime} → {_market_regime}"
                        )
                    except Exception:
                        pass

            except Exception as _fe:
                logger.warning(f"FUTURES_LOOP_WARN | err={_fe}")

            if _dca_enabled:
                try:
                    _run_dca_loop(engine, dca_mgr, tp_sl_mgr, risk_mgr,
                                  market_regime=_market_regime,
                                  futures_engine=futures_engine)
                except Exception as e:
                    logger.warning(f"DCA_LOOP_WARN | err={e}")

            if _dca_enabled and futures_engine.enabled:
                try:
                    futures_engine.check_dca_hedge_addons()
                    futures_engine.check_dca_hedge_l3()
                except Exception as _he:
                    logger.warning(f"HEDGE_CHECK_WARN | err={_he}")

            if _dca_enabled and futures_engine.enabled and futures_engine.short_dca_enabled:
                try:
                    futures_engine.check_independent_short_open()
                    futures_engine.check_independent_short_addons()
                except Exception as _se:
                    logger.warning(f"SHORT_DCA_LOOP_WARN | err={_se}")

            if _dca_enabled and futures_engine.enabled and futures_engine.mirror_enabled:
                try:
                    futures_engine.check_mirror_tp_sl()
                    futures_engine.check_mirror_engine_open()
                    futures_engine.check_mirror_addons()
                except Exception as _me:
                    logger.warning(f"MIRROR_ENGINE_LOOP_WARN | err={_me}")

            if _dca_enabled:
                try:
                    _check_and_open_layer2(engine, tp_sl_mgr)
                except Exception as _l2e:
                    logger.warning(f"LAYER2_CHECK_WARN | err={_l2e}")

            if _dca_enabled:
                try:
                    _check_cascade_exchange(engine, tp_sl_mgr)
                except Exception as _cce:
                    logger.warning(f"CASCADE_CHECK_WARN | err={_cce}")

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

                    if verdict == "SELL":
                        source = sig.get("meta", {}).get("source", "UNKNOWN")
                        if source == "PROTECTIVE_SELL":
                            logger.warning(
                                f"[AUTO] PROTECTIVE_SELL → executing | "
                                f"id={signal_id} source={source}"
                            )
                            engine.execute_signal(sig)
                        else:
                            logger.info(
                                f"[AUTO] SELL blocked (DCA holds) | "
                                f"id={signal_id} source={source}"
                            )

                    elif verdict == "TRADE":
                        if sig.get("signal_type") == "PAIRS_ADDON":
                            _pa_sym = str((sig.get("execution") or {}).get("symbol", ""))
                            logger.info(
                                f"[PAIRS_ADDON] trigger | lag={_pa_sym} "
                                f"z={sig.get('meta', {}).get('z_score', '?')} "
                                f"lead_move={sig.get('meta', {}).get('lead_move_pct', '?')}%"
                            )
                            if engine.exchange is None and _dca_enabled and _pa_sym:
                                try:
                                    from execution.db.repository import (
                                        get_open_dca_position_for_symbol,
                                        update_dca_position_after_addon,
                                        add_dca_order,
                                    )
                                    from execution.dca_position_manager import recalculate_average
                                    _pa_pos = get_open_dca_position_for_symbol(_pa_sym)
                                    if _pa_pos:
                                        _pa_price = _price_cache.get(_pa_sym, 0.0)
                                        if _pa_price <= 0:
                                            _pa_t = engine.price_feed.fetch_ticker(_pa_sym)
                                            _pa_price = float(_pa_t.get("last") or 0.0)
                                        if _pa_price > 0:
                                            _pa_quote  = float(os.getenv("BOT_QUOTE_PER_TRADE", "15.0"))
                                            _pa_qty    = _pa_quote / _pa_price
                                            _pa_avg_old = float(_pa_pos["avg_entry_price"] or 0)
                                            _pa_tot_qty = float(_pa_pos["total_qty"] or 0)
                                            _pa_tot_q   = float(_pa_pos["total_quote_spent"] or 0)
                                            _pa_addons  = int(_pa_pos["add_on_count"] or 0)

                                            _pa_avg_res = recalculate_average(
                                                _pa_tot_qty, _pa_avg_old, _pa_qty, _pa_price
                                            )
                                            _pa_new_avg = _pa_avg_res["avg_entry_price"]
                                            _pa_new_qty = _pa_avg_res["total_qty"]
                                            _pa_tp_pct  = float(os.getenv("DCA_TP_PCT", "0.55"))
                                            _pa_new_tp  = round(_pa_new_avg * (1.0 + _pa_tp_pct / 100.0), 6)

                                            update_dca_position_after_addon(
                                                _pa_pos["id"],
                                                new_avg_entry=_pa_new_avg,
                                                new_total_qty=_pa_new_qty,
                                                new_total_quote=_pa_tot_q + _pa_quote,
                                                new_add_on_count=_pa_addons + 1,
                                                new_tp_price=_pa_new_tp,
                                                new_sl_price=0.0,
                                                last_add_on_ts=time.time(),
                                                last_addon_price=_pa_price,
                                            )
                                            add_dca_order(
                                                position_id=_pa_pos["id"],
                                                symbol=_pa_sym,
                                                order_type="PAIRS_ADDON",
                                                entry_price=_pa_price,
                                                qty=_pa_qty,
                                                quote_spent=_pa_quote,
                                                avg_entry_after=_pa_new_avg,
                                                tp_after=_pa_new_tp,
                                                sl_after=0.0,
                                                trigger_drawdown_pct=0.0,
                                                exchange_order_id="",
                                            )
                                            logger.info(
                                                f"[PAIRS_ADDON] DEMO_EXECUTED | {_pa_sym} "
                                                f"price={_pa_price:.4f} new_avg={_pa_new_avg:.4f} "
                                                f"new_tp={_pa_new_tp:.4f} addon#{_pa_addons+1}"
                                            )
                                    else:
                                        logger.info(
                                            f"[PAIRS_ADDON] NO_OPEN_POS | {_pa_sym} → skip"
                                        )
                                except Exception as _pa_err:
                                    logger.warning(f"[PAIRS_ADDON] EXEC_FAIL | err={_pa_err}")
                            pass

                        else:
                            trend   = float(sig.get("trend",     0) or 0)
                            atr_pct = float(sig.get("atr_pct",   0) or 0)
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

                            # ── DEMO: DCA position გახსნა ──────────────────
                            if engine.exchange is None and _dca_enabled:
                                try:
                                    from execution.db.repository import (
                                        open_dca_position, add_dca_order, open_trade,
                                        get_open_dca_position_for_symbol,
                                        get_executed_signal_action,
                                        get_all_open_trades,
                                        get_all_open_dca_positions,
                                    )
                                    _sym = str((sig.get("execution") or {}).get("symbol", "BTC/USDT"))

                                    _exec_action = get_executed_signal_action(signal_id)
                                    _REAL_REJECTS = {
                                        "REJECT_MAX_OPEN_TRADES",
                                        "REJECT_ABOVE_MIN_OPEN",
                                        "REJECT_MAX_POSITIONS",
                                        "REJECT_ACTIVE_OCO",
                                        "REJECT_OPEN_TRADE_RACE",
                                        "REJECT_MIN_NOTIONAL",
                                        "REJECT_PORTFOLIO_EXPOSURE",
                                    }
                                    _is_real_reject = _exec_action in _REAL_REJECTS

                                    if _is_real_reject:
                                        logger.info(
                                            f"[DEMO] SKIP_REJECTED | {_sym} "
                                            f"action={_exec_action} id={signal_id}"
                                        )

                                    # FIX #20: (_L\d+|_LP)$ — LP positions L1-ად არ ითვლება
                                    import re as _re_main_cnt
                                    _all_dca_cnt = get_all_open_dca_positions() or []
                                    _l1_open_cnt = sum(
                                        1 for _p in _all_dca_cnt
                                        if not _re_main_cnt.search(r'(_L\d+|_LP\d*|_LW\d*)$', str(_p.get("symbol", "")))
                                    )  # FIX #24: _LW excluded from L1 count
                                    _max_open_cnt = int(os.getenv("MAX_OPEN_TRADES", "6"))
                                    _at_max = _l1_open_cnt >= _max_open_cnt
                                    if _at_max:
                                        logger.info(
                                            f"[DEMO] SKIP_MAX_OPEN | {_sym} "
                                            f"l1_open={_l1_open_cnt} >= MAX_OPEN_TRADES={_max_open_cnt}"
                                        )

                                    _allow_dup = os.getenv("ALLOW_DCA_DUPLICATE", "false").strip().lower() in ("1", "true", "yes")
                                    _max_dca_per_sym = int(os.getenv("MAX_DCA_PER_SYMBOL", "1"))
                                    try:
                                        from execution.db.repository import count_open_dca_positions_for_symbol
                                        _sym_dca_count = count_open_dca_positions_for_symbol(_sym)
                                    except Exception:
                                        _sym_dca_count = 1 if get_open_dca_position_for_symbol(_sym) else 0

                                    if _allow_dup:
                                        _existing_blocked = _sym_dca_count >= _max_dca_per_sym
                                    else:
                                        _existing_blocked = _sym_dca_count > 0

                                    _entry_cd = int(os.getenv("ENTRY_COOLDOWN_SECONDS", "0"))
                                    if _entry_cd > 0 and not _existing_blocked:
                                        try:
                                            from execution.db.repository import get_last_entry_ts_for_symbol
                                            _last_e_ts = get_last_entry_ts_for_symbol(_sym) or 0.0
                                            _cd_elapsed = time.time() - _last_e_ts
                                            if _cd_elapsed < _entry_cd:
                                                logger.info(
                                                    f"[DEMO] ENTRY_COOLDOWN | {_sym} "
                                                    f"remaining={int(_entry_cd - _cd_elapsed)}s"
                                                )
                                                _existing_blocked = True
                                        except Exception:
                                            pass

                                    _rejected = _is_real_reject or _at_max or _existing_blocked
                                    if not _rejected:
                                        _quote = float(os.getenv("BOT_QUOTE_PER_TRADE", "15.0"))
                                        _price = _price_cache.get(_sym, 0.0)
                                        if _price <= 0:
                                            _t = engine.price_feed.fetch_ticker(_sym)
                                            _price = float(_t.get("last") or 0.0)
                                        if _price > 0:
                                            _qty = _quote / _price
                                            _tp_pct = float(os.getenv("DCA_TP_PCT", "0.55"))
                                            _tp = round(_price * (1.0 + _tp_pct / 100.0), 6)
                                            _quote_pt  = float(os.getenv("BOT_QUOTE_PER_TRADE", "15.0"))
                                            _sizes_str = os.getenv("DCA_ADDON_SIZES", "12,15,18,15,10")
                                            try:
                                                _addon_sum = sum(float(x.strip()) for x in _sizes_str.split(",") if x.strip())
                                            except Exception:
                                                _addon_sum = 70.0
                                            _auto_cap = _quote_pt + _addon_sum
                                            _max_cap  = float(os.getenv("DCA_MAX_CAPITAL_USDT") or _auto_cap)
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
                                            open_trade(
                                                signal_id=signal_id,
                                                symbol=_sym,
                                                qty=_qty,
                                                quote_in=_quote,
                                                entry_price=_price,
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
                                                    mode=os.getenv("MODE", "DEMO"),
                                                )
                                            except Exception as _tg_new:
                                                logger.warning(f"[DEMO] TG_NEW_SIGNAL_FAIL | {_sym} err={_tg_new}")

                                except Exception as _de:
                                    logger.warning(f"[DEMO] DCA_OPEN_FAIL | err={_de}")

                    else:
                        logger.info(f"[AUTO] Unsupported verdict={verdict} | id={signal_id} → skip")

                else:
                    logger.info("Worker alive, waiting for SIGNAL_OUTBOX...")

            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # FIX #24 + FIX #25: PHANTOM WINGS CHECK
            # signal processing-ის შემდეგ — L1 DB-ში უკვე არის
            # LW1/LW2 → limit (ჯავშანი), LW3-LW6 + LP1-LP2 → market buy
            # duplicate guard: DB + _pending_wings_orders (LW1/LW2)
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if _dca_enabled and _wings_enabled_flag:
                try:
                    _w_tp_pct     = float(os.getenv("DCA_TP_PCT", "0.55"))
                    _w_max_addons = int(os.getenv("DCA_MAX_ADD_ONS", "3"))
                    _w_sizes_str  = os.getenv("DCA_ADDON_SIZES", "15,15,15")
                    try:
                        _w_addon_sum = sum(float(x.strip()) for x in _w_sizes_str.split(",") if x.strip())
                    except Exception:
                        _w_addon_sum = 45.0
                    _w_max_quote_str = os.getenv("WINGS_QUOTES", "15,15,30,30,30,30")
                    try:
                        _w_max_quote = max(float(x.strip()) for x in _w_max_quote_str.split(",") if x.strip())
                    except Exception:
                        _w_max_quote = 30.0
                    _w_max_cap = float(os.getenv("DCA_MAX_CAPITAL_USDT") or (_w_max_quote + _w_addon_sum))
                    _check_and_open_wings(
                        engine=engine,
                        tp_pct=_w_tp_pct,
                        max_add_ons=_w_max_addons,
                        max_capital=_w_max_cap,
                    )
                except Exception as _we:
                    logger.warning(f"WINGS_CHECK_WARN | err={_we}")

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

                _hb_silent = (3 <= _hb_hour < 9)
                _hb_day_ok = not _hb_silent and (now - last_heartbeat_ts) >= 1800
                _hb_midnight_ok = (_hb_hour == 23 and _hb_minute >= 57)

                if _hb_day_ok or _hb_midnight_ok:
                    from execution.db.repository import get_all_open_dca_positions, get_trade_stats
                    from execution.telegram_notifier import notify_heartbeat
                    import resource as _res
                    _hb_positions = get_all_open_dca_positions()
                    _hb_capital = sum(
                        float(p.get("total_quote_spent", 0)) for p in _hb_positions
                    )
                    _hb_stats = get_trade_stats()
                    _hb_pnl_today = float(_hb_stats.get("pnl_quote_sum", 0.0))
                    _hb_mem = _res.getrusage(_res.RUSAGE_SELF).ru_maxrss / 1024
                    notify_heartbeat(
                        open_count=len(_hb_positions),
                        open_capital=_hb_capital,
                        prices=_price_cache,
                        memory_mb=_hb_mem,
                        pnl_today=_hb_pnl_today,
                        positions=_hb_positions,
                    )
                    last_heartbeat_ts = now
            except Exception as _hbe:
                logger.warning(f"HEARTBEAT_FAIL | err={_hbe}")

            try:
                now_local = _now_dt()
                today_str = now_local.date().isoformat()

                if (
                    now_local.hour == 23
                    and now_local.minute in (57, 58, 59)
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
