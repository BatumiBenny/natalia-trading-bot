# execution/dca_tp_sl_manager.py
# ============================================================
# DCA TP/SL Manager — Dynamic recalculation after each add-on,
# SL confirmation (no noise-triggered close), breakeven logic.
#
# ENV პარამეტრები:
#   DCA_TP_PCT=2.0
#   DCA_SL_PCT=6.0
#   DCA_SL_CONFIRM_CANDLES=2
#   DCA_BREAKEVEN_TRIGGER_PCT=0.5
# ============================================================
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("gbm")


def _ef(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v is not None else default
    except Exception:
        return default


def _ei(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v is not None else default
    except Exception:
        return default


def _trend_strength(closes: List[float]) -> float:
    """Simple trend score 0..1 — same logic as signal_generator."""
    if len(closes) < 10:
        return 0.5
    last = closes[-1]
    prev = closes[-2]
    # SMA slope
    s5  = sum(closes[-5:]) / 5
    s10 = sum(closes[-10:]) / 10
    slope = (s5 / s10 - 1.0) if s10 else 0.0
    # ups in last 3
    ups3 = sum(1 for i in range(-3, 0) if closes[i] > closes[i - 1])
    base = 0.0
    base += 0.35 * (1.0 if last > prev else 0.0)
    base += 0.25 * max(0.0, min(1.0, slope / 0.003))
    base += 0.40 * (ups3 / 3.0)
    return max(0.0, min(1.0, base))


class DCATpSlManager:
    """
    DCA-სპეციფიური TP/SL მართვა.

    ძირითადი განსხვავება ჩვეულებრივი OCO-სგან:
      - SL ბევრად დიდია (6%) — averaging-ს სჭირდება სივრცე
      - SL confirmed — 2 consecutive candle close below SL
      - TP/SL recalculates on every add-on (avg_entry changes)
      - Breakeven: avg_entry+0.5% → SL moves to avg_entry
    """

    def __init__(self) -> None:
        self.tp_pct              = _ef("DCA_TP_PCT",                  2.0)
        self.sl_pct              = _ef("DCA_SL_PCT",                  6.0)
        self.sl_confirm_candles  = _ei("DCA_SL_CONFIRM_CANDLES",      2)
        self.breakeven_trigger   = _ef("DCA_BREAKEVEN_TRIGGER_PCT",    0.5)

        logger.info(
            f"[DCA] DCATpSlManager init | TP={self.tp_pct}% SL={self.sl_pct}% "
            f"sl_confirm={self.sl_confirm_candles} breakeven_trigger={self.breakeven_trigger}%"
        )

    def calculate(self, avg_entry_price: float) -> Dict[str, float]:
        """
        avg_entry-დან TP და SL გამოთვლა.
        გამოიძახება პოზიციის გახსნისას და ყოველ add-on-ის შემდეგ.
        """
        avg = float(avg_entry_price)
        tp  = round(avg * (1.0 + self.tp_pct / 100.0), 6)
        sl  = round(avg * (1.0 - self.sl_pct / 100.0), 6)
        return {
            "tp_price": tp,
            "sl_price": sl,
            "tp_pct":   self.tp_pct,
            "sl_pct":   self.sl_pct,
        }

    def is_sl_confirmed(
        self,
        sl_price: float,
        ohlcv: List[List[float]],
    ) -> Tuple[bool, str]:
        """
        SL hit-ი დასტურდება მხოლოდ:
          1. ბოლო N candle-ის CLOSE ყველა SL-ზე დაბლა
          2. trend strength < 0.25 (downtrend დასტური)

        ერთი touch ან wick — არ ითვლება.

        Returns: (confirmed: bool, reason: str)
        """
        n = self.sl_confirm_candles
        if len(ohlcv) < n + 1:
            return False, f"not_enough_candles_need_{n+1}"

        closes = [float(c[4]) for c in ohlcv]
        last_n_closes = closes[-n:]

        # ყველა ბოლო N candle close below SL
        all_below = all(c < sl_price for c in last_n_closes)
        if not all_below:
            above_count = sum(1 for c in last_n_closes if c >= sl_price)
            return False, f"ONLY_{n - above_count}/{n}_CANDLES_BELOW_SL"

        # trend strength დასტური
        trend = _trend_strength(closes[-20:] if len(closes) >= 20 else closes)
        if trend > 0.25:
            return False, f"TREND_STILL_OK_{trend:.3f}_>_0.25"

        return True, f"SL_CONFIRMED_{n}_CANDLES_BELOW_trend={trend:.3f}"

    def check_breakeven(
        self,
        avg_entry_price: float,
        current_price: float,
        current_sl_price: float,
    ) -> Tuple[bool, float]:
        """
        თუ current_price >= avg_entry × (1 + trigger%),
        SL-ი avg_entry-ზე გადადის (breakeven protection).

        Returns: (should_update: bool, new_sl_price: float)
        """
        trigger_price = avg_entry_price * (1.0 + self.breakeven_trigger / 100.0)
        if current_price >= trigger_price:
            new_sl = round(avg_entry_price * 0.9995, 6)  # 0.05% buffer below entry
            if new_sl > current_sl_price:
                logger.info(
                    f"[DCA] BREAKEVEN_TRIGGERED | "
                    f"price={current_price:.4f} >= trigger={trigger_price:.4f} | "
                    f"SL {current_sl_price:.4f} → {new_sl:.4f}"
                )
                return True, new_sl
        return False, current_sl_price

    def should_force_close(
        self,
        position: Dict[str, Any],
        current_price: float,
    ) -> Tuple[bool, str]:
        """
        Force close — recovery-ს შანსი აღარ ჩანს.

        პირობები:
          1. max_add_ons reached AND price still below SL
          2. drawdown > DCA_MAX_DRAWDOWN_PCT
        """
        avg_entry = float(position.get("avg_entry_price", 0.0))
        sl_price  = float(position.get("current_sl_price", 0.0))
        add_on_n  = int(position.get("add_on_count", 0))
        max_n     = int(position.get("max_add_ons", _ei("DCA_MAX_ADD_ONS", 3)))
        max_dd    = float(position.get("max_drawdown_pct", _ef("DCA_MAX_DRAWDOWN_PCT", 8.0)))

        if avg_entry <= 0:
            return False, "no_avg_entry"

        drawdown = (avg_entry - current_price) / avg_entry * 100.0

        # max drawdown exceeded
        if drawdown > max_dd:
            return True, f"MAX_DRAWDOWN {drawdown:.2f}% > {max_dd:.1f}%"

        # max add-ons AND below SL
        if add_on_n >= max_n and sl_price > 0 and current_price < sl_price:
            return True, f"MAX_ADD_ONS_REACHED ({add_on_n}/{max_n}) + BELOW_SL"

        return False, "OK"


# module-level singleton
_tp_sl_mgr: Optional[DCATpSlManager] = None


def get_tp_sl_manager() -> DCATpSlManager:
    global _tp_sl_mgr
    if _tp_sl_mgr is None:
        _tp_sl_mgr = DCATpSlManager()
    return _tp_sl_mgr
