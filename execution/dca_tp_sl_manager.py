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
        self.tp_pct              = _ef("DCA_TP_PCT",                  0.55)   # FIX: default 0.55% (was 2.0%)
        self.sl_pct              = _ef("DCA_SL_PCT",                  999.0)  # DCA: default=999 (disabled)
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
        DCA სტრატეგია: SL confirmation გათიშულია.
        ბოტი არ ყიდის SL-ზე — ინახავს პოზიციას.
        """
        # DCA: SL disabled — hold until TP
        return False, "SL_DISABLED_DCA_MODE"

    def check_breakeven(
        self,
        avg_entry_price: float,
        current_price: float,
        current_sl_price: float,
    ) -> Tuple[bool, float]:
        """
        DCA სტრატეგია: Breakeven სრულად გათიშულია.
        ბოტი ინახავს პოზიციას სანამ TP-ს არ მიაღწევს.
        """
        # DCA: breakeven disabled — hold until TP
        return False, current_sl_price

    def should_force_close(
        self,
        position: Dict[str, Any],
        current_price: float,
    ) -> Tuple[bool, str]:
        """
        DCA სტრატეგია: Force close სრულად გათიშულია.
        ბოტი ინახავს პოზიციას სანამ TP-ს არ მიაღწევს.
        არავითარი იძულებითი გაყიდვა არ არის.
        """
        # DCA: force close disabled — hold until TP
        return False, "OK"


# module-level singleton
_tp_sl_mgr: Optional[DCATpSlManager] = None


def get_tp_sl_manager() -> DCATpSlManager:
    global _tp_sl_mgr
    if _tp_sl_mgr is None:
        _tp_sl_mgr = DCATpSlManager()
    return _tp_sl_mgr
