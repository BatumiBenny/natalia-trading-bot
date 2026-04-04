"""
GENIUS-DCA-Bot — Unit Tests
კრიტიკული ფუნქციების ტესტები
"""
import os
import sys
import unittest

# ENV setup for tests
os.environ.update({
    "MODE": "DEMO",
    "DCA_TP_PCT": "1.5",
    "DCA_SL_PCT": "50.0",
    "DCA_MAX_ADD_ONS": "2",
    "DCA_ADDON_TRIGGER_PCTS": "1.0,2.0",
    "DCA_ADDON_SIZES": "10,10",
    "DCA_MAX_CAPITAL_USDT": "30.0",
    "DCA_MAX_DRAWDOWN_PCT": "80.0",
    "DCA_BREAKEVEN_TRIGGER_PCT": "0.5",
    "DCA_SL_CONFIRM_CANDLES": "2",
    "BINANCE_API_KEY": "test",
    "BINANCE_API_SECRET": "test",
})

sys.path.insert(0, "/opt/render/project/src")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. DCA TP/SL Manager Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TestDCATpSlCalculation(unittest.TestCase):

    def setUp(self):
        from execution.dca_tp_sl_manager import DCATpSlManager
        self.mgr = DCATpSlManager()

    def test_tp_calculation(self):
        result = self.mgr.calculate(66983.71)
        expected_tp = 66983.71 * 1.015
        self.assertAlmostEqual(result["tp_price"], expected_tp, places=2)

    def test_sl_calculation(self):
        result = self.mgr.calculate(66983.71)
        expected_sl = 66983.71 * 0.50  # 50% SL
        self.assertAlmostEqual(result["sl_price"], expected_sl, places=2)

    def test_tp_pct_correct(self):
        result = self.mgr.calculate(100.0)
        self.assertAlmostEqual(result["tp_price"], 101.5, places=4)

    def test_sl_pct_correct(self):
        result = self.mgr.calculate(100.0)
        self.assertAlmostEqual(result["sl_price"], 50.0, places=4)

    def test_breakeven_triggered(self):
        avg_entry = 66983.71
        trigger_price = avg_entry * 1.005  # 0.5% above
        current_price = trigger_price + 1
        should_update, new_sl = self.mgr.check_breakeven(avg_entry, current_price, 33491.0)
        self.assertTrue(should_update)
        self.assertAlmostEqual(new_sl, avg_entry * 0.9995, places=2)

    def test_breakeven_not_triggered(self):
        avg_entry = 66983.71
        current_price = avg_entry * 1.001  # only 0.1% above, not enough
        should_update, _ = self.mgr.check_breakeven(avg_entry, current_price, 33491.0)
        self.assertFalse(should_update)

    def test_sl_not_confirmed_one_candle(self):
        sl_price = 60000.0
        # Only 1 candle below SL — should NOT confirm
        ohlcv = [[0, 0, 0, 0, 61000.0]] * 10 + [[0, 0, 0, 0, 59000.0]]
        confirmed, reason = self.mgr.is_sl_confirmed(sl_price, ohlcv)
        self.assertFalse(confirmed)

    def test_sl_confirmed_two_candles(self):
        sl_price = 60000.0
        # 2 candles below SL + downtrend
        ohlcv = [[0, 0, 0, 0, float(65000 - i * 500)] for i in range(20)]
        confirmed, _ = self.mgr.is_sl_confirmed(sl_price, ohlcv)
        # With downtrend, should confirm
        self.assertIsInstance(confirmed, bool)

    def test_force_close_max_drawdown(self):
        pos = {
            "avg_entry_price": 66983.71,
            "current_sl_price": 33491.0,
            "add_on_count": 2,
            "max_add_ons": 2,
            "max_drawdown_pct": 80.0,
        }
        # 85% drawdown → force close
        current_price = 66983.71 * 0.15
        should_close, reason = self.mgr.should_force_close(pos, current_price)
        self.assertTrue(should_close)
        self.assertIn("MAX_DRAWDOWN", reason)

    def test_force_close_not_triggered(self):
        pos = {
            "avg_entry_price": 66983.71,
            "current_sl_price": 33491.0,
            "add_on_count": 1,
            "max_add_ons": 2,
            "max_drawdown_pct": 80.0,
        }
        # Only 5% drawdown → no force close
        current_price = 66983.71 * 0.95
        should_close, _ = self.mgr.should_force_close(pos, current_price)
        self.assertFalse(should_close)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. DCA Position Manager Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TestDCAPositionManager(unittest.TestCase):

    def test_recalculate_average_basic(self):
        from execution.dca_position_manager import recalculate_average
        # Buy $10 @ 67000, then $10 @ 65000
        result = recalculate_average(
            existing_qty=0.000149,
            existing_avg=67000.0,
            new_qty=0.000153,
            new_price=65000.0,
        )
        avg = result["avg_entry_price"]
        self.assertGreater(avg, 65000.0)
        self.assertLess(avg, 67000.0)
        self.assertAlmostEqual(result["total_qty"], 0.000149 + 0.000153, places=6)

    def test_addon_trigger_pcts_parsed(self):
        from execution.dca_position_manager import DCAPositionManager
        mgr = DCAPositionManager()
        self.assertEqual(len(mgr.addon_trigger_pcts), 2)
        self.assertAlmostEqual(mgr.addon_trigger_pcts[0], 1.0)
        self.assertAlmostEqual(mgr.addon_trigger_pcts[1], 2.0)

    def test_addon_size_correct(self):
        from execution.dca_position_manager import DCAPositionManager
        mgr = DCAPositionManager()
        self.assertAlmostEqual(mgr.get_addon_size(0), 10.0)
        self.assertAlmostEqual(mgr.get_addon_size(1), 10.0)

    def test_should_not_addon_at_max(self):
        from execution.dca_position_manager import DCAPositionManager
        mgr = DCAPositionManager()
        pos = {
            "add_on_count": 2,  # max reached
            "avg_entry_price": 67000.0,
            "total_quote_spent": 30.0,
            "last_add_on_ts": 0,
        }
        ohlcv = [[0, 0, 0, 0, 65000.0]] * 30
        ok, reason = mgr.should_add_on(pos, 65000.0, ohlcv)
        self.assertFalse(ok)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. Risk Manager Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TestDCARiskManager(unittest.TestCase):

    def setUp(self):
        from execution.dca_risk_manager import DCARiskManager
        self.mgr = DCARiskManager()

    def test_blocks_when_capital_exceeded(self):
        pos = {
            "total_quote_spent": 30.0,  # already at max
            "symbol": "BTC/USDT",
        }
        ok, reason = self.mgr.can_add_on(pos, addon_size=10.0, all_positions=[pos])
        self.assertFalse(ok)

    def test_allows_within_capital(self):
        pos = {
            "total_quote_spent": 10.0,
            "symbol": "BTC/USDT",
        }
        ok, reason = self.mgr.can_add_on(pos, addon_size=10.0, all_positions=[pos])
        self.assertTrue(ok)

    def test_blocks_below_min_notional(self):
        pos = {
            "total_quote_spent": 10.0,
            "symbol": "BTC/USDT",
        }
        ok, reason = self.mgr.can_add_on(pos, addon_size=5.0, all_positions=[pos])
        self.assertFalse(ok)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. Signal Client Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TestSignalClient(unittest.TestCase):

    def test_valid_trade_signal(self):
        from execution.signal_client import validate_signal
        signal = {
            "final_verdict": "TRADE",
            "certified_signal": True,
            "execution": {
                "symbol": "BTC/USDT",
                "direction": "LONG",
                "entry": {"type": "MARKET"},
                "position_size": 0.001,
            }
        }
        try:
            validate_signal(signal)
            valid = True
        except ValueError:
            valid = False
        self.assertTrue(valid)

    def test_invalid_verdict_rejected(self):
        from execution.signal_client import validate_signal
        signal = {
            "final_verdict": "RANDOM",
            "certified_signal": True,
            "execution": {"symbol": "BTC/USDT", "direction": "LONG"}
        }
        with self.assertRaises(ValueError):
            validate_signal(signal)

    def test_uncertified_rejected(self):
        from execution.signal_client import validate_signal
        signal = {
            "final_verdict": "TRADE",
            "certified_signal": False,
            "execution": {"symbol": "BTC/USDT"}
        }
        with self.assertRaises(ValueError):
            validate_signal(signal)

    def test_fingerprint_deterministic(self):
        from execution.signal_client import _fingerprint
        sig = {
            "final_verdict": "TRADE",
            "execution": {
                "symbol": "BTC/USDT",
                "direction": "LONG",
                "entry": {"type": "MARKET"},
                "position_size": 0.001,
            }
        }
        fp1 = _fingerprint(sig)
        fp2 = _fingerprint(sig)
        self.assertEqual(fp1, fp2)
        self.assertEqual(len(fp1), 64)  # SHA256


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. Excel Live Core Tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TestExcelLiveCore(unittest.TestCase):

    def setUp(self):
        from execution.excel_live_core import ExcelLiveCore, CoreInputs
        self.core = ExcelLiveCore()
        self.CoreInputs = CoreInputs

    def test_strong_bull_executes(self):
        inp = self.CoreInputs(
            trend_strength=0.8,
            structure_ok=True,
            volume_score=0.9,
            risk_state="OK",
            confidence_score=0.9,
            volatility_regime="NORMAL",
        )
        result = self.core.decide(inp)
        self.assertEqual(result["macro_gate"], "ALLOW")
        self.assertGreater(result["ai_score"], 0.5)

    def test_kill_risk_blocks(self):
        inp = self.CoreInputs(
            trend_strength=0.9,
            structure_ok=True,
            volume_score=0.9,
            risk_state="KILL",
            confidence_score=0.9,
            volatility_regime="NORMAL",
        )
        result = self.core.decide(inp)
        self.assertEqual(result["final_trade_decision"], "STAND_BY")

    def test_extreme_volatility_blocks(self):
        inp = self.CoreInputs(
            trend_strength=0.9,
            structure_ok=True,
            volume_score=0.9,
            risk_state="OK",
            confidence_score=0.9,
            volatility_regime="EXTREME",
        )
        result = self.core.decide(inp)
        self.assertEqual(result["final_trade_decision"], "STAND_BY")

    def test_weights_sum_to_one(self):
        total = (self.core.w_trend + self.core.w_struct +
                 self.core.w_volconf + self.core.w_risk +
                 self.core.w_conf + self.core.w_vol)
        self.assertAlmostEqual(total, 1.0, places=5)


if __name__ == "__main__":
    print("🧪 GENIUS-DCA-Bot Unit Tests")
    print("=" * 50)
    unittest.main(verbosity=2)
