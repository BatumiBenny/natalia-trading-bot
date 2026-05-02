# execution/config.py
# ============================================================
# სრული კონფიგურაცია — ყველა .env პარამეტრი
# ============================================================
# SYNC CONTRACT:
#   config.py defaults == signal_generator.py defaults == env_final.txt values
#   ნებისმიერი ცვლილება ამ სამ ფაილში ერთდროულად უნდა მოხდეს.
#   "source of truth" = env_final.txt — ENV ყოველთვის override-ავს defaults-ებს.
# ============================================================
import os
from pathlib import Path

def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, AttributeError):
        return default

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, AttributeError):
        return default

def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

MODE = _env_str("MODE", "DEMO").upper()
if MODE not in ("DEMO", "TESTNET", "LIVE"):
    MODE = "DEMO"

LIVE_CONFIRMATION    = _env_bool("LIVE_CONFIRMATION",    "true")
KILL_SWITCH          = _env_bool("KILL_SWITCH",          "false")
STARTUP_SYNC_ENABLED = _env_bool("STARTUP_SYNC_ENABLED", "true")

BINANCE_API_KEY        = _env_str("BINANCE_API_KEY",        "")
BINANCE_API_SECRET     = _env_str("BINANCE_API_SECRET",     "")
BINANCE_LIVE_REST_BASE = _env_str("BINANCE_LIVE_REST_BASE", "https://api.binance.com/api/v3")

DB_PATH = Path(_env_str("DB_PATH", "/var/data/genius_dca.db"))

BOT_SYMBOLS      = _env_str("BOT_SYMBOLS",      "BTC/USDT,ETH/USDT,BNB/USDT")
SYMBOL_WHITELIST = _env_str("SYMBOL_WHITELIST", "BTC/USDT,ETH/USDT,BNB/USDT")
BOT_TIMEFRAME    = _env_str("BOT_TIMEFRAME",    "15m")
MTF_TIMEFRAME    = _env_str("MTF_TIMEFRAME",    "1h")
BOT_CANDLE_LIMIT = _env_int("BOT_CANDLE_LIMIT", 300)

BOT_QUOTE_PER_TRADE  = _env_float("BOT_QUOTE_PER_TRADE",  50.0)
MAX_QUOTE_PER_TRADE  = _env_float("MAX_QUOTE_PER_TRADE",  50.0)
BOT_POSITION_SIZE    = _env_float("BOT_POSITION_SIZE",     0.0)

CAPITAL_USAGE_MAX      = _env_float("CAPITAL_USAGE_MAX",      0.80)
CAPITAL_USAGE_MIN      = _env_float("CAPITAL_USAGE_MIN",      0.30)
MAX_ACCOUNT_DRAWDOWN   = _env_float("MAX_ACCOUNT_DRAWDOWN",   999.0)

USE_DYNAMIC_SIZING     = _env_bool("USE_DYNAMIC_SIZING",     "true")
ALLOW_POSITION_SCALING = _env_bool("ALLOW_POSITION_SCALING", "false")
DYNAMIC_SIZE_AI_LOW    = _env_float("DYNAMIC_SIZE_AI_LOW",  0.55)
DYNAMIC_SIZE_AI_HIGH   = _env_float("DYNAMIC_SIZE_AI_HIGH", 0.80)
VIRTUAL_START_BALANCE  = _env_float("VIRTUAL_START_BALANCE", 100000.0)

MAX_OPEN_TRADES          = _env_int("MAX_OPEN_TRADES",          6)
MAX_POSITIONS_PER_SYMBOL = _env_int("MAX_POSITIONS_PER_SYMBOL", 1)
MAX_TRADES_PER_DAY       = _env_int("MAX_TRADES_PER_DAY",      60)
MAX_TRADES_PER_HOUR      = _env_int("MAX_TRADES_PER_HOUR",     12)
MAX_CONSECUTIVE_LOSSES   = _env_int("MAX_CONSECUTIVE_LOSSES",   5)
MAX_DAILY_LOSS           = _env_float("MAX_DAILY_LOSS",         3.0)

TP_PCT = _env_float("TP_PCT", 0.55)
SL_PCT = _env_float("SL_PCT", 999.0)

ATR_MULT_TP_BULL        = _env_float("ATR_MULT_TP_BULL",        4.0)
ATR_MULT_SL_BULL        = _env_float("ATR_MULT_SL_BULL",        2.0)
ATR_TO_TP_SANITY_FACTOR = _env_float("ATR_TO_TP_SANITY_FACTOR", 0.08)

USE_PARTIAL_TP   = _env_bool("USE_PARTIAL_TP", "true")
PARTIAL_TP1_PCT  = _env_float("PARTIAL_TP1_PCT",  1.0)
PARTIAL_TP1_SIZE = _env_float("PARTIAL_TP1_SIZE", 0.5)

BREAKEVEN_TRIGGER_PCT  = _env_float("BREAKEVEN_TRIGGER_PCT",  0.48)
TRAILING_STOP_DISTANCE = _env_float("TRAILING_STOP_DISTANCE", 0.25)

SL_COOLDOWN_AFTER_N       = _env_int("SL_COOLDOWN_AFTER_N",       3)
SL_COOLDOWN_PAUSE_SECONDS = _env_int("SL_COOLDOWN_PAUSE_SECONDS", 1800)
SL_LIMIT_GAP_PCT          = _env_float("SL_LIMIT_GAP_PCT",        0.15)

RECOVERY_CANDLE_PCT    = _env_float("RECOVERY_CANDLE_PCT",    0.05)
RECOVERY_GREEN_CANDLES = _env_int("RECOVERY_GREEN_CANDLES",   2)

AI_CONFIDENCE_BOOST      = _env_float("AI_CONFIDENCE_BOOST",    1.05)
AI_SIGNAL_THRESHOLD      = _env_float("AI_SIGNAL_THRESHOLD",    0.0)
AI_FILTER_LOW_CONFIDENCE = _env_bool("AI_FILTER_LOW_CONFIDENCE", "false")

BUY_CONFIDENCE_MIN      = _env_float("BUY_CONFIDENCE_MIN",      0.15)
BUY_LIQUIDITY_MIN_SCORE = _env_float("BUY_LIQUIDITY_MIN_SCORE", 0.0)

THRESHOLD_CONF   = _env_float("THRESHOLD_CONF",   0.32)
THRESHOLD_TREND  = _env_float("THRESHOLD_TREND",  0.30)
THRESHOLD_VOLUME = _env_float("THRESHOLD_VOLUME", 0.25)

RSI_MIN      = _env_int("RSI_MIN",      35)
RSI_MAX      = _env_int("RSI_MAX",      72)
RSI_SELL_MIN = _env_int("RSI_SELL_MIN", 72)

MIN_VOLUME_24H = _env_float("MIN_VOLUME_24H", 30_000_000)

MAX_SPREAD_PCT     = _env_float("MAX_SPREAD_PCT",      0.08)
MIN_MOVE_PCT       = _env_float("MIN_MOVE_PCT",        0.22)
MIN_NET_PROFIT_PCT = _env_float("MIN_NET_PROFIT_PCT",  0.25)
MIN_SL_PCT         = _env_float("MIN_SL_PCT",          0.40)

ENABLE_SOFT_VOLUME_OVERRIDE = _env_bool("ENABLE_SOFT_VOLUME_OVERRIDE", "true")
SOFT_VOLUME_AI_MIN          = _env_float("SOFT_VOLUME_AI_MIN",  0.40)
SOFT_VOLUME_RELAX           = _env_float("SOFT_VOLUME_RELAX",   0.10)
SOFT_VOLUME_REQUIRE_VOLBAND = _env_bool("SOFT_VOLUME_REQUIRE_VOLBAND", "false")

USE_MA_FILTERS     = _env_bool("USE_MA_FILTERS",     "false")
USE_MACD_FILTER    = _env_bool("USE_MACD_FILTER",    "false")
USE_MTF_FILTER     = _env_bool("USE_MTF_FILTER",     "false")
USE_RSI_FILTER     = _env_bool("USE_RSI_FILTER",     "false")
USE_ADX_FILTER     = _env_bool("USE_ADX_FILTER",     "false")
USE_VWAP_FILTER    = _env_bool("USE_VWAP_FILTER",    "false")
USE_TIME_FILTER    = _env_bool("USE_TIME_FILTER",    "false")
USE_FUNDING_FILTER = _env_bool("USE_FUNDING_FILTER", "false")

ADX_MIN_THRESHOLD = _env_float("ADX_MIN_THRESHOLD", 23.0)
ADX_PERIOD        = _env_int("ADX_PERIOD", 14)

VWAP_TOLERANCE    = _env_float("VWAP_TOLERANCE", 0.006)
VWAP_SESSION_BARS = _env_int("VWAP_SESSION_BARS", 96)

MACD_SMART_MODE      = _env_bool("MACD_SMART_MODE",      "true")
MACD_IMPROVING_BARS  = _env_int("MACD_IMPROVING_BARS",   4)
MACD_HIST_ATR_FACTOR = _env_float("MACD_HIST_ATR_FACTOR", 0.2)

TRADE_HOUR_START_UTC = _env_int("TRADE_HOUR_START_UTC", 7)
TRADE_HOUR_END_UTC   = _env_int("TRADE_HOUR_END_UTC",  22)

FUNDING_MAX_LONG_PCT  = _env_float("FUNDING_MAX_LONG_PCT",  0.10)
FUNDING_MIN_SHORT_PCT = _env_float("FUNDING_MIN_SHORT_PCT", -0.05)

MTF_BLOCK_ON_BEAR_DIVERGE = _env_bool("MTF_BLOCK_ON_BEAR_DIVERGE", "false")
MTF_TP_BONUS    = _env_float("MTF_TP_BONUS",    0.25)
MTF_TP_PENALTY  = _env_float("MTF_TP_PENALTY",  0.20)

ADAPTIVE_MODE  = _env_bool("ADAPTIVE_MODE", "true")
MARKET_MODE    = _env_str("MARKET_MODE",   "ADAPTIVE")
STRATEGY_MODE  = _env_str("STRATEGY_MODE", "HYBRID")
TRADE_ACTIVITY = _env_str("TRADE_ACTIVITY","HIGH")

REGIME_BULL_TREND_MIN      = _env_float("REGIME_BULL_TREND_MIN",    0.30)
REGIME_SIDEWAYS_ATR_MAX    = _env_float("REGIME_SIDEWAYS_ATR_MAX",  0.20)
REGIME_CONF_BULL_MULT      = _env_float("REGIME_CONF_BULL_MULT",    0.85)
REGIME_CONF_UNCERTAIN_MULT = _env_float("REGIME_CONF_UNCERTAIN_MULT", 1.20)
REGIME_STABILITY_MIN       = _env_float("REGIME_STABILITY_MIN",     0.60)

STRUCT_SOFT_OVERRIDE        = _env_bool("STRUCT_SOFT_OVERRIDE",        "true")
STRUCT_SOFT_MIN_MA_GAP      = _env_float("STRUCT_SOFT_MIN_MA_GAP",    0.10)
STRUCT_SOFT_MIN_TREND       = _env_float("STRUCT_SOFT_MIN_TREND",      0.25)
STRUCT_SOFT_MIN_MOM10       = _env_float("STRUCT_SOFT_MIN_MOM10",     -0.02)
STRUCT_SOFT_REQUIRE_LAST_UP = _env_int("STRUCT_SOFT_REQUIRE_LAST_UP",  1)

WEIGHT_TREND      = _env_float("WEIGHT_TREND",      0.30)
WEIGHT_STRUCTURE  = _env_float("WEIGHT_STRUCTURE",  0.20)
WEIGHT_VOLUME     = _env_float("WEIGHT_VOLUME",     0.13)
WEIGHT_RISK       = _env_float("WEIGHT_RISK",       0.15)
WEIGHT_CONFIDENCE = _env_float("WEIGHT_CONFIDENCE", 0.15)
WEIGHT_VOLATILITY = _env_float("WEIGHT_VOLATILITY", 0.07)

EXECUTION_STYLE = _env_str("EXECUTION_STYLE", "FAST")

LIMIT_ENTRY_OFFSET_PCT      = _env_float("LIMIT_ENTRY_OFFSET_PCT",      0.03)
LIMIT_ENTRY_TIMEOUT_SEC     = _env_int("LIMIT_ENTRY_TIMEOUT_SEC",        15)
ESTIMATED_ROUNDTRIP_FEE_PCT = _env_float("ESTIMATED_ROUNDTRIP_FEE_PCT",  0.14)

SELL_TREND_THRESHOLD = _env_float("SELL_TREND_THRESHOLD", -0.05)
SELL_BUFFER          = _env_float("SELL_BUFFER",           0.999)
SELL_RETRY_BUFFER    = _env_float("SELL_RETRY_BUFFER",     0.998)

BLOCK_SIGNALS_WHEN_ACTIVE_OCO = _env_bool("BLOCK_SIGNALS_WHEN_ACTIVE_OCO", "true")
DEDUPE_ONLY_WHEN_ACTIVE_OCO   = _env_bool("DEDUPE_ONLY_WHEN_ACTIVE_OCO",   "false")

LOOP_SLEEP_SECONDS          = _env_int("LOOP_SLEEP_SECONDS",          120)
BOT_SIGNAL_COOLDOWN_SECONDS = _env_int("BOT_SIGNAL_COOLDOWN_SECONDS",  120)

USE_KELLY_SIZING    = _env_bool("USE_KELLY_SIZING",    "false")
USE_ADAPTIVE_SIZING = _env_bool("USE_ADAPTIVE_SIZING", "true")

PORTFOLIO_ENABLED  = _env_bool("PORTFOLIO_ENABLED", "false")
SIGNAL_OUTBOX_PATH = _env_str("SIGNAL_OUTBOX_PATH", "/var/data/signal_outbox.json")

TELEGRAM_NOTIFICATIONS        = _env_bool("TELEGRAM_NOTIFICATIONS",        "true")
TELEGRAM_BOT_TOKEN            = _env_str("TELEGRAM_BOT_TOKEN",             "")
TELEGRAM_CHAT_ID              = _env_str("TELEGRAM_CHAT_ID",               "")
TELEGRAM_PARSE_MODE           = _env_str("TELEGRAM_PARSE_MODE",            "HTML")
TELEGRAM_TIMEZONE             = _env_str("TELEGRAM_TIMEZONE",              "Asia/Tbilisi")
TELEGRAM_REPORT_EVERY_SECONDS = _env_int("TELEGRAM_REPORT_EVERY_SECONDS",  10800)
REPORT_EVERY_SECONDS          = _env_int("REPORT_EVERY_SECONDS",           60)

GEN_DEBUG       = _env_bool("GEN_DEBUG",       "true")
GEN_TEST_SIGNAL = _env_bool("GEN_TEST_SIGNAL", "false")

# ─────────────────────────────────────────────
# DCA — Dollar Cost Averaging
# ─────────────────────────────────────────────

DCA_ENABLED              = _env_bool("DCA_ENABLED",              "true")
DCA_MAX_ADD_ONS          = _env_int("DCA_MAX_ADD_ONS",           5)
DCA_MAX_CAPITAL_USDT     = _env_float("DCA_MAX_CAPITAL_USDT",   350.0)
DCA_MAX_TOTAL_USDT       = _env_float("DCA_MAX_TOTAL_USDT",    2200.0)
DCA_MAX_DRAWDOWN_PCT     = _env_float("DCA_MAX_DRAWDOWN_PCT",    999.0)
DCA_MIN_NOTIONAL         = _env_float("DCA_MIN_NOTIONAL",        10.0)

DCA_ADDON_TRIGGER_PCTS   = _env_str("DCA_ADDON_TRIGGER_PCTS",   "1.0,2.2,3.5,5.0,6.5")
DCA_ADDON_SIZES          = _env_str("DCA_ADDON_SIZES",           "50,65,75,65,40")

DCA_TP_PCT               = _env_float("DCA_TP_PCT",              0.55)
DCA_SL_PCT               = _env_float("DCA_SL_PCT",              999.0)
DCA_SL_CONFIRM_CANDLES   = _env_int("DCA_SL_CONFIRM_CANDLES",    2)
DCA_BREAKEVEN_TRIGGER_PCT = _env_float("DCA_BREAKEVEN_TRIGGER_PCT", 999.0)
DCA_ADDON_COOLDOWN_SECONDS = _env_int("DCA_ADDON_COOLDOWN_SECONDS", 300)
DCA_MIN_RECOVERY_SCORE   = _env_int("DCA_MIN_RECOVERY_SCORE",    1)

# ─────────────────────────────────────────────
# PHANTOM OS — 5-Level L-Phantom სისტემა (FIX #23)
# ─────────────────────────────────────────────
# LP-ების L1-L2 შუალედში განაწილება:
#   LP1 @ L1 x (1-1.0%)   LP2 @ L1 x (1-2.0%)
#   LP3 @ L1 x (1-3.5%)   LP4 @ L1 x (1-5.0%)
#   LP5 @ L1 x (1-6.0%)
#
# PHANTOM_ENABLED=true  -> PHANTOM OS (5 levels, price-triggered)
# PHANTOM_ENABLED=false -> ძველი single LP (LP_ENABLED)
# ─────────────────────────────────────────────

PHANTOM_ENABLED = _env_bool("PHANTOM_ENABLED", "false")          # master switch
PHANTOM_LEVELS  = _env_str("PHANTOM_LEVELS",   "1.0,2.0,3.5,5.0,6.0")  # drop % per level
PHANTOM_QUOTES  = _env_str("PHANTOM_QUOTES",   "15,15,15,15,15")         # USDT per level
