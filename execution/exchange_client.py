import os
import time
import logging
from typing import Any, Dict, Optional

import ccxt

logger = logging.getLogger("gbm")


class ExchangeClientError(Exception):
    pass


class LiveTradingBlocked(Exception):
    pass


class BinanceSpotClient:
    """
    Bybit Spot client — იგივე interface, Bybit backend.
    კლასის სახელი შენარჩუნებულია (BinanceSpotClient) რათა
    დანარჩენი კოდი (execution_engine.py და სხვა) ცვლილების გარეშე იმუშაოს.
    """

    def __init__(self):
        self.mode = os.getenv("MODE", "DEMO").upper()  # DEMO | TESTNET | LIVE
        self.kill_switch = os.getenv("KILL_SWITCH", "false").lower() == "true"
        self.live_confirmation = os.getenv("LIVE_CONFIRMATION", "false").lower() == "true"

        self.max_quote_per_trade = float(os.getenv("MAX_QUOTE_PER_TRADE", "10"))
        self.symbol_whitelist = set(
            s.strip().upper()
            for s in os.getenv("SYMBOL_WHITELIST", "BTC/USDT").split(",")
            if s.strip()
        )

        self.order_retry_count    = int(os.getenv("ORDER_RETRY_COUNT",    "3"))
        self.order_retry_delay_ms = int(os.getenv("ORDER_RETRY_DELAY_MS", "400"))

        self.spread_limit_pct = float(
            os.getenv("SPREAD_LIMIT_PERCENT") or os.getenv("MAX_SPREAD_PCT") or "0.12"
        )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # BYBIT API KEYS (Render ENV-დან)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        api_key    = os.getenv("BYBIT_API_KEY",    "").strip()
        api_secret = os.getenv("BYBIT_API_SECRET", "").strip()

        if self.mode in ("LIVE", "TESTNET"):
            if not api_key or not api_secret:
                raise ExchangeClientError("Missing BYBIT_API_KEY / BYBIT_API_SECRET for LIVE/TESTNET.")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # ccxt.bybit — Spot რეჟიმი
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        exchange_config = {
            "apiKey":          api_key,
            "secret":          api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",  # Spot trading
            },
        }

        if self.mode == "TESTNET":
            exchange_config["options"]["testnet"] = True
            logger.info("BYBIT_TESTNET | testnet mode enabled")

        self.exchange = ccxt.bybit(exchange_config)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # BYBIT GEO-BLOCK FIX
        # Render-ის IP საქართველოდან — Bybit CloudFront ბლოკავს:
        #   /v5/asset/coin/query-info  → fetchCurrencies
        #   /v5/account/fee-rate       → fetchTradingFees
        # ამ endpoint-ების გათიშვა load_markets-ამდე სავალდებულოა.
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        self.exchange.options["fetchCurrencies"]  = False
        self.exchange.options["fetchTradingFees"] = False
        self.exchange.options["fetchBalance"]     = "unified"  # Unified Trading Account

        # warm up markets for precision helpers
        try:
            self.exchange.load_markets()
            logger.info("BYBIT_LOAD_MARKETS | OK")
        except Exception as e:
            logger.warning(f"LOAD_MARKETS_WARN | err={e}")

    def _guard(self, symbol: str, quote_amount: Optional[float] = None) -> None:
        if self.kill_switch:
            raise LiveTradingBlocked("KILL_SWITCH is ON.")
        if self.mode == "LIVE" and not self.live_confirmation:
            raise LiveTradingBlocked("LIVE_CONFIRMATION is OFF.")
        if self.mode == "DEMO":
            raise LiveTradingBlocked("MODE=DEMO -> exchange client must not execute real orders.")
        if symbol and symbol.upper() not in self.symbol_whitelist:
            raise LiveTradingBlocked(f"Symbol not allowed by whitelist: {symbol}.")
        if quote_amount is not None and quote_amount > self.max_quote_per_trade:
            raise LiveTradingBlocked(f"quote_amount {quote_amount} exceeds MAX_QUOTE_PER_TRADE={self.max_quote_per_trade}")

    def _with_retry(self, fn, *args, label: str = "ORDER", **kwargs):
        """
        ORDER_RETRY_COUNT / ORDER_RETRY_DELAY_MS — exponential backoff.
        NetworkError / RequestTimeout → retry. სხვა exception → immediately raise.
        """
        delay_s  = self.order_retry_delay_ms / 1000.0
        last_err = None
        for attempt in range(1, self.order_retry_count + 1):
            try:
                return fn(*args, **kwargs)
            except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
                last_err = e
                if attempt < self.order_retry_count:
                    wait = delay_s * (2 ** (attempt - 1))
                    logger.warning(
                        f"{label}_RETRY | attempt={attempt}/{self.order_retry_count} "
                        f"wait={wait:.2f}s err={e}"
                    )
                    time.sleep(wait)
            except Exception:
                raise
        raise ExchangeClientError(
            f"{label}_RETRY_EXHAUSTED after {self.order_retry_count} attempts | last_err={last_err}"
        )

    def diagnostics(self) -> Dict[str, Any]:
        try:
            sym = next(iter(self.symbol_whitelist)) if self.symbol_whitelist else "BTC/USDT"
            t = self.exchange.fetch_ticker(sym)
            usdt_free = self.fetch_balance_free("USDT")
            return {
                "mode": self.mode,
                "kill_switch": self.kill_switch,
                "live_confirmation": self.live_confirmation,
                "symbol_probe": sym,
                "last_price": float(t.get("last") or 0.0),
                "usdt_free": usdt_free,
                "ok": True,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def fetch_last_price(self, symbol: str) -> float:
        t = self.exchange.fetch_ticker(symbol)
        return float(t["last"])

    def get_min_notional(self, symbol: str) -> float:
        """
        Bybit Spot minimum notional (quote value) for an order.
        """
        try:
            m = self.exchange.market(symbol)

            # 1) ccxt normalized limits
            cost_min = (((m.get("limits") or {}).get("cost") or {}).get("min"))
            if cost_min is not None:
                return float(cost_min)

            # 2) raw Bybit info filters
            info = m.get("info") or {}
            # Bybit uses lotSizeFilter / priceFilter
            lot_filter = info.get("lotSizeFilter") or {}
            min_order_qty = lot_filter.get("minOrderQty")
            if min_order_qty is not None:
                # qty-based min — convert to notional approximation
                try:
                    price = self.fetch_last_price(symbol)
                    return float(min_order_qty) * price
                except Exception:
                    pass

        except Exception as e:
            logger.warning(f"MIN_NOTIONAL_LOOKUP_FAIL | symbol={symbol} err={e}")

        return 0.0

    def fetch_balance_free(self, asset: str) -> float:
        try:
            bal = self.exchange.fetch_balance({"type": "unified"})
            free = float((bal.get("free", {}) or {}).get(asset.upper(), 0.0) or 0.0)
            if free == 0.0:
                bal2 = self.exchange.fetch_balance({"type": "spot"})
                free = float((bal2.get("free", {}) or {}).get(asset.upper(), 0.0) or 0.0)
            return free
        except Exception as e:
            logger.warning(f"FETCH_BALANCE_FAIL | asset={asset} err={e}")
            return 0.0

    def fetch_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.exchange.fetch_order(str(order_id), symbol)

    def cancel_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        return self.exchange.cancel_order(str(order_id), symbol)

    # ----------------------------
    # Precision helpers (STRING!)
    # ----------------------------
    def floor_amount(self, symbol: str, amount: float) -> float:
        try:
            s = self.exchange.amount_to_precision(symbol, amount)
            return float(s)
        except Exception:
            return float(amount)

    def floor_price(self, symbol: str, price: float) -> float:
        try:
            s = self.exchange.price_to_precision(symbol, price)
            return float(s)
        except Exception:
            return float(price)

    def _amount_str(self, symbol: str, amount: float) -> str:
        return str(self.exchange.amount_to_precision(symbol, amount))

    def _price_str(self, symbol: str, price: float) -> str:
        return str(self.exchange.price_to_precision(symbol, price))

    # ----------------------------
    # Orders
    # ----------------------------
    def place_market_buy_by_quote(self, symbol: str, quote_amount: float) -> Dict[str, Any]:
        """
        Bybit Spot market buy by quote (USDT).
        Bybit ccxt-ში: create_order(..., params={"quoteOrderQty": ...}) ან
        createMarketBuyOrderWithCost — ccxt unified method.
        """
        self._guard(symbol, quote_amount=quote_amount)
        try:
            # ccxt unified: cost-based market buy
            return self._with_retry(
                self.exchange.create_order,
                symbol, "market", "buy", None, None,
                {"quoteOrderQty": float(quote_amount)},
                label="MARKET_BUY"
            )
        except ExchangeClientError:
            raise
        except Exception as e:
            raise ExchangeClientError(f"Market buy failed: {e}")

    def place_market_sell(self, symbol: str, base_amount: float) -> Dict[str, Any]:
        self._guard(symbol)
        try:
            amt = float(self.exchange.amount_to_precision(symbol, base_amount))
            return self._with_retry(
                self.exchange.create_order, symbol, "market", "sell", float(amt), None,
                label="MARKET_SELL"
            )
        except ExchangeClientError:
            raise
        except Exception as e:
            raise ExchangeClientError(f"Market sell failed: {e}")

    def place_limit_sell_amount(self, symbol: str, base_amount: float, price: float) -> Dict[str, Any]:
        self._guard(symbol)
        try:
            amt = float(self.exchange.amount_to_precision(symbol, base_amount))
            px  = float(self.exchange.price_to_precision(symbol, price))
            return self._with_retry(
                self.exchange.create_order, symbol, "limit", "sell", float(amt), float(px),
                label="LIMIT_SELL"
            )
        except ExchangeClientError:
            raise
        except Exception as e:
            raise ExchangeClientError(f"Limit sell failed: {e}")

    def place_stop_loss_limit_sell(self, symbol: str, base_amount: float, stop_price: float, limit_price: float) -> Dict[str, Any]:
        self._guard(symbol)
        try:
            amt      = float(self.exchange.amount_to_precision(symbol, base_amount))
            stop_px  = float(self.exchange.price_to_precision(symbol, stop_price))
            limit_px = float(self.exchange.price_to_precision(symbol, limit_price))
            # Bybit Spot stop-limit: triggerPrice param
            params = {
                "triggerPrice": stop_px,
                "triggerBy":    "LastPrice",
                "orderType":    "Limit",
                "timeInForce":  "GTC",
            }
            return self._with_retry(
                self.exchange.create_order, symbol, "limit", "sell",
                float(amt), float(limit_px), params,
                label="SL_LIMIT_SELL"
            )
        except ExchangeClientError:
            raise
        except Exception as e:
            raise ExchangeClientError(f"Stop-loss-limit sell failed: {e}")

    def place_oco_sell(self, symbol: str, base_amount: float, tp_price: float, sl_stop_price: float, sl_limit_price: float) -> Dict[str, Any]:
        """
        Bybit Spot-ს native OCO არ აქვს Binance-ის მსგავსად.
        ვათავსებთ ორ ცალკე ორდერს: Limit TP + Stop-Limit SL.
        OCO semantics: execution_engine უნდა მართავდეს cancel-ს fill-ის შემდეგ.
        """
        self._guard(symbol)
        try:
            qty      = self._amount_str(symbol, base_amount)
            tp_px    = self._price_str(symbol, tp_price)
            sl_stop  = self._price_str(symbol, sl_stop_price)
            sl_limit = self._price_str(symbol, sl_limit_price)

            # 1) TP — Limit Sell
            tp_order = self._with_retry(
                self.exchange.create_order,
                symbol, "limit", "sell", float(qty), float(tp_px),
                {"timeInForce": "GTC"},
                label="OCO_TP"
            )

            # 2) SL — Stop-Limit Sell
            sl_order = self._with_retry(
                self.exchange.create_order,
                symbol, "limit", "sell", float(qty), float(sl_limit),
                {
                    "triggerPrice": float(sl_stop),
                    "triggerBy":    "LastPrice",
                    "timeInForce":  "GTC",
                },
                label="OCO_SL"
            )

            return {
                "raw": {
                    "tp_order": tp_order,
                    "sl_order": sl_order,
                    "oco_emulated": True,
                }
            }
        except Exception as e:
            raise ExchangeClientError(f"OCO sell failed: {e}")
