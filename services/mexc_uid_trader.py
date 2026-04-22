"""
MEXC UID Flip Trading Engine
Торговля через браузерную сессию (UID + cookies/bearer token).
Использует внутренние endpoint'ы MEXC web интерфейса.

Принцип работы:
1. Пользователь логинится на futures.mexc.com в браузере
2. Копирует из DevTools:
   - uid (userId из localStorage или ответов API)
   - bearer token (Authorization header)
   - cookies (session cookies)
3. Бот имитирует браузерные запросы к внутренним API MEXC

Преимущества:
- Нулевая комиссия (используются акции веб-интерфейса)
- Доступ к промо-тарифам
- Нет ограничений API rate limit
"""
import asyncio
import aiohttp
import json
import time
import logging
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Tuple
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass, field
from collections import deque

from config import settings
from database.models import Database

logger = logging.getLogger(__name__)


class MexcUIDClient:
    """
    Клиент для торговли через MEXC web session (UID).
    Использует cookies + bearer token из браузерной сессии.
    """

    BASE_URL = "https://futures.mexc.com"
    API_URL = "https://futures.mexc.com/api/v1/private"
    PUBLIC_URL = "https://futures.mexc.com/api/v1/public"

    def __init__(self, uid: str = None, bearer_token: str = None, cookies: str = None):
        """
        Args:
            uid: User ID из MEXC (виден в DevTools → localStorage → userId)
            bearer_token: Bearer token из DevTools → Network → Authorization header
            cookies: Cookies из браузера (формат: "key1=val1; key2=val2")
        """
        self.uid = uid
        self.bearer_token = bearer_token
        self.cookies_raw = cookies
        self._session: Optional[aiohttp.ClientSession] = None
        self._contract_cache: Dict[str, dict] = {}

    def _get_headers(self) -> dict:
        """Формируем заголовки браузерного запроса"""
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://futures.mexc.com",
            "Referer": "https://futures.mexc.com/exchange/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        return headers

    def _get_cookies_dict(self) -> dict:
        """Парсим cookies из строки в словарь"""
        cookies = {}
        if self.cookies_raw:
            for pair in self.cookies_raw.split(";"):
                pair = pair.strip()
                if "=" in pair:
                    key, val = pair.split("=", 1)
                    cookies[key.strip()] = val.strip()
        return cookies

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            cookie_jar = aiohttp.CookieJar()
            cookies = self._get_cookies_dict()

            # Создаем сессию с cookies
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                cookies=cookies,
                cookie_jar=cookie_jar
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    #  Публичные методы (не требуют auth)
    # ------------------------------------------------------------------

    async def get_server_time(self) -> int:
        """Получить время сервера"""
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.PUBLIC_URL}/ping",
                headers=self._get_headers(),
            ) as resp:
                data = await resp.json()
                return data.get('data', int(time.time() * 1000))
        except Exception as e:
            logger.error(f"[MexcUID] Server time error: {e}")
            return int(time.time() * 1000)

    async def get_contract_detail(self, symbol: str) -> dict:
        """Получить детали контракта"""
        mexc_symbol = f"{symbol.upper()}_USDT"

        if mexc_symbol in self._contract_cache:
            return self._contract_cache[mexc_symbol]

        session = await self._get_session()
        try:
            async with session.get(
                f"{self.PUBLIC_URL}/contract/detail",
                params={"symbol": mexc_symbol},
                headers=self._get_headers(),
            ) as resp:
                data = await resp.json()
                if data.get("success") or data.get("code") == 0:
                    contract = data.get("data", {})
                    self._contract_cache[mexc_symbol] = contract
                    logger.info(
                        f"[MexcUID] Contract {mexc_symbol}: "
                        f"volScale={contract.get('volScale')}, minVol={contract.get('minVol')}, "
                        f"contractSize={contract.get('contractSize')}"
                    )
                    return contract
                else:
                    logger.warning(f"[MexcUID] Contract detail failed: {data}")
                    return {}
        except Exception as e:
            logger.error(f"[MexcUID] Contract detail error: {e}")
            return {}

    async def get_quantity_precision(self, symbol: str) -> Tuple[int, float, float]:
        """Получить precision для символа"""
        contract = await self.get_contract_detail(symbol)
        if not contract:
            logger.warning(f"[MexcUID] No contract for {symbol}, using fallback")
            return 3, 0.001, 0.001

        vol_scale = int(contract.get("volScale", 0))
        min_vol = float(contract.get("minVol", 1))
        vol_unit = float(contract.get("volUnit", 1))
        return vol_scale, min_vol, vol_unit

    @staticmethod
    def round_quantity(quantity: float, vol_scale: int, vol_unit: float, min_vol: float) -> float:
        """Округлить quantity согласно правилам MEXC"""
        if quantity <= 0:
            return 0.0

        if vol_scale <= 0:
            quantize_str = "1"
        else:
            quantize_str = "0." + "0" * vol_scale

        rounded = float(Decimal(str(quantity)).quantize(Decimal(quantize_str), rounding=ROUND_DOWN))

        if vol_unit > 0:
            rounded = math.floor(rounded / vol_unit) * vol_unit
            rounded = float(Decimal(str(rounded)).quantize(Decimal(quantize_str), rounding=ROUND_DOWN))

        if rounded < min_vol:
            logger.warning(f"[MexcUID] Qty {rounded} < minVol {min_vol}")
            return 0.0

        return rounded

    # ------------------------------------------------------------------
    #  Приватные методы (требуют auth через bearer + cookies)
    # ------------------------------------------------------------------

    async def test_connection(self) -> dict:
        """Проверить подключение через UID сессию"""
        if not self.uid or not self.bearer_token:
            return {"success": False, "error": "UID or bearer token not configured"}

        balance_result = await self.get_balance()
        if balance_result.get("success"):
            return {
                "success": True,
                "balance_usdt": balance_result.get("balance_usdt", 0),
                "available": balance_result.get("available", 0),
                "message": "Connected via UID session",
                "uid": self.uid,
            }
        return {"success": False, "error": balance_result.get("error", "Connection failed")}

    async def get_balance(self) -> dict:
        """Получить баланс фьючерсного аккаунта"""
        if not self.bearer_token:
            return {"success": False, "error": "Bearer token not configured"}

        session = await self._get_session()
        try:
            headers = self._get_headers()
            # UID может передаваться как параметр или в заголовке
            params = {}
            if self.uid:
                params["uid"] = self.uid

            async with session.get(
                f"{self.API_URL}/account/assets",
                headers=headers,
                params=params,
            ) as resp:
                data = await resp.json()

                # Логируем для отладки
                logger.debug(f"[MexcUID] Balance raw: {json.dumps(data, default=str)[:500]}")

                if data.get("success") or data.get("code") == 0:
                    assets = data.get("data", [])
                    if not assets:
                        return {"success": True, "balance_usdt": 0.0, "available": 0.0}

                    # Ищем USDT
                    usdt_asset = next(
                        (a for a in assets if str(a.get("currency", "")).upper() == "USDT"),
                        None
                    )
                    if not usdt_asset and assets:
                        usdt_asset = assets[0]

                    if usdt_asset:
                        balance_fields = ["equity", "marginBalance", "cashBalance", "walletBalance"]
                        available_fields = ["availableBalance", "availableOpen", "cashBalance", "equity"]

                        balance = 0.0
                        for f in balance_fields:
                            val = usdt_asset.get(f)
                            if val is not None and float(val) > 0:
                                balance = float(val)
                                break

                        available = 0.0
                        for f in available_fields:
                            val = usdt_asset.get(f)
                            if val is not None and float(val) > 0:
                                available = float(val)
                                break

                        if available == 0 and balance > 0:
                            available = balance

                        return {
                            "success": True,
                            "balance_usdt": balance,
                            "available": available,
                            "currency": usdt_asset.get("currency", "USDT"),
                        }

                    return {"success": True, "balance_usdt": 0.0, "available": 0.0}
                else:
                    code = data.get("code")
                    msg = data.get("message", "")
                    if code == 401:
                        return {"success": False, "error": "Session expired. Get new bearer token from browser."}
                    return {"success": False, "error": f"MEXC error: code={code}, msg={msg}"}
        except Exception as e:
            logger.error(f"[MexcUID] Balance error: {e}")
            return {"success": False, "error": str(e)}

    async def get_position(self, symbol: str) -> dict:
        """Получить текущую позицию по символу"""
        if not self.bearer_token:
            return {"success": False, "error": "Bearer token not configured"}

        session = await self._get_session()
        try:
            headers = self._get_headers()
            mexc_symbol = f"{symbol.upper()}_USDT"

            # Параметры запроса
            params = {}
            if self.uid:
                params["uid"] = self.uid

            async with session.get(
                f"{self.API_URL}/position/open_positions",
                headers=headers,
                params=params,
            ) as resp:
                data = await resp.json()

                if data.get("success") or data.get("code") == 0:
                    positions = data.get("data", [])
                    for pos in positions:
                        if pos.get("symbol") == mexc_symbol:
                            return {
                                "success": True,
                                "position": {
                                    "symbol": symbol,
                                    "side": pos.get("positionType"),  # 1=long, 2=short
                                    "volume": float(pos.get("holdVol", 0)),
                                    "avg_price": float(pos.get("holdAvgPrice", 0)),
                                    "leverage": int(pos.get("leverage", 1)),
                                    "pnl": float(pos.get("unrealizedPnl", 0)),
                                    "margin": float(pos.get("holdMargin", 0)),
                                }
                            }
                    return {"success": True, "position": None}
                else:
                    return {"success": False, "error": f"MEXC error: {data}"}
        except Exception as e:
            logger.error(f"[MexcUID] Get position error: {e}")
            return {"success": False, "error": str(e)}

    async def set_leverage(self, symbol: str, leverage: int, position_type: int = 1) -> bool:
        """Установить плечо"""
        if not self.bearer_token:
            logger.warning("[MexcUID] No bearer token, skipping leverage set")
            return True

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            body = {
                "symbol": mexc_symbol,
                "leverage": leverage,
                "openType": 1,  # isolated
                "positionType": position_type,  # 1=long, 2=short
            }
            if self.uid:
                body["uid"] = self.uid

            async with session.post(
                f"{self.API_URL}/position/change_leverage",
                json=body,
                headers=self._get_headers(),
            ) as resp:
                data = await resp.json()
                if data.get("success") or data.get("code") == 0:
                    logger.info(f"[MexcUID] Leverage set to {leverage}x for {symbol}")
                    return True
                else:
                    logger.warning(f"[MexcUID] Leverage set failed: {data}")
                    return False
        except Exception as e:
            logger.error(f"[MexcUID] Set leverage error: {e}")
            return False

    async def open_long(self, symbol: str, quantity: float, leverage: int = None) -> dict:
        """Открыть лонг позицию через UID сессию"""
        return await self._place_order(
            symbol=symbol,
            side=1,  # Open Long
            quantity=quantity,
            leverage=leverage,
            position_type=1,  # long
        )

    async def close_long(self, symbol: str, quantity: float) -> dict:
        """Закрыть лонг позицию через UID сессию"""
        return await self._place_order(
            symbol=symbol,
            side=4,  # Close Long
            quantity=quantity,
            position_type=1,  # long
        )

    async def open_short(self, symbol: str, quantity: float, leverage: int = None) -> dict:
        """Открыть шорт позицию через UID сессию"""
        return await self._place_order(
            symbol=symbol,
            side=3,  # Open Short
            quantity=quantity,
            leverage=leverage,
            position_type=2,  # short
        )

    async def close_short(self, symbol: str, quantity: float) -> dict:
        """Закрыть шорт позицию через UID сессию"""
        return await self._place_order(
            symbol=symbol,
            side=2,  # Close Short
            quantity=quantity,
            position_type=2,  # short
        )

    async def _place_order(
        self,
        symbol: str,
        side: int,
        quantity: float,
        leverage: int = None,
        position_type: int = 1,
    ) -> dict:
        """Универсальный метод размещения ордера через UID сессию"""
        if not self.bearer_token:
            return self._emulate_order(symbol, side, quantity)

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            body = {
                "symbol": mexc_symbol,
                "side": side,
                "vol": quantity,
                "type": 5,  # Market order
                "openType": 1,  # isolated margin
                "positionType": position_type,
            }
            if leverage:
                body["leverage"] = leverage
            if self.uid:
                body["uid"] = self.uid

            logger.info(
                f"[MexcUID] PLACING ORDER: {mexc_symbol} side={side} "
                f"vol={quantity} type=MARKET leverage={leverage}"
            )

            async with session.post(
                f"{self.API_URL}/order/create",
                json=body,
                headers=self._get_headers(),
            ) as resp:
                data = await resp.json()

                if data.get("success") or data.get("code") == 0:
                    result = data.get("data", {})
                    side_names = {1: "OPEN_LONG", 2: "CLOSE_SHORT", 3: "OPEN_SHORT", 4: "CLOSE_LONG"}
                    logger.info(
                        f"[MexcUID] {side_names.get(side, side)} OK: {symbol} "
                        f"orderId={result.get('orderId')}"
                    )
                    return {
                        "success": True,
                        "order_id": result.get("orderId", ""),
                        "price": float(result.get("price", 0) or 0),
                        "quantity": quantity,
                    }
                else:
                    err = self._handle_error(data)
                    logger.error(f"[MexcUID] Order failed: {err}")
                    return err
        except Exception as e:
            logger.error(f"[MexcUID] Place order error: {e}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def _handle_error(data: dict) -> dict:
        """Обработка ошибок MEXC"""
        code = data.get("code")
        msg = data.get("message", "")
        if code == 401:
            return {"success": False, "error": "MEXC: Session expired. Update bearer token from browser."}
        if code == 402:
            return {"success": False, "error": "MEXC: Token expired (402)."}
        if code == 406:
            return {"success": False, "error": "MEXC: IP not in whitelist (406)."}
        return {"success": False, "error": f"MEXC error: code={code}, message={msg}"}

    def _emulate_order(self, symbol: str, side: int, quantity: float) -> dict:
        """Эмуляция ордера в тестовом режиме"""
        import random
        base_price = 50000.0 if symbol.upper() == "BTC" else 3000.0 if symbol.upper() == "ETH" else 100.0
        slippage = random.uniform(-0.0005, 0.0005)
        price = base_price * (1 + slippage)
        side_names = {1: "OPEN_LONG", 2: "CLOSE_SHORT", 3: "OPEN_SHORT", 4: "CLOSE_LONG"}
        logger.info(f"[MexcUID][TEST] Emulated {side_names.get(side, side)} {symbol} qty={quantity} @ {price:.2f}")
        return {
            "success": True,
            "order_id": f"test_uid_{int(time.time() * 1000)}",
            "price": price,
            "quantity": quantity,
            "test_mode": True,
        }

    def update_credentials(self, uid: str = None, bearer_token: str = None, cookies: str = None):
        """Обновить креденшелы UID сессии"""
        if uid:
            self.uid = uid
        if bearer_token:
            self.bearer_token = bearer_token
        if cookies:
            self.cookies_raw = cookies
            # Сбрасываем сессию чтобы пересоздать с новыми cookies
            if self._session and not self._session.closed:
                asyncio.create_task(self._session.close())
            self._session = None
        logger.info("[MexcUID] Credentials updated")


@dataclass
class UIDFlipSettings:
    """Настройки MEXC UID Flip Trading для пользователя"""
    user_id: int
    enabled: bool = False
    selected_symbols: list = field(default_factory=lambda: ["BTC", "ETH", "SOL"])
    leverage: int = 200
    position_size_usd: float = 100.0
    max_daily_flips: int = 300
    max_daily_loss_usd: float = 50.0
    min_price_movement_pct: float = 0.01
    test_mode: bool = True
    # UID session credentials
    uid: str = ""               # MEXC user ID
    bearer_token: str = ""      # Bearer token из браузера
    cookies: str = ""           # Cookies из браузера
    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now().isoformat()


@dataclass
class UIDFlipTrade:
    """Одна сделка MEXC UID Flip Trading"""
    id: Optional[int] = None
    user_id: int = 0
    symbol: str = ""
    direction: str = "long"
    entry_price: float = 0.0
    exit_price: float = 0.0
    pnl_usd: float = 0.0
    pnl_percent: float = 0.0
    leverage: int = 200
    position_size_usd: float = 100.0
    quantity: float = 0.0
    status: str = "open"
    close_reason: str = ""
    binance_entry_price: float = 0.0
    binance_exit_price: float = 0.0
    opened_at: str = field(default_factory=lambda: datetime.now().isoformat())
    closed_at: Optional[str] = None
    duration_ms: int = 0
    metadata: dict = field(default_factory=dict)


class UIDFlipSession:
    """
    Торговая сессия для одного пользователя и символа через UID.
    Управляет открытием/закрытием позиций.
    """

    def __init__(self, user_id: int, symbol: str, flip_settings: UIDFlipSettings,
                 price_tracker: "BinancePriceTracker", mexc_client: MexcUIDClient, db: Database):
        self.user_id = user_id
        self.symbol = symbol.upper()
        self.settings = flip_settings
        self.price_tracker = price_tracker
        self.mexc_client = mexc_client
        self.db = db

        self.is_running = True
        self.has_open_position = False
        self.current_trade_id: Optional[int] = None
        self.entry_price = 0.0
        self.entry_binance_price = 0.0
        self.trades_count = 0
        self.pnl_today = 0.0
        self._position_lock = asyncio.Lock()
        self._opened_at: Optional[str] = None
        self.current_quantity: float = 0.0
        self._open_failure_cooldown_until: float = 0.0
        self.stop_loss_pct: float = 2.0
        self.take_profit_pct: float = 5.0
        self.max_position_duration_sec: int = 300
        self.current_direction: Optional[str] = None

    async def _on_price_signal(self, user_id: int, symbol: str, direction: str, price: float):
        if user_id != self.user_id or symbol != self.symbol:
            return
        await self.on_price_direction(direction, price)

    async def run(self):
        try:
            await self._run_impl()
        except Exception as e:
            logger.error(f"[UIDFlipSession] FATAL user={self.user_id} sym={self.symbol}: {e}", exc_info=True)
            try:
                self.price_tracker.unsubscribe_from_signals(self.user_id, self.symbol)
            except Exception:
                pass

    async def _run_impl(self):
        logger.info(
            f"[UIDFlipSession] START user={self.user_id} symbol={self.symbol} "
            f"test={self.settings.test_mode} leverage={self.settings.leverage}x "
            f"margin=${self.settings.position_size_usd} uid={'set' if self.settings.uid else 'not_set'}"
        )

        await self._load_open_position()

        self.price_tracker.subscribe_to_signals(
            self._on_price_signal, self.user_id, self.symbol
        )
        logger.info(f"[UIDFlipSession] Subscribed: user={self.user_id}, symbol={self.symbol}")

        try:
            if not self.settings.test_mode:
                logger.info(f"[UIDFlipSession] Setting leverage {self.settings.leverage}x for {self.symbol}")
                lev_long = await self.mexc_client.set_leverage(self.symbol, self.settings.leverage, position_type=1)
                lev_short = await self.mexc_client.set_leverage(self.symbol, self.settings.leverage, position_type=2)
                if lev_long and lev_short:
                    logger.info(f"[UIDFlipSession] Leverage OK: {self.symbol} {self.settings.leverage}x")
                else:
                    logger.warning(f"[UIDFlipSession] Leverage issue for {self.symbol}")

            heartbeat_counter = 0
            while self.is_running:
                try:
                    await asyncio.sleep(1)
                    heartbeat_counter += 1
                    if heartbeat_counter >= 30:
                        heartbeat_counter = 0
                        pos_status = "OPEN" if self.has_open_position else "flat"
                        latest_price = self.price_tracker.get_latest_price(self.symbol)
                        if self.has_open_position:
                            await self._check_sl_tp_time()
                        pos_size = self.settings.position_size_usd * self.settings.leverage
                        logger.info(
                            f"[UIDFlipSession] HEARTBEAT user={self.user_id} sym={self.symbol} "
                            f"pos={pos_status} trades={self.trades_count} pnl=${self.pnl_today:.4f} "
                            f"margin=${self.settings.position_size_usd:.2f} pos=${pos_size:.0f} "
                            f"price={latest_price:.2f}"
                        )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"[UIDFlipSession] Loop error: {e}")
        finally:
            self.price_tracker.unsubscribe_from_signals(self.user_id, self.symbol)
            if self.has_open_position:
                logger.info(f"[UIDFlipSession] Closing position on stop: user={self.user_id}, sym={self.symbol}")
                await self._close_position("session_stop")

        logger.info(f"[UIDFlipSession] STOPPED user={self.user_id}, sym={self.symbol}")

    async def on_price_direction(self, direction: str, binance_price: float):
        """Обработка сигнала направления"""
        if not self.is_running:
            return

        pos_status = "OPEN" if self.has_open_position else "flat"
        logger.info(
            f"[UIDFlipSession] SIGNAL user={self.user_id} sym={self.symbol} "
            f"direction={direction} price={binance_price:.4f} pos={pos_status}"
        )

        async with self._position_lock:
            try:
                if direction == "up" and not self.has_open_position:
                    now = time.time()
                    if now < self._open_failure_cooldown_until:
                        remaining = int(self._open_failure_cooldown_until - now)
                        logger.info(f"[UIDFlipSession] COOLDOWN: {remaining}s remaining")
                        return
                    logger.info(f"[UIDFlipSession] OPENING long: user={self.user_id}, sym={self.symbol}")
                    await self._open_position(binance_price)

                elif direction == "up" and self.has_open_position and self.current_direction == "short":
                    logger.info(f"[UIDFlipSession] CLOSING short (reverse): user={self.user_id}, sym={self.symbol}")
                    await self._close_position("reverse")

                elif direction == "down" and not self.has_open_position:
                    now = time.time()
                    if now < self._open_failure_cooldown_until:
                        remaining = int(self._open_failure_cooldown_until - now)
                        logger.debug(f"[UIDFlipSession] COOLDOWN: {remaining}s")
                        return
                    logger.info(f"[UIDFlipSession] OPENING short: user={self.user_id}, sym={self.symbol}")
                    await self._open_short_position(binance_price)

                elif direction == "down" and self.has_open_position and self.current_direction == "long":
                    logger.info(f"[UIDFlipSession] CLOSING long (reverse): user={self.user_id}, sym={self.symbol}")
                    await self._close_position("reverse")

                else:
                    logger.info(
                        f"[UIDFlipSession] SIGNAL IGNORED: direction={direction} "
                        f"has_pos={self.has_open_position} dir={self.current_direction}"
                    )
            except Exception as e:
                logger.error(f"[UIDFlipSession] Direction handler error: {e}", exc_info=True)

    async def _open_position(self, binance_price: float):
        """Открыть лонг"""
        try:
            today_count = await self.db.get_today_uid_flip_count(self.user_id)
            if today_count >= self.settings.max_daily_flips:
                logger.info(f"[UIDFlipSession] Daily limit reached: {today_count}")
                return

            today_pnl = await self.db.get_today_uid_flip_pnl(self.user_id)
            if today_pnl <= -self.settings.max_daily_loss_usd:
                logger.info(f"[UIDFlipSession] Daily loss limit: ${today_pnl:.2f}")
                return

            margin_usd = self.settings.position_size_usd
            position_size = margin_usd * self.settings.leverage

            if not self.settings.test_mode:
                bal = await self.mexc_client.get_balance()
                if not bal.get("success"):
                    logger.error(f"[UIDFlipSession] Balance check failed: {bal.get('error')}")
                    return
                available = bal.get("available", 0)
                if available < margin_usd:
                    logger.error(
                        f"[UIDFlipSession] INSUFFICIENT MARGIN: available=${available:.2f}, "
                        f"required=${margin_usd:.2f}"
                    )
                    return

            contract = await self.mexc_client.get_contract_detail(self.symbol)
            if contract:
                contract_size = float(contract.get("contractSize", 1))
                vol_scale = int(contract.get("volScale", 0))
                min_vol = float(contract.get("minVol", 1))
                vol_unit = float(contract.get("volUnit", 1))
            else:
                contract_size = 1.0
                vol_scale, min_vol, vol_unit = 3, 0.001, 0.001

            contract_value = binance_price * contract_size
            raw_quantity = position_size / contract_value if contract_value > 0 else 0
            quantity = self.mexc_client.round_quantity(raw_quantity, vol_scale, vol_unit, min_vol)

            if quantity <= 0:
                logger.warning(f"[UIDFlipSession] Invalid quantity: raw={raw_quantity:.6f}, rounded={quantity}")
                return

            logger.info(
                f"[UIDFlipSession] PLACING LONG: user={self.user_id} sym={self.symbol} "
                f"qty={quantity} margin=${margin_usd:.2f} pos=${position_size:.0f} "
                f"mode={'TEST' if self.settings.test_mode else 'REAL'}"
            )

            result = await self.mexc_client.open_long(self.symbol, quantity, self.settings.leverage)

            if not result.get("success"):
                err_msg = result.get("error", "Unknown")
                logger.error(f"[UIDFlipSession] OPEN LONG FAILED: {err_msg}")
                self._open_failure_cooldown_until = time.time() + 5
                return

            entry_price = result.get("price", binance_price)
            self.entry_price = entry_price
            self.entry_binance_price = binance_price
            self.has_open_position = True
            self.current_direction = "long"
            self.current_quantity = quantity

            now_utc = datetime.now(timezone.utc)
            self._opened_at = now_utc.isoformat()

            trade = UIDFlipTrade(
                user_id=self.user_id,
                symbol=self.symbol,
                direction="long",
                entry_price=entry_price,
                leverage=self.settings.leverage,
                position_size_usd=self.settings.position_size_usd,
                quantity=quantity,
                status="open",
                binance_entry_price=binance_price,
                opened_at=self._opened_at,
                metadata={"test_mode": self.settings.test_mode, "uid_mode": True},
            )
            self.current_trade_id = await self.db.add_uid_flip_trade(trade)
            self.trades_count += 1
            self._open_failure_cooldown_until = time.time() + 3

            logger.info(
                f"[UIDFlipSession] LONG OPENED #{self.current_trade_id}: {self.symbol} "
                f"@{entry_price:.4f} qty={quantity:.4f} test={self.settings.test_mode}"
            )

        except Exception as e:
            logger.error(f"[UIDFlipSession] Open position error: {e}")

    async def _open_short_position(self, binance_price: float):
        """Открыть шорт"""
        try:
            today_count = await self.db.get_today_uid_flip_count(self.user_id)
            if today_count >= self.settings.max_daily_flips:
                return

            today_pnl = await self.db.get_today_uid_flip_pnl(self.user_id)
            if today_pnl <= -self.settings.max_daily_loss_usd:
                return

            margin_usd = self.settings.position_size_usd
            position_size = margin_usd * self.settings.leverage

            if not self.settings.test_mode:
                bal = await self.mexc_client.get_balance()
                if not bal.get("success"):
                    return
                if bal.get("available", 0) < margin_usd:
                    logger.error(f"[UIDFlipSession] INSUFFICIENT MARGIN")
                    return

            contract = await self.mexc_client.get_contract_detail(self.symbol)
            if contract:
                contract_size = float(contract.get("contractSize", 1))
                vol_scale = int(contract.get("volScale", 0))
                min_vol = float(contract.get("minVol", 1))
                vol_unit = float(contract.get("volUnit", 1))
            else:
                contract_size = 1.0
                vol_scale, min_vol, vol_unit = 3, 0.001, 0.001

            contract_value = binance_price * contract_size
            raw_quantity = position_size / contract_value if contract_value > 0 else 0
            quantity = self.mexc_client.round_quantity(raw_quantity, vol_scale, vol_unit, min_vol)

            if quantity <= 0:
                return

            result = await self.mexc_client.open_short(self.symbol, quantity, self.settings.leverage)

            if not result.get("success"):
                logger.error(f"[UIDFlipSession] OPEN SHORT FAILED: {result.get('error')}")
                self._open_failure_cooldown_until = time.time() + 5
                return

            entry_price = result.get("price", binance_price)
            self.entry_price = entry_price
            self.entry_binance_price = binance_price
            self.has_open_position = True
            self.current_direction = "short"
            self.current_quantity = quantity

            now_utc = datetime.now(timezone.utc)
            self._opened_at = now_utc.isoformat()

            trade = UIDFlipTrade(
                user_id=self.user_id,
                symbol=self.symbol,
                direction="short",
                entry_price=entry_price,
                leverage=self.settings.leverage,
                position_size_usd=self.settings.position_size_usd,
                quantity=quantity,
                status="open",
                binance_entry_price=binance_price,
                opened_at=self._opened_at,
                metadata={"test_mode": self.settings.test_mode, "uid_mode": True},
            )
            self.current_trade_id = await self.db.add_uid_flip_trade(trade)
            self.trades_count += 1
            self._open_failure_cooldown_until = time.time() + 3

            logger.info(
                f"[UIDFlipSession] SHORT OPENED #{self.current_trade_id}: {self.symbol} "
                f"@{entry_price:.4f} qty={quantity:.4f}"
            )

        except Exception as e:
            logger.error(f"[UIDFlipSession] Open short error: {e}")

    async def _close_position(self, reason: str):
        """Закрыть позицию (лонг или шорт)"""
        if not self.has_open_position or not self.current_trade_id:
            return

        try:
            binance_price = self.price_tracker.get_latest_price(self.symbol)
            quantity = self.current_quantity

            if quantity <= 0:
                logger.warning(f"[UIDFlipSession] current_quantity not set, recalculating")
                position_size = self.settings.position_size_usd * self.settings.leverage
                raw_quantity = position_size / self.entry_price if self.entry_price > 0 else 0
                vol_scale, min_vol, vol_unit = await self.mexc_client.get_quantity_precision(self.symbol)
                quantity = self.mexc_client.round_quantity(raw_quantity, vol_scale, vol_unit, min_vol)

            if quantity <= 0:
                logger.error(f"[UIDFlipSession] Invalid quantity for close: {quantity}")
                return

            # Пере-округляем
            vol_scale, min_vol, vol_unit = await self.mexc_client.get_quantity_precision(self.symbol)
            quantity = self.mexc_client.round_quantity(quantity, vol_scale, vol_unit, min_vol)

            if quantity <= 0:
                logger.error(f"[UIDFlipSession] Quantity became 0 after rounding")
                return

            direction_label = self.current_direction or "long"
            logger.info(
                f"[UIDFlipSession] PLACING CLOSE: user={self.user_id} sym={self.symbol} "
                f"qty={quantity} dir={direction_label} reason={reason}"
            )

            if self.current_direction == "short":
                result = await self.mexc_client.close_short(self.symbol, quantity)
            else:
                result = await self.mexc_client.close_long(self.symbol, quantity)

            if not result.get("success"):
                err_msg = result.get("error", "Unknown")
                logger.error(f"[UIDFlipSession] CLOSE FAILED: {err_msg}")
                return

            exit_price = result.get("price", binance_price)

            if self.entry_price > 0:
                if self.current_direction == "short":
                    price_change_pct = (self.entry_price - exit_price) / self.entry_price * 100
                else:
                    price_change_pct = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                price_change_pct = 0

            pnl_usd = self.settings.position_size_usd * price_change_pct / 100 * self.settings.leverage

            now = datetime.now(timezone.utc)
            if self._opened_at:
                try:
                    opened_dt = datetime.fromisoformat(self._opened_at.replace("Z", "+00:00"))
                    duration_ms = int((now - opened_dt).total_seconds() * 1000)
                except (ValueError, TypeError):
                    duration_ms = 0
            else:
                duration_ms = 0

            await self.db.close_uid_flip_trade(
                trade_id=self.current_trade_id,
                exit_price=exit_price,
                pnl_usd=pnl_usd,
                pnl_percent=price_change_pct * self.settings.leverage,
                close_reason=reason,
                binance_exit_price=binance_price,
                duration_ms=duration_ms,
            )

            self.pnl_today += pnl_usd
            self.has_open_position = False

            emoji = "GREEN" if pnl_usd >= 0 else "RED"
            logger.info(
                f"[UIDFlipSession] {direction_label.upper()} CLOSED #{self.current_trade_id}: "
                f"{self.symbol} entry={self.entry_price:.4f} exit={exit_price:.4f} "
                f"PnL=${pnl_usd:.4f} dur={duration_ms}ms reason={reason} [{emoji}]"
            )

            self.current_trade_id = None
            self.entry_price = 0.0
            self.entry_binance_price = 0.0
            self.current_direction = None
            self.current_quantity = 0.0
            self._opened_at = None

        except Exception as e:
            logger.error(f"[UIDFlipSession] Close position error: {e}")

    async def _load_open_position(self):
        """Загрузить открытую позицию из БД"""
        try:
            open_trades = await self.db.get_open_uid_flip_trades(self.user_id)
            for trade in open_trades:
                if trade.symbol == self.symbol and trade.status == "open":
                    self.has_open_position = True
                    self.current_trade_id = trade.id
                    self.current_direction = trade.direction
                    self.entry_price = trade.entry_price
                    self.entry_binance_price = trade.binance_entry_price
                    self._opened_at = trade.opened_at
                    self.current_quantity = trade.quantity
                    logger.info(
                        f"[UIDFlipSession] RESTORED: {self.symbol} #{trade.id} "
                        f"dir={trade.direction} qty={trade.quantity}"
                    )
                    return
        except Exception as e:
            logger.error(f"[UIDFlipSession] Load position error: {e}")

    async def _check_sl_tp_time(self):
        """Проверка SL/TP/time limit"""
        if not self.has_open_position or not self.entry_price > 0:
            return

        current_price = self.price_tracker.get_latest_price(self.symbol)
        if current_price <= 0:
            return

        if self.current_direction == "long":
            price_change_pct = (current_price - self.entry_price) / self.entry_price * 100
        elif self.current_direction == "short":
            price_change_pct = (self.entry_price - current_price) / self.entry_price * 100
        else:
            return

        if price_change_pct <= -self.stop_loss_pct:
            logger.info(f"[UIDFlipSession] STOP LOSS: {self.symbol} {price_change_pct:.2f}%")
            await self._close_position("stop_loss")
            return

        if price_change_pct >= self.take_profit_pct:
            logger.info(f"[UIDFlipSession] TAKE PROFIT: {self.symbol} {price_change_pct:.2f}%")
            await self._close_position("take_profit")
            return

        if self._opened_at:
            try:
                opened_dt = datetime.fromisoformat(self._opened_at.replace("Z", "+00:00"))
                elapsed_sec = (datetime.now(timezone.utc) - opened_dt).total_seconds()
                if elapsed_sec >= self.max_position_duration_sec:
                    logger.info(f"[UIDFlipSession] TIME LIMIT: {self.symbol} {elapsed_sec:.0f}s")
                    await self._close_position("time_limit")
                    return
            except (ValueError, TypeError):
                pass

    async def close(self):
        """Закрыть сессию"""
        logger.info(f"[UIDFlipSession] Closing: user={self.user_id}, sym={self.symbol}")
        self.is_running = False
        try:
            self.price_tracker.unsubscribe_from_signals(self.user_id, self.symbol)
        except Exception:
            pass
        if self.has_open_position:
            await self._close_position("manual")


class UIDFlipTrader:
    """Основной движок MEXC UID Flip Trading"""

    def __init__(self):
        self.price_tracker: Optional["BinancePriceTracker"] = None
        self.db = Database(settings.db_file)
        self.running = False
        self._tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        self.active_sessions: Dict[tuple, UIDFlipSession] = {}
        self._sessions_lock = asyncio.Lock()
        self.user_clients: Dict[int, MexcUIDClient] = {}
        self._clients_lock = asyncio.Lock()

    async def start(self):
        self.running = True
        await self.db.initialize()
        logger.info("[UIDFlipTrader] Started")

        self._tasks = [
            asyncio.create_task(self._session_manager_loop(), name="uid_session_manager"),
        ]

        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self):
        logger.info("[UIDFlipTrader] Stopping...")
        self.running = False
        self._shutdown_event.set()

        async with self._sessions_lock:
            for key, session in list(self.active_sessions.items()):
                try:
                    await session.close()
                except Exception as e:
                    logger.error(f"[UIDFlipTrader] Error closing session {key}: {e}")
            self.active_sessions.clear()

        if self.price_tracker:
            try:
                await self.price_tracker.stop()
            except Exception as e:
                logger.error(f"[UIDFlipTrader] Error stopping price tracker: {e}")
            self.price_tracker = None

        async with self._clients_lock:
            for uid, client in list(self.user_clients.items()):
                try:
                    await client.close()
                except Exception as e:
                    logger.error(f"[UIDFlipTrader] Error closing client for user {uid}: {e}")
            self.user_clients.clear()

        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._tasks.clear()
        logger.info("[UIDFlipTrader] Stopped")

    async def start_user_session(self, user_id: int, flip_settings: UIDFlipSettings) -> dict:
        """Запуск сессии для пользователя"""
        try:
            today_count = await self.db.get_today_uid_flip_count(user_id)
            if today_count >= flip_settings.max_daily_flips:
                return {"success": False, "error": f"Daily limit: {today_count}/{flip_settings.max_daily_flips}"}

            today_pnl = await self.db.get_today_uid_flip_pnl(user_id)
            if today_pnl <= -flip_settings.max_daily_loss_usd:
                return {"success": False, "error": f"Daily loss limit: ${today_pnl:.2f}"}

            symbols = flip_settings.selected_symbols
            if not symbols:
                return {"success": False, "error": "No symbols selected"}

            if not flip_settings.test_mode:
                if not flip_settings.uid or not flip_settings.bearer_token:
                    return {"success": False, "error": "UID and Bearer Token required for real trading."}

            async with self._clients_lock:
                old_client = self.user_clients.pop(user_id, None)
                if old_client:
                    try:
                        await old_client.close()
                    except Exception:
                        pass

                client = MexcUIDClient(
                    uid=flip_settings.uid,
                    bearer_token=flip_settings.bearer_token,
                    cookies=flip_settings.cookies,
                )
                self.user_clients[user_id] = client

            if not flip_settings.test_mode:
                logger.info(f"[UIDFlipTrader] Testing UID connection for user {user_id}...")
                conn_test = await client.test_connection()
                if not conn_test.get("success"):
                    err_msg = conn_test.get("error", "Unknown")
                    logger.error(f"[UIDFlipTrader] UID connection failed: {err_msg}")
                    async with self._clients_lock:
                        self.user_clients.pop(user_id, None)
                    try:
                        await client.close()
                    except Exception:
                        pass
                    return {"success": False, "error": f"MEXC UID error: {err_msg}"}
                logger.info(
                    f"[UIDFlipTrader] UID connected for user {user_id}, "
                    f"balance=${conn_test.get('balance_usdt', 0):.2f}"
                )
            else:
                logger.info(f"[UIDFlipTrader] Test mode: skipping UID connection check")

            all_symbols = set(s.upper() for s in symbols)
            async with self._sessions_lock:
                for (uid, _), session in self.active_sessions.items():
                    if uid != user_id:
                        all_symbols.add(session.symbol)

            async with self._sessions_lock:
                keys_to_stop = [k for k in self.active_sessions.keys() if k[0] == user_id]
                for key in keys_to_stop:
                    session = self.active_sessions.pop(key)
                    try:
                        await session.close()
                    except Exception as e:
                        logger.error(f"[UIDFlipTrader] Error stopping old session {key}: {e}")

                if self.price_tracker:
                    self.price_tracker.add_symbols(list(all_symbols))
                else:
                    from services.mexc_flip_trader import BinancePriceTracker
                    self.price_tracker = BinancePriceTracker(
                        symbols=list(all_symbols),
                        window_size=settings.flip_price_history_window,
                    )
                    asyncio.create_task(self.price_tracker.start())
                    await asyncio.sleep(2)

                started_symbols = []
                for symbol in symbols:
                    key = (user_id, symbol.upper())
                    if key in self.active_sessions:
                        continue

                    session = UIDFlipSession(
                        user_id=user_id,
                        symbol=symbol,
                        flip_settings=flip_settings,
                        price_tracker=self.price_tracker,
                        mexc_client=client,
                        db=self.db,
                    )
                    self.active_sessions[key] = session
                    task = asyncio.create_task(session.run())

                    def _on_done(t):
                        if t.cancelled():
                            return
                        exc = t.exception()
                        if exc:
                            logger.error(f"[UIDFlipTrader] Session crashed: user={user_id}, sym={symbol}: {exc}")

                    task.add_done_callback(_on_done)
                    started_symbols.append(symbol.upper())

            position_size = flip_settings.position_size_usd * flip_settings.leverage
            logger.info(
                f"[UIDFlipTrader] UID session started for user {user_id}: "
                f"symbols={started_symbols}, leverage={flip_settings.leverage}x, "
                f"margin=${flip_settings.position_size_usd}, position=${position_size}, "
                f"mode={'TEST' if flip_settings.test_mode else 'REAL'}"
            )
            return {
                "success": True,
                "symbols": started_symbols,
                "leverage": flip_settings.leverage,
                "margin": flip_settings.position_size_usd,
                "position_size": position_size,
                "test_mode": flip_settings.test_mode,
            }

        except Exception as e:
            logger.error(f"[UIDFlipTrader] Start session error for user {user_id}: {e}")
            return {"success": False, "error": str(e)}

    async def stop_user_session(self, user_id: int) -> dict:
        """Остановка сессий пользователя"""
        closed = []
        async with self._sessions_lock:
            keys_to_remove = [k for k in self.active_sessions.keys() if k[0] == user_id]
            for key in keys_to_remove:
                session = self.active_sessions.pop(key)
                try:
                    self.price_tracker.unsubscribe_from_signals(user_id, key[1])
                except Exception:
                    pass
                try:
                    await session.close()
                    closed.append(key[1])
                except Exception as e:
                    logger.error(f"[UIDFlipTrader] Error closing session {key}: {e}")

        if self.price_tracker:
            for sym in closed:
                remaining = [s for s in self.price_tracker._signal_subscribers if s[2] == sym]
                if not remaining:
                    self.price_tracker.remove_symbols([sym])
            if not self.price_tracker._signal_subscribers:
                try:
                    await self.price_tracker.stop()
                except Exception as e:
                    logger.error(f"[UIDFlipTrader] Error stopping price tracker: {e}")
                self.price_tracker = None

        async with self._clients_lock:
            client = self.user_clients.pop(user_id, None)
            if client:
                try:
                    await client.close()
                except Exception as e:
                    logger.error(f"[UIDFlipTrader] Error closing client for user {user_id}: {e}")

        logger.info(f"[UIDFlipTrader] Stopped sessions for user {user_id}: {closed}")
        return {"success": True, "closed_symbols": closed}

    async def _session_manager_loop(self):
        """Цикл управления сессиями"""
        while self.running:
            try:
                await asyncio.sleep(30)

                async with self._sessions_lock:
                    keys_to_remove = []

                    for key, session in list(self.active_sessions.items()):
                        user_id, symbol = key

                        try:
                            today_count = await self.db.get_today_uid_flip_count(user_id)
                            flip_settings = await self.db.get_uid_flip_settings(user_id)

                            if flip_settings:
                                if today_count >= flip_settings.max_daily_flips:
                                    logger.info(f"[UIDFlipTrader] Daily limit for user {user_id}")
                                    await session.close()
                                    keys_to_remove.append(key)
                                    continue

                                today_pnl = await self.db.get_today_uid_flip_pnl(user_id)
                                if today_pnl <= -flip_settings.max_daily_loss_usd:
                                    logger.info(f"[UIDFlipTrader] Daily loss limit for user {user_id}")
                                    await session.close()
                                    keys_to_remove.append(key)
                                    continue

                        except Exception as e:
                            logger.error(f"[UIDFlipTrader] Limit check error: {e}")

                        if not session.is_running:
                            keys_to_remove.append(key)

                    for key in keys_to_remove:
                        self.active_sessions.pop(key, None)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[UIDFlipTrader] Session manager error: {e}")

    async def get_session_status(self, user_id: int) -> dict:
        """Статус сессий пользователя"""
        active_symbols = []
        async with self._sessions_lock:
            for key, session in self.active_sessions.items():
                if key[0] == user_id:
                    active_symbols.append({
                        "symbol": key[1],
                        "has_position": session.has_open_position,
                        "trades_today": session.trades_count,
                        "pnl_today": session.pnl_today,
                    })

        today_count = await self.db.get_today_uid_flip_count(user_id)
        today_pnl = await self.db.get_today_uid_flip_pnl(user_id)
        today_str = datetime.now().strftime("%Y-%m-%d")
        stats = await self.db.get_uid_flip_trade_stats(user_id, since=today_str)

        return {
            "active": len(active_symbols) > 0,
            "symbols": active_symbols,
            "today_count": today_count,
            "today_pnl": today_pnl,
            "today_stats": stats,
        }

    async def get_user_stats(self, user_id: int, since: str = None) -> dict:
        """Детальная статистика"""
        stats = await self.db.get_uid_flip_trade_stats(user_id, since=since)
        open_trades = await self.db.get_open_uid_flip_trades(user_id)

        return {
            "stats": stats,
            "open_trades": len(open_trades),
            "open_trade_details": [
                {
                    "id": t.id,
                    "symbol": t.symbol,
                    "direction": t.direction,
                    "entry_price": t.entry_price,
                    "leverage": t.leverage,
                    "opened_at": t.opened_at,
                } for t in open_trades
            ],
        }

    def is_user_active(self, user_id: int) -> bool:
        for uid, _ in self.active_sessions.keys():
            if uid == user_id:
                return True
        return False


# Глобальный экземпляр
uid_flip_trader = UIDFlipTrader()
