"""
MEXC Flip Trading Engine
Мониторит цены Binance и открывает/закрывает лонги на MEXC фьючерсах
"""
import asyncio
import aiohttp
import websockets
import json
import time
import logging
import hmac
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import deque
from decimal import Decimal, ROUND_DOWN

from config import settings
from database.models import Database, FlipSettings, FlipTrade

logger = logging.getLogger(__name__)


class BinancePriceTracker:
    """Отслеживание цен Binance через WebSocket для определения направления"""

    def __init__(self, symbols: List[str], window_size: int = 20):
        # symbols: список базовых символов без USDT (BTC, ETH и т.д.)
        self.symbols = [s.upper() for s in symbols]
        self.window_size = window_size
        # История цен: {symbol: deque of (timestamp, price)}
        self.price_history: Dict[str, deque] = {}
        self.latest_prices: Dict[str, float] = {}
        self.latest_timestamps: Dict[str, float] = {}
        self.running = False
        self._tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        # Подписчики на сигналы направления: [(callback, user_id, symbol)]
        self._signal_subscribers: List[tuple] = []
        self._lock = asyncio.Lock()

    async def start(self):
        """Запуск WebSocket подключения к Binance"""
        self.running = True
        logger.info(f"Starting Binance price tracker for symbols: {self.symbols}")
        self._tasks = [
            asyncio.create_task(self._ws_price_stream(), name="binance_ws"),
            asyncio.create_task(self._analyze_loop(), name="price_analyze"),
        ]
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self):
        """Остановка трекера"""
        logger.info("Stopping Binance price tracker...")
        self.running = False
        self._shutdown_event.set()
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logger.warning("Price tracker tasks stop timeout")
        self._tasks.clear()
        logger.info("Binance price tracker stopped")

    async def _ws_price_stream(self):
        """WebSocket подключение к Binance futures для получения цен"""
        if not self.symbols:
            logger.warning("No symbols configured for price tracking")
            return

        # Формируем потоки: btcusdt@ticker, ethusdt@ticker и т.д.
        streams = "/".join([f"{s.lower()}usdt@ticker" for s in self.symbols])
        uri = f"wss://fstream.binance.com/stream?streams={streams}"

        reconnect_delay = 1
        max_reconnect_delay = 30

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"Binance WS connected for flip tracking ({len(self.symbols)} symbols)")
                    reconnect_delay = 1  # Сброс при успешном подключении

                    try:
                        async for msg in ws:
                            if not self.running:
                                break
                            try:
                                data = json.loads(msg)
                                payload = data.get('data', {})
                                await self._process_ticker(payload)
                            except json.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.debug(f"WS msg process error: {e}")
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("Binance flip WS closed")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Binance flip WS error: {e}. Reconnect in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def _process_ticker(self, ticker: dict):
        """Обработка тикера от Binance"""
        try:
            symbol = ticker.get('s', '').replace('USDT', '')
            if not symbol or symbol not in self.symbols:
                return

            price = float(ticker.get('c', 0))  # last price
            if price <= 0:
                return

            now = time.time()

            async with self._lock:
                # Инициализируем очередь если нужно
                if symbol not in self.price_history:
                    self.price_history[symbol] = deque(maxlen=self.window_size * 2)

                # Добавляем цену в историю
                self.price_history[symbol].append((now, price))
                self.latest_prices[symbol] = price
                self.latest_timestamps[symbol] = now

        except Exception as e:
            logger.debug(f"Ticker process error: {e}")

    async def _analyze_loop(self):
        """Цикл анализа направления цены"""
        while self.running:
            try:
                # Используем интервал из настроек (в миллисекундах)
                tick_interval = settings.flip_tick_interval_ms / 1000.0
                await asyncio.sleep(tick_interval)

                for symbol in self.symbols:
                    direction = await self._detect_direction(symbol)
                    if direction:
                        # Отправляем сигнал подписчикам
                        for callback, user_id, sub_symbol in self._signal_subscribers:
                            if sub_symbol == symbol:
                                try:
                                    await callback(user_id, symbol, direction, self.latest_prices.get(symbol, 0))
                                except Exception as e:
                                    logger.error(f"Signal callback error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Analyze loop error: {e}")
                await asyncio.sleep(1)

    async def _detect_direction(self, symbol: str) -> Optional[str]:
        """
        Определение направления цены.
        Возвращает: 'up', 'down', или None

        Алгоритм:
        - Сравниваем среднюю цену последних N тиков с средней предыдущих N тиков
        - Если recent_avg > old_avg * (1 + threshold) -> 'up'
        - Если recent_avg < old_avg * (1 - threshold) -> 'down'
        """
        async with self._lock:
            history = self.price_history.get(symbol)
            if not history or len(history) < self.window_size:
                return None

            prices = [p for _, p in history]
            half = len(prices) // 2

            old_avg = sum(prices[:half]) / half
            recent_avg = sum(prices[half:]) / (len(prices) - half)

            if old_avg <= 0:
                return None

            change_pct = (recent_avg - old_avg) / old_avg * 100

            # Минимальное движение из настроек
            min_movement = settings.flip_min_price_movement_pct

            if change_pct > min_movement:
                return 'up'
            elif change_pct < -min_movement:
                return 'down'

            return None

    def get_latest_price(self, symbol: str) -> float:
        """Получить последнюю известную цену"""
        return self.latest_prices.get(symbol.upper(), 0)

    def subscribe_to_signals(self, callback: Callable, user_id: int, symbol: str):
        """Подписаться на сигналы направления"""
        self._signal_subscribers.append((callback, user_id, symbol.upper()))
        logger.info(f"User {user_id} subscribed to {symbol.upper()} signals")

    def unsubscribe(self, user_id: int):
        """Отписать пользователя от сигналов"""
        self._signal_subscribers = [
            s for s in self._signal_subscribers if s[1] != user_id
        ]
        logger.info(f"User {user_id} unsubscribed from all flip signals")

    def add_symbols(self, new_symbols: List[str]):
        """Добавить новые символы для отслеживания (без перезапуска)"""
        for s in new_symbols:
            s = s.upper()
            if s not in self.symbols:
                self.symbols.append(s)
                logger.info(f"Added symbol {s} to price tracker")

    def remove_symbols(self, symbols_to_remove: List[str]):
        """Удалить символы из отслеживания"""
        for s in symbols_to_remove:
            s = s.upper()
            if s in self.symbols:
                self.symbols.remove(s)
                self.price_history.pop(s, None)
                self.latest_prices.pop(s, None)
                self.latest_timestamps.pop(s, None)
                logger.info(f"Removed symbol {s} from price tracker")

    @property
    def tracked_symbols(self) -> List[str]:
        """Список отслеживаемых символов"""
        return list(self.symbols)


class MexcAPI:
    """Обёртка для MEXC Contract API (асинхронная)"""

    BASE_URL = "https://contract.mexc.com"

    def __init__(self, api_key: str = None, api_secret: str = None):
        self.api_key = api_key or settings.mexc_api_key
        self.api_secret = api_secret or settings.mexc_api_secret
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    def _generate_signature(self, params: dict) -> str:
        """Генерация подписи для MEXC API — params отсортированы по ключу"""
        query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def _build_signed_url(self, endpoint: str, params: dict) -> str:
        """Формирует URL с подписью для GET запроса (порядок параметров фиксирован)"""
        # Сначала создаем подпись из отсортированных параметров
        sign = self._generate_signature(params)
        # Затем формируем query string в том же отсортированном порядке + sign в конце
        query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        query_string += f"&sign={sign}"
        return f"{self.BASE_URL}{endpoint}?{query_string}"

    def _build_signed_body(self, params: dict) -> str:
        """Формирует form-encoded тело с подписью для POST запроса"""
        sign = self._generate_signature(params)
        sorted_params = dict(sorted(params.items()))
        sorted_params["sign"] = sign
        return "&".join([f"{k}={v}" for k, v in sorted_params.items()])

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def get_server_time(self) -> int:
        """Получить время сервера MEXC"""
        session = await self._get_session()
        try:
            async with session.get(f"{self.BASE_URL}/api/v1/contract/ping") as resp:
                data = await resp.json()
                return data.get('data', int(time.time() * 1000))
        except Exception as e:
            logger.error(f"MEXC server time error: {e}")
            return int(time.time() * 1000)

    async def get_ticker(self, symbol: str) -> dict:
        """Получить тикер фьючерса MEXC"""
        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            async with session.get(
                f"{self.BASE_URL}/api/v1/contract/ticker",
                params={"symbol": mexc_symbol}
            ) as resp:
                data = await resp.json()
                return data.get('data', {})
        except Exception as e:
            logger.error(f"MEXC ticker error for {symbol}: {e}")
            return {}

    async def get_depth(self, symbol: str) -> dict:
        """Получить стакан ордеров"""
        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            async with session.get(
                f"{self.BASE_URL}/api/v1/contract/depth/{mexc_symbol}"
            ) as resp:
                data = await resp.json()
                return data.get('data', {})
        except Exception as e:
            logger.error(f"MEXC depth error for {symbol}: {e}")
            return {}

    async def set_leverage(self, symbol: str, leverage: int) -> bool:
        """Установить плечо для символа"""
        if not self.api_key or not self.api_secret:
            logger.warning("MEXC API keys not configured, skipping leverage set")
            return True  # В тестовом режиме считаем OK

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            timestamp = await self.get_server_time()
            params = {
                "symbol": mexc_symbol,
                "leverage": leverage,
                "timestamp": timestamp,
                "api_key": self.api_key
            }
            body = self._build_signed_body(params)

            async with session.post(
                f"{self.BASE_URL}/api/v1/private/position/change_leverage",
                data=body,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            ) as resp:
                data = await resp.json()
                if data.get('success') or data.get('code') == 200:
                    logger.info(f"Leverage set to {leverage}x for {symbol}")
                    return True
                else:
                    logger.warning(f"Leverage set failed: {data}")
                    return False
        except Exception as e:
            logger.error(f"Set leverage error: {e}")
            return False

    async def open_long(self, symbol: str, quantity: float) -> dict:
        """Открыть лонг позицию"""
        if not self.api_key or not self.api_secret:
            return self._emulate_order(symbol, "BUY", quantity)

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            timestamp = await self.get_server_time()
            params = {
                "symbol": mexc_symbol,
                "side": 1,  # 1 = Open Long
                "vol": quantity,
                "type": 5,  # 5 = Market order
                "openType": 1,  # isolated margin
                "timestamp": timestamp,
                "api_key": self.api_key
            }
            body = self._build_signed_body(params)

            async with session.post(
                f"{self.BASE_URL}/api/v1/private/order/submit",
                data=body,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            ) as resp:
                data = await resp.json()
                if data.get('success') or data.get('code') == 200:
                    result = data.get('data', {})
                    logger.info(f"Long opened: {symbol} qty={quantity}")
                    return {
                        'success': True,
                        'order_id': result.get('orderId', ''),
                        'price': float(result.get('price', 0) or 0),
                        'quantity': quantity
                    }
                else:
                    logger.error(f"Open long failed: {data}")
                    return {'success': False, 'error': str(data)}
        except Exception as e:
            logger.error(f"Open long error: {e}")
            return {'success': False, 'error': str(e)}

    async def close_long(self, symbol: str, quantity: float) -> dict:
        """Закрыть лонг позицию"""
        if not self.api_key or not self.api_secret:
            return self._emulate_order(symbol, "SELL", quantity)

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            timestamp = await self.get_server_time()
            params = {
                "symbol": mexc_symbol,
                "side": 3,  # 3 = Close Long
                "vol": quantity,
                "type": 5,  # Market order
                "openType": 1,
                "timestamp": timestamp,
                "api_key": self.api_key
            }
            body = self._build_signed_body(params)

            async with session.post(
                f"{self.BASE_URL}/api/v1/private/order/submit",
                data=body,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            ) as resp:
                data = await resp.json()
                if data.get('success') or data.get('code') == 200:
                    result = data.get('data', {})
                    logger.info(f"Long closed: {symbol} qty={quantity}")
                    return {
                        'success': True,
                        'order_id': result.get('orderId', ''),
                        'price': float(result.get('price', 0) or 0),
                        'quantity': quantity
                    }
                else:
                    logger.error(f"Close long failed: {data}")
                    return {'success': False, 'error': str(data)}
        except Exception as e:
            logger.error(f"Close long error: {e}")
            return {'success': False, 'error': str(e)}

    async def get_position(self, symbol: str) -> dict:
        """Получить текущую позицию"""
        if not self.api_key or not self.api_secret:
            return {'success': True, 'position': None}  # Тестовый режим

        session = await self._get_session()
        try:
            mexc_symbol = f"{symbol.upper()}_USDT"
            timestamp = await self.get_server_time()
            params = {
                "symbol": mexc_symbol,
                "timestamp": timestamp,
                "api_key": self.api_key
            }
            url = self._build_signed_url("/api/v1/private/position/open_positions", params)

            async with session.get(url) as resp:
                data = await resp.json()
                if data.get('success') or data.get('code') == 200:
                    positions = data.get('data', [])
                    # Ищем позицию по символу
                    for pos in positions:
                        if pos.get('symbol') == mexc_symbol:
                            return {
                                'success': True,
                                'position': {
                                    'symbol': symbol,
                                    'side': pos.get('positionType'),  # 1=long, 2=short
                                    'volume': float(pos.get('holdVol', 0)),
                                    'avg_price': float(pos.get('holdAvgPrice', 0)),
                                    'leverage': int(pos.get('leverage', 1)),
                                    'pnl': float(pos.get('unrealizedPnl', 0))
                                }
                            }
                    return {'success': True, 'position': None}
                else:
                    return {'success': False, 'error': str(data)}
        except Exception as e:
            logger.error(f"Get position error: {e}")
            return {'success': False, 'error': str(e)}

    async def get_balance(self) -> dict:
        """Получить баланс фьючерсного аккаунта MEXC"""
        if not self.api_key or not self.api_secret:
            return {'success': True, 'balance_usdt': 0.0, 'available': 0.0, 'test_mode': True}

        session = await self._get_session()
        try:
            timestamp = await self.get_server_time()
            params = {
                "timestamp": timestamp,
                "api_key": self.api_key
            }
            url = self._build_signed_url("/api/v1/private/account/assets", params)

            async with session.get(url) as resp:
                data = await resp.json()
                if data.get('success') or data.get('code') == 200:
                    assets = data.get('data', [])
                    usdt_asset = next((a for a in assets if a.get('currency', '').upper() == 'USDT'), None)
                    if usdt_asset:
                        balance = float(usdt_asset.get('totalMarginBalance', 0) or usdt_asset.get('marginBalance', 0) or 0)
                        available = float(usdt_asset.get('availableBalance', 0) or usdt_asset.get('availableOpen', 0) or 0)
                        return {
                            'success': True,
                            'balance_usdt': balance,
                            'available': available
                        }
                    return {'success': True, 'balance_usdt': 0.0, 'available': 0.0}
                else:
                    logger.warning(f"MEXC balance error: {data}")
                    return {'success': False, 'error': str(data)}
        except Exception as e:
            logger.error(f"Get MEXC balance error: {e}")
            return {'success': False, 'error': str(e)}

    async def test_connection(self) -> dict:
        """Проверить подключение к MEXC API"""
        if not self.api_key or not self.api_secret:
            return {'success': False, 'error': 'API keys not configured'}

        balance_result = await self.get_balance()
        if balance_result.get('success'):
            return {
                'success': True,
                'balance_usdt': balance_result.get('balance_usdt', 0),
                'message': 'Connected'
            }
        return {'success': False, 'error': balance_result.get('error', 'Connection failed')}

    def update_credentials(self, api_key: str, api_secret: str):
        """Обновить API ключи"""
        self.api_key = api_key
        self.api_secret = api_secret
        logger.info("MEXC API credentials updated")

    def _emulate_order(self, symbol: str, side: str, quantity: float) -> dict:
        """Эмуляция ордера в тестовом режиме"""
        import random
        base_price = 50000.0 if symbol.upper() == 'BTC' else 3000.0 if symbol.upper() == 'ETH' else 100.0
        slippage = random.uniform(-0.0005, 0.0005)
        price = base_price * (1 + slippage)
        logger.info(f"[TEST] Emulated {side} {symbol} qty={quantity} @ {price:.2f}")
        return {
            'success': True,
            'order_id': f"test_{int(time.time() * 1000)}",
            'price': price,
            'quantity': quantity,
            'test_mode': True
        }


class FlipSession:
    """
    Торговая сессия для одного пользователя и одного символа.
    Управляет открытием/закрытием позиций.
    """

    def __init__(self, user_id: int, symbol: str, flip_settings: FlipSettings,
                 price_tracker: BinancePriceTracker, mexc_api: MexcAPI, db: Database):
        self.user_id = user_id
        self.symbol = symbol.upper()
        self.settings = flip_settings
        self.price_tracker = price_tracker
        self.mexc_api = mexc_api
        self.db = db

        self.is_running = True
        self.has_open_position = False
        self.current_trade_id: Optional[int] = None
        self.entry_price = 0.0
        self.entry_binance_price = 0.0
        self.trades_count = 0
        self.pnl_today = 0.0
        self._position_lock = asyncio.Lock()
        # Храним время открытия сделки для точного расчета длительности
        self._opened_at: Optional[str] = None

    async def run(self):
        """Основной цикл сессии"""
        logger.info(f"FlipSession started: user={self.user_id}, symbol={self.symbol}")

        # Устанавливаем плечо (если не тестовый режим)
        if not self.settings.test_mode:
            leverage_set = await self.mexc_api.set_leverage(self.symbol, self.settings.leverage)
            if not leverage_set:
                logger.warning(f"Failed to set leverage for {self.symbol}, continuing anyway")
        else:
            logger.info(f"Test mode: skipping leverage set for {self.symbol}")

        while self.is_running:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Session loop error: user={self.user_id}, symbol={self.symbol}: {e}")

        # Закрываем позицию при остановке сессии
        if self.has_open_position:
            await self._close_position("session_stop")

        logger.info(f"FlipSession stopped: user={self.user_id}, symbol={self.symbol}")

    async def on_price_direction(self, direction: str, binance_price: float):
        """
        Обработка сигнала направления цены.

        Логика:
        - direction='up' и нет позиции -> открыть лонг
        - direction='down' и есть позиция -> закрыть лонг (если close_on_reverse=True)
        """
        if not self.is_running:
            return

        async with self._position_lock:
            try:
                if direction == 'up' and not self.has_open_position:
                    await self._open_position(binance_price)
                elif direction == 'down' and self.has_open_position and self.settings.close_on_reverse:
                    await self._close_position("reverse")
            except Exception as e:
                logger.error(f"Direction handler error: user={self.user_id}, symbol={self.symbol}: {e}")

    async def _open_position(self, binance_price: float):
        """Открыть лонг позицию на MEXC"""
        try:
            # Проверяем дневные лимиты перед открытием
            today_count = await self.db.get_today_flip_count(self.user_id)
            if today_count >= self.settings.max_daily_flips:
                logger.info(f"Daily flip limit reached for user {self.user_id}: {today_count}/{self.settings.max_daily_flips}")
                return

            today_pnl = await self.db.get_today_flip_pnl(self.user_id)
            if today_pnl <= -self.settings.max_daily_loss_usd:
                logger.info(f"Daily loss limit reached for user {self.user_id}: ${today_pnl:.2f}")
                return

            # Рассчитываем количество
            quantity = self.settings.position_size_usd / binance_price

            # Округляем quantity (для MEXC обычно 3 знака)
            quantity = float(Decimal(str(quantity)).quantize(Decimal('0.001'), rounding=ROUND_DOWN))

            if quantity <= 0:
                logger.warning(f"Invalid quantity for {self.symbol}: {quantity}")
                return

            # Открываем позицию
            result = await self.mexc_api.open_long(self.symbol, quantity)

            if not result.get('success'):
                logger.error(f"Failed to open long: {result.get('error')}")
                return

            entry_price = result.get('price', binance_price)
            self.entry_price = entry_price
            self.entry_binance_price = binance_price
            self.has_open_position = True

            # Сохраняем время открытия
            now_utc = datetime.now(timezone.utc)
            self._opened_at = now_utc.isoformat()

            # Создаем запись в БД
            trade = FlipTrade(
                user_id=self.user_id,
                symbol=self.symbol,
                direction='long',
                entry_price=entry_price,
                leverage=self.settings.leverage,
                position_size_usd=self.settings.position_size_usd,
                quantity=quantity,
                status='open',
                binance_entry_price=binance_price,
                opened_at=self._opened_at,
                metadata={'test_mode': self.settings.test_mode}
            )

            self.current_trade_id = await self.db.add_flip_trade(trade)
            self.trades_count += 1

            logger.info(
                f"FLIP OPEN #{self.current_trade_id}: {self.symbol} "
                f"@{entry_price:.4f} (Binance: {binance_price:.4f}) "
                f"qty={quantity:.4f} lev={self.settings.leverage}x "
                f"[user={self.user_id}]"
            )

        except Exception as e:
            logger.error(f"Open position error: user={self.user_id}, symbol={self.symbol}: {e}")

    async def _close_position(self, reason: str):
        """Закрыть лонг позицию на MEXC"""
        if not self.has_open_position or not self.current_trade_id:
            return

        try:
            # Получаем текущую цену Binance
            binance_price = self.price_tracker.get_latest_price(self.symbol)

            # Рассчитываем количество для закрытия
            quantity = self.settings.position_size_usd / self.entry_price
            quantity = float(Decimal(str(quantity)).quantize(Decimal('0.001'), rounding=ROUND_DOWN))

            if quantity <= 0:
                quantity = 0.001  # Минимальное количество

            result = await self.mexc_api.close_long(self.symbol, quantity)

            if not result.get('success'):
                logger.error(f"Failed to close long #{self.current_trade_id}: {result.get('error')}")
                # Не сбрасываем has_open_position чтобы попробовать закрыть позже
                return

            exit_price = result.get('price', binance_price)

            # Рассчитываем PnL
            # Для лонга: PnL = (exit - entry) / entry * leverage * position_size
            if self.entry_price > 0:
                price_change_pct = (exit_price - self.entry_price) / self.entry_price * 100
            else:
                price_change_pct = 0

            pnl_usd = self.settings.position_size_usd * price_change_pct / 100 * self.settings.leverage

            # Длительность сделки в миллисекундах
            now = datetime.now(timezone.utc)
            if self._opened_at:
                try:
                    opened_dt = datetime.fromisoformat(self._opened_at.replace('Z', '+00:00'))
                    duration_ms = int((now - opened_dt).total_seconds() * 1000)
                except (ValueError, TypeError):
                    duration_ms = 0
            else:
                duration_ms = 0

            # Закрываем в БД
            await self.db.close_flip_trade(
                trade_id=self.current_trade_id,
                exit_price=exit_price,
                pnl_usd=pnl_usd,
                pnl_percent=price_change_pct * self.settings.leverage,
                close_reason=reason,
                binance_exit_price=binance_price,
                duration_ms=duration_ms
            )

            self.pnl_today += pnl_usd
            self.has_open_position = False

            emoji = "GREEN" if pnl_usd >= 0 else "RED"
            logger.info(
                f"FLIP CLOSE #{self.current_trade_id}: {self.symbol} "
                f"entry={self.entry_price:.4f} exit={exit_price:.4f} "
                f"PnL=${pnl_usd:.4f} ({price_change_pct:.4f}%) "
                f"dur={duration_ms}ms reason={reason} [{emoji}] "
                f"[user={self.user_id}]"
            )

            # Сбрасываем состояние
            self.current_trade_id = None
            self.entry_price = 0.0
            self.entry_binance_price = 0.0
            self._opened_at = None

        except Exception as e:
            logger.error(f"Close position error: user={self.user_id}, symbol={self.symbol}: {e}")

    async def close(self):
        """Закрыть сессию"""
        logger.info(f"Closing FlipSession: user={self.user_id}, symbol={self.symbol}")
        self.is_running = False
        if self.has_open_position:
            await self._close_position("manual")


class FlipTrader:
    """
    Основной движок MEXC Flip Trading.

    Для каждого пользователя и символа:
    1. Следит за ценами Binance
    2. Открывает лонг на MEXC при росте
    3. Закрывает при падении
    """

    def __init__(self):
        self.price_tracker: Optional[BinancePriceTracker] = None
        self.db = Database(settings.db_file)
        self.running = False
        self._tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()
        # Активные сессии: {(user_id, symbol): FlipSession}
        self.active_sessions: Dict[tuple, FlipSession] = {}
        self._sessions_lock = asyncio.Lock()
        # Per-user MEXC API instances: {user_id: MexcAPI}
        self.user_mexc_apis: Dict[int, MexcAPI] = {}
        self._apis_lock = asyncio.Lock()

    async def start(self):
        """Запуск Flip Trading сервиса"""
        self.running = True
        await self.db.initialize()
        logger.info("MEXC Flip Trader started")

        # Запускаем цикл управления сессиями
        self._tasks = [
            asyncio.create_task(self._session_manager_loop(), name="session_manager"),
        ]

        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self):
        """Остановка всех сессий и сервиса"""
        logger.info("Stopping MEXC Flip Trader...")
        self.running = False
        self._shutdown_event.set()

        # Закрываем все активные сессии
        closed_sessions = []
        async with self._sessions_lock:
            for key, session in list(self.active_sessions.items()):
                try:
                    await session.close()
                    closed_sessions.append(key)
                except Exception as e:
                    logger.error(f"Error closing session {key}: {e}")
            self.active_sessions.clear()

        if closed_sessions:
            logger.info(f"Closed {len(closed_sessions)} active sessions")

        # Останавливаем price tracker
        if self.price_tracker:
            try:
                await self.price_tracker.stop()
            except Exception as e:
                logger.error(f"Error stopping price tracker: {e}")
            self.price_tracker = None

        # Закрываем все per-user MEXC API сессии
        async with self._apis_lock:
            for uid, mexc_api in list(self.user_mexc_apis.items()):
                try:
                    await mexc_api.close()
                except Exception as e:
                    logger.error(f"Error closing MEXC API for user {uid}: {e}")
            self.user_mexc_apis.clear()

        # Отменяем задачи
        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._tasks.clear()

        logger.info("MEXC Flip Trader stopped")

    async def start_user_session(self, user_id: int, flip_settings: FlipSettings) -> dict:
        """
        Запуск торговой сессии для пользователя.
        Возвращает статус запуска.
        """
        try:
            # Проверяем дневные лимиты
            today_count = await self.db.get_today_flip_count(user_id)
            if today_count >= flip_settings.max_daily_flips:
                return {
                    'success': False,
                    'error': f'Daily flip limit reached: {today_count}/{flip_settings.max_daily_flips}'
                }

            today_pnl = await self.db.get_today_flip_pnl(user_id)
            if today_pnl <= -flip_settings.max_daily_loss_usd:
                return {
                    'success': False,
                    'error': f'Daily loss limit reached: ${today_pnl:.2f}'
                }

            symbols = flip_settings.selected_symbols
            if not symbols:
                return {'success': False, 'error': 'No symbols selected'}

            # Проверяем API ключи пользователя
            user_api_key = flip_settings.mexc_api_key
            user_api_secret = flip_settings.mexc_api_secret

            if not flip_settings.test_mode:
                if not user_api_key or not user_api_secret:
                    return {
                        'success': False,
                        'error': 'MEXC API keys not configured. Set your API keys in flip settings.'
                    }

            # Создаем per-user MexcAPI (или с пользовательскими ключами, или с глобальными для тестового режима)
            async with self._apis_lock:
                # Закрываем старый API instance для этого пользователя если есть
                old_api = self.user_mexc_apis.pop(user_id, None)
                if old_api:
                    try:
                        await old_api.close()
                    except Exception as e:
                        logger.error(f"Error closing old MEXC API for user {user_id}: {e}")

                mexc_api = MexcAPI(
                    api_key=user_api_key or None,
                    api_secret=user_api_secret or None
                )
                self.user_mexc_apis[user_id] = mexc_api

            # Собираем все символы от всех активных пользователей
            all_symbols = set(s.upper() for s in symbols)
            async with self._sessions_lock:
                for (uid, _), session in self.active_sessions.items():
                    if uid != user_id:
                        all_symbols.add(session.symbol)

            async with self._sessions_lock:
                # Останавливаем существующие сессии этого пользователя (перезапуск)
                keys_to_stop = [k for k in self.active_sessions.keys() if k[0] == user_id]
                for key in keys_to_stop:
                    session = self.active_sessions.pop(key)
                    try:
                        await session.close()
                    except Exception as e:
                        logger.error(f"Error stopping old session {key}: {e}")

                # Перезапускаем price tracker с актуальным набором символов
                if self.price_tracker:
                    # Добавляем новые символы к существующему трекеру
                    self.price_tracker.add_symbols(list(all_symbols))
                else:
                    self.price_tracker = BinancePriceTracker(
                        symbols=list(all_symbols),
                        window_size=settings.flip_price_history_window
                    )
                    # Запускаем price tracker
                    asyncio.create_task(self.price_tracker.start())
                    # Даем время на подключение
                    await asyncio.sleep(2)

                # Создаем сессию для каждого символа
                started_symbols = []
                for symbol in symbols:
                    key = (user_id, symbol.upper())
                    if key in self.active_sessions:
                        continue  # Уже активна

                    session = FlipSession(
                        user_id=user_id,
                        symbol=symbol,
                        flip_settings=flip_settings,
                        price_tracker=self.price_tracker,
                        mexc_api=mexc_api,
                        db=self.db
                    )
                    self.active_sessions[key] = session
                    asyncio.create_task(session.run())
                    started_symbols.append(symbol.upper())

                    # Подписываем сессию на сигналы
                    self.price_tracker.subscribe_to_signals(
                        self._on_price_signal, user_id, symbol
                    )

            logger.info(
                f"Flip session started for user {user_id}, symbols: {started_symbols}, "
                f"leverage={flip_settings.leverage}x, size=${flip_settings.position_size_usd}, "
                f"keys={'custom' if (user_api_key and user_api_secret) else 'global' if flip_settings.test_mode else 'none'}"
            )
            return {
                'success': True,
                'symbols': started_symbols,
                'leverage': flip_settings.leverage,
                'position_size': flip_settings.position_size_usd,
                'test_mode': flip_settings.test_mode
            }

        except Exception as e:
            logger.error(f"Start session error for user {user_id}: {e}")
            return {'success': False, 'error': str(e)}

    async def stop_user_session(self, user_id: int) -> dict:
        """Остановка всех сессий пользователя"""
        closed = []
        async with self._sessions_lock:
            keys_to_remove = [k for k in self.active_sessions.keys() if k[0] == user_id]
            for key in keys_to_remove:
                session = self.active_sessions.pop(key)
                try:
                    await session.close()
                    closed.append(key[1])  # symbol
                except Exception as e:
                    logger.error(f"Error closing session {key}: {e}")

        # Отписываем от сигналов
        if self.price_tracker:
            self.price_tracker.unsubscribe(user_id)

        # Закрываем per-user MEXC API
        async with self._apis_lock:
            mexc_api = self.user_mexc_apis.pop(user_id, None)
            if mexc_api:
                try:
                    await mexc_api.close()
                except Exception as e:
                    logger.error(f"Error closing MEXC API for user {user_id}: {e}")

        logger.info(f"Stopped sessions for user {user_id}: {closed}")
        return {'success': True, 'closed_symbols': closed}

    async def _on_price_signal(self, user_id: int, symbol: str, direction: str, price: float):
        """Обработка сигнала направления цены"""
        try:
            key = (user_id, symbol.upper())
            async with self._sessions_lock:
                session = self.active_sessions.get(key)
                if not session or not session.is_running:
                    return

            await session.on_price_direction(direction, price)

        except Exception as e:
            logger.error(f"Price signal error: user={user_id}, symbol={symbol}: {e}")

    async def _session_manager_loop(self):
        """Цикл управления сессиями - проверка лимитов и зомби-сессий"""
        while self.running:
            try:
                await asyncio.sleep(30)  # Каждые 30 секунд

                async with self._sessions_lock:
                    keys_to_remove = []

                    for key, session in list(self.active_sessions.items()):
                        user_id, symbol = key

                        # Проверяем дневные лимиты
                        try:
                            today_count = await self.db.get_today_flip_count(user_id)
                            flip_settings = await self.db.get_flip_settings(user_id)

                            if flip_settings:
                                if today_count >= flip_settings.max_daily_flips:
                                    logger.info(f"Daily limit reached for user {user_id}, stopping {symbol}")
                                    await session.close()
                                    keys_to_remove.append(key)
                                    continue

                                today_pnl = await self.db.get_today_flip_pnl(user_id)
                                if today_pnl <= -flip_settings.max_daily_loss_usd:
                                    logger.info(f"Daily loss limit for user {user_id}, stopping {symbol}")
                                    await session.close()
                                    keys_to_remove.append(key)
                                    continue
                        except Exception as e:
                            logger.error(f"Limit check error for user {user_id}: {e}")

                        # Удаляем остановленные сессии
                        if not session.is_running:
                            keys_to_remove.append(key)

                    # Удаляем помеченные сессии
                    for key in keys_to_remove:
                        self.active_sessions.pop(key, None)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Session manager error: {e}")

    async def get_session_status(self, user_id: int) -> dict:
        """Получить статус сессий пользователя"""
        active_symbols = []
        async with self._sessions_lock:
            for key, session in self.active_sessions.items():
                if key[0] == user_id:
                    active_symbols.append({
                        'symbol': key[1],
                        'has_position': session.has_open_position,
                        'trades_today': session.trades_count,
                        'pnl_today': session.pnl_today
                    })

        today_count = await self.db.get_today_flip_count(user_id)
        today_pnl = await self.db.get_today_flip_pnl(user_id)

        # Получаем статистику за сегодня
        today_str = datetime.now().strftime('%Y-%m-%d')
        stats = await self.db.get_flip_trade_stats(user_id, since=today_str)

        return {
            'active': len(active_symbols) > 0,
            'symbols': active_symbols,
            'today_count': today_count,
            'today_pnl': today_pnl,
            'today_stats': stats
        }

    async def get_user_stats(self, user_id: int, since: str = None) -> dict:
        """Получить детальную статистику flip trading пользователя"""
        stats = await self.db.get_flip_trade_stats(user_id, since=since)
        open_trades = await self.db.get_open_flip_trades(user_id)

        return {
            'stats': stats,
            'open_trades': len(open_trades),
            'open_trade_details': [
                {
                    'id': t.id,
                    'symbol': t.symbol,
                    'entry_price': t.entry_price,
                    'leverage': t.leverage,
                    'opened_at': t.opened_at
                } for t in open_trades
            ]
        }

    def is_user_active(self, user_id: int) -> bool:
        """Проверить активна ли сессия пользователя"""
        for uid, _ in self.active_sessions.keys():
            if uid == user_id:
                return True
        return False


# Глобальный экземпляр
flip_trader = FlipTrader()
