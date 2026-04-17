import asyncio
import aiohttp
import websockets
import json
import time
from typing import Dict, List, Callable, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
from collections import OrderedDict

from config import settings

logger = logging.getLogger(__name__)

class ArbitrageType(Enum):
    INTER_EXCHANGE_FUTURES = "inter_exchange_futures"
    BASIS_SPOT_FUTURES = "basis_spot_futures"
    CROSS_EXCHANGE_BASIS = "cross_exchange_basis"

@dataclass
class PriceData:
    symbol: str
    exchange: str
    market_type: str = "futures"
    last_price: float = 0.0
    mark_price: float = 0.0
    index_price: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    funding_rate: float = 0.0
    volume_24h: float = 0.0
    timestamp: float = 0.0

    @property
    def effective_price(self) -> float:
        return self.mark_price if self.mark_price > 0 else self.last_price

    @property
    def mark_last_diff(self) -> float:
        if self.last_price == 0:
            return 0
        return abs(self.mark_price - self.last_price) / self.last_price * 100

@dataclass
class SpreadAlert:
    symbol: str
    spread_percent: float
    buy_exchange: str
    sell_exchange: str
    buy_price: PriceData
    sell_price: PriceData
    volume_24h: float
    funding_diff: float
    timestamp: datetime
    alert_level: str
    arbitrage_type: ArbitrageType
    basis_info: Optional[Dict] = None
    hours_to_funding: Optional[float] = None

    @property
    def mark_price_spread(self) -> float:
        if self.buy_price.mark_price == 0 or self.sell_price.mark_price == 0:
            return 0.0
        return (self.sell_price.mark_price - self.buy_price.mark_price) / self.buy_price.mark_price * 100

    @property
    def is_basis(self) -> bool:
        return self.arbitrage_type in [ArbitrageType.BASIS_SPOT_FUTURES, ArbitrageType.CROSS_EXCHANGE_BASIS]

@dataclass
class CachedSpread:
    symbol: str
    spread_percent: float
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    volume_24h: float
    funding_diff: float
    timestamp: float
    arbitrage_type: ArbitrageType

    @property
    def is_fresh(self) -> bool:
        return time.time() - self.timestamp < settings.spread_ttl_seconds

class RateLimiter:
    def __init__(self, max_requests: int = 10, window_seconds: int = 1):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, key: str = "default"):
        async with self._lock:
            now = time.time()
            if key not in self.requests:
                self.requests[key] = []
            
            self.requests[key] = [t for t in self.requests[key] if now - t < self.window]
            
            if len(self.requests[key]) >= self.max_requests:
                sleep_time = self.window - (now - self.requests[key][0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
            
            self.requests[key].append(time.time())

class SpreadScanner:
    def __init__(self, min_spread=0.2, check_interval=5, basis_threshold=0.3):
        self.min_spread = min_spread
        self.check_interval = check_interval
        self.basis_threshold = basis_threshold

        self.prices: Dict[str, Dict[str, Dict[str, PriceData]]] = {}
        
        self.active_spreads: OrderedDict[str, CachedSpread] = OrderedDict()
        self.max_spread_cache = 1000
        
        self.subscribers: List[Callable] = []
        self.running = False
        self.rate_limiter = RateLimiter(max_requests=5, window_seconds=1)

        self.user_settings: Dict[int, float] = {}
        self.user_basis_settings: Dict[int, float] = {}
        self.default_min_spread = min_spread

        self.exchange_symbols: Dict[str, Set[str]] = {
            'binance': set(),
            'bybit': set(),
            'okx': set(),
            'whitebit': set(),
            'mexc': set()
        }

        self.fallback_symbols = [
            'BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'DOGE', 'ADA', 'TRX', 'AVAX',
            'LINK', 'LTC', 'BCH', 'DOT', 'UNI', 'TON', 'SUI', 'APT', 'FIL',
            'ETC', 'ALGO', 'NEAR', 'AAVE', 'ATOM', 'XTZ', 'VET', 'THETA',
            'ICP', 'SHIB', 'PEPE', 'WIF', 'BONK', 'FLOKI', 'JUP', 'PYTH'
        ]

        self.stats = {
            'connections': {
                'binance_futures': False, 'binance_spot': False,
                'bybit_futures': False, 'bybit_spot': False,
                'okx_futures': False, 'okx_spot': False,
                'whitebit_futures': False, 'whitebit_spot': False,
                'mexc_futures': False, 'mexc_spot': False
            },
            'last_status_log': 0,
            'spreads_found': 0,
            'basis_found': 0,
            'last_spread_log': {}
        }

        self.sent_alerts: OrderedDict[str, datetime] = OrderedDict()
        self.max_alert_history = 500
        
        self._shutdown_event = asyncio.Event()
        self._tasks = []
        
        # Graceful degradation
        self.min_exchanges_required = 2
        self._degraded_mode = False
        self._unavailable_exchanges = set()
        
        # Exponential backoff
        self._reconnect_attempts: Dict[str, int] = {}
        self._max_reconnect_delay = 60

    def set_user_threshold(self, user_id: int, threshold: float, for_basis: bool = False):
        if for_basis:
            self.user_basis_settings[user_id] = threshold
        else:
            self.user_settings[user_id] = threshold

    def get_user_threshold(self, user_id: int, for_basis: bool = False) -> float:
        if for_basis:
            return self.user_basis_settings.get(user_id, self.basis_threshold)
        return self.user_settings.get(user_id, self.default_min_spread)

    def subscribe(self, callback: Callable, user_id: int = None):
        if user_id:
            self.subscribers.append((callback, user_id))
            if user_id not in self.user_settings:
                self.user_settings[user_id] = self.default_min_spread
            if user_id not in self.user_basis_settings:
                self.user_basis_settings[user_id] = self.basis_threshold
        else:
            self.subscribers.append(callback)

    def unsubscribe(self, user_id: int):
        self.subscribers = [s for s in self.subscribers
                          if not (isinstance(s, tuple) and s[1] == user_id)]
        self.user_settings.pop(user_id, None)
        self.user_basis_settings.pop(user_id, None)

    def get_active_spreads(self, min_spread: float = 0.1) -> Dict[str, CachedSpread]:
        now = time.time()
        fresh = {}
        
        keys_to_remove = []
        for key, spread in self.active_spreads.items():
            if spread.is_fresh and spread.spread_percent >= min_spread:
                fresh[key] = spread
            elif not spread.is_fresh:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            self.active_spreads.pop(key, None)
            
        return fresh

    def get_spread_by_key(self, key: str) -> Optional[CachedSpread]:
        spread = self.active_spreads.get(key)
        if spread and spread.is_fresh:
            return spread
        return None

    def _cleanup_old_alerts(self):
        while len(self.sent_alerts) > self.max_alert_history:
            self.sent_alerts.popitem(last=False)

    def _get_reconnect_delay(self, key: str) -> float:
        if key not in self._reconnect_attempts:
            self._reconnect_attempts[key] = 0
        
        self._reconnect_attempts[key] += 1
        attempt = self._reconnect_attempts[key]
        
        delay = min(5 * (2 ** (attempt - 1)), self._max_reconnect_delay)
        
        if delay >= self._max_reconnect_delay:
            self._reconnect_attempts[key] = 0
            
        return delay
    
    def _reset_reconnect(self, key: str):
        if key in self._reconnect_attempts:
            if self._reconnect_attempts[key] > 0:
                logger.info(f"Connection {key} restored after {self._reconnect_attempts[key]} attempts")
            del self._reconnect_attempts[key]

    async def _check_exchange_health(self):
        """Проверка здоровья бирж и переключение в degraded mode"""
        from services.exchange_status import status_checker
        
        available = []
        unavailable = []
        
        for ex in ['binance', 'bybit', 'okx', 'whitebit', 'mexc']:
            if status_checker.is_exchange_available(ex):
                available.append(ex)
            else:
                unavailable.append(ex)
        
        if len(available) < self.min_exchanges_required:
            if not self._degraded_mode:
                self._degraded_mode = True
                logger.error(f"🚨 DEGRADED MODE: Only {len(available)} exchanges available")
                
                try:
                    from services.notification import alert_manager
                    if alert_manager:
                        await alert_manager.critical(
                            f"Degraded mode: Only {len(available)} exchanges available",
                            source="scanner"
                        )
                except:
                    pass
        else:
            if self._degraded_mode:
                self._degraded_mode = False
                logger.info(f"✅ Normal mode restored: {len(available)} exchanges")
        
        self._unavailable_exchanges = set(unavailable)
        return available

    async def notify_subscribers(self, alert: SpreadAlert):
        if alert.is_basis:
            self.stats['basis_found'] += 1
        else:
            self.stats['spreads_found'] += 1

        arb_type = "БАЗИС" if alert.is_basis else "МЕЖБИРЖЕВОЙ"
        if alert.spread_percent >= 0.2:
            logger.info(f"🚨 {arb_type}: {alert.symbol} | {alert.buy_exchange} → {alert.sell_exchange} | {alert.spread_percent:.2f}%")

        key = f"{alert.symbol}:{alert.buy_exchange}:{alert.sell_exchange}"
        
        if len(self.active_spreads) >= self.max_spread_cache:
            self.active_spreads.popitem(last=False)
            
        self.active_spreads[key] = CachedSpread(
            symbol=alert.symbol,
            spread_percent=alert.spread_percent,
            buy_exchange=alert.buy_exchange,
            sell_exchange=alert.sell_exchange,
            buy_price=alert.buy_price.last_price,
            sell_price=alert.sell_price.last_price,
            volume_24h=alert.volume_24h,
            funding_diff=alert.funding_diff,
            timestamp=time.time(),
            arbitrage_type=alert.arbitrage_type
        )
        
        self.active_spreads.move_to_end(key)

        if settings.auto_trading_default and alert.spread_percent >= settings.min_spread_auto:
            await self._trigger_auto_trade(alert)

        for callback_info in self.subscribers:
            try:
                if isinstance(callback_info, tuple):
                    callback, user_id = callback_info
                    user_threshold = self.get_user_threshold(user_id, for_basis=alert.is_basis)

                    if alert.spread_percent >= user_threshold:
                        await callback(alert, user_id)
                else:
                    default_thresh = self.basis_threshold if alert.is_basis else self.default_min_spread
                    if alert.spread_percent >= default_thresh:
                        await callback_info(alert)
            except Exception as e:
                logger.error(f"Error notifying subscriber: {e}")

    async def _trigger_auto_trade(self, alert: SpreadAlert):
        """Триггер авто-трейдинга для всех пользователей с включенным авто-трейдингом"""
        try:
            from services.trading_engine import trading_engine
            from database.models import Database

            max_hours = settings.max_funding_hours
            if max_hours > 0 and hasattr(alert, 'hours_to_funding') and alert.hours_to_funding is not None:
                if alert.hours_to_funding > max_hours:
                    return

            db = Database(settings.db_file)
            await db.initialize()
            
            try:
                # Получаем всех пользователей с включенным авто-трейдингом
                all_users = await db.get_all_users()
                
                for user in all_users:
                    # Проверяем включен ли авто-трейдинг для пользователя
                    if not user.alert_settings.get('auto_trading', False):
                        continue
                    
                    # Проверяем есть ли API ключи для обеих бирж
                    has_buy = alert.buy_exchange in user.api_keys and user.api_keys[alert.buy_exchange].get('api_key')
                    has_sell = alert.sell_exchange in user.api_keys and user.api_keys[alert.sell_exchange].get('api_key')
                    
                    if not has_buy or not has_sell:
                        continue
                    
                    # Проверяем лимит позиций
                    open_trades = await db.get_open_trades(user.user_id)
                    max_positions = user.risk_settings.get('max_open_positions', 5)
                    
                    if len(open_trades) >= max_positions:
                        continue
                    
                    # Проверяем минимальный спред для авто-трейдинга
                    min_spread = user.alert_settings.get('min_spread_auto', settings.min_spread_auto)
                    if alert.spread_percent < min_spread:
                        continue
                    
                    key = f"{alert.symbol}:{alert.buy_exchange}:{alert.sell_exchange}"
                    result = await trading_engine.validate_and_open(
                        user, key, self.prices, auto=True, 
                        available_exchanges={'buy': has_buy, 'sell': has_sell}
                    )
                    
                    if result.success:
                        logger.info(f"Auto-trade opened for user {user.user_id}: {alert.symbol} #{result.trade_id}")
                        
                        # Отправляем уведомление пользователю
                        if hasattr(self, '_bot') and self._bot:
                            try:
                                await self._bot.send_message(
                                    chat_id=user.user_id,
                                    text=f"🤖 Авто-сделка открыта: {alert.symbol}\n"
                                         f"Спред: {alert.spread_percent:.2f}%\n"
                                         f"ID: #{result.trade_id}"
                                )
                            except:
                                pass
            finally:
                await db.close()
        except Exception as e:
            logger.error(f"Auto trade error: {e}")

    async def start(self):
        self.running = True
        logger.info("Starting spread scanner...")

        await self._fetch_all_symbols()

        tasks = [
            self._binance_ws_futures(),
            self._binance_ws_spot(),
            self._bybit_ws_futures(),
            self._bybit_ws_spot(),
            self._okx_ws_futures(),
            self._okx_ws_spot(),
            self._whitebit_ws_futures(),
            self._mexc_ws_futures(),
            self._analyze_loop(),
            self._cleanup_loop()
        ]
        
        self._tasks = [asyncio.create_task(t) for t in tasks]
        
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
    
    async def stop(self):
        logger.info("Stopping spread scanner...")
        self.running = False
        self._shutdown_event.set()
        
        # Отменяем все задачи
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения с таймаутом
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                logger.warning("Scanner tasks stop timeout, some tasks may still be running")
        
        logger.info("Spread scanner stopped")
    
    async def _cleanup_loop(self):
        while self.running:
            try:
                await asyncio.sleep(300)
                
                now = datetime.now()
                old_keys = [
                    k for k, v in self.sent_alerts.items() 
                    if (now - v).total_seconds() > 3600
                ]
                for k in old_keys:
                    del self.sent_alerts[k]
                
                if old_keys:
                    logger.info(f"Cleaned up {len(old_keys)} old alerts")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _fetch_all_symbols(self):
        headers = {'User-Agent': 'Mozilla/5.0'}

        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [
                self._fetch_binance_symbols(session),
                self._fetch_bybit_symbols(session),
                self._fetch_okx_symbols(session),
                self._fetch_whitebit_symbols(session),
                self._fetch_mexc_symbols(session),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, (ex, error) in enumerate(zip(['binance', 'bybit', 'okx', 'whitebit', 'mexc'], results)):
                if isinstance(error, Exception):
                    logger.error(f"{ex.upper()} fetch error: {error}")

            for ex, symbols in self.exchange_symbols.items():
                if len(symbols) == 0:
                    logger.warning(f"⚠️ {ex.upper()}: fallback to default symbols")
                    self.exchange_symbols[ex] = set(self.fallback_symbols)
                else:
                    logger.info(f"📋 {ex.upper()}: {len(symbols)} pairs")

    async def _binance_ws_futures(self):
        uri = "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr"
        conn_key = "binance_futures"
        logger.info(f"🔌 {conn_key} WS...")

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)
                    
                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                                stream = data.get('stream', '')
                                payload = data.get('data', {})

                                if 'ticker' in stream:
                                    await self._process_binance_ticker(payload, is_futures=True)
                                elif 'markPrice' in stream:
                                    await self._process_binance_mark(payload)
                            except json.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.error(f"Error processing {conn_key} msg: {e}")
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")
                        
            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS error: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _binance_ws_spot(self):
        uri = "wss://stream.binance.com:9443/ws/!ticker@arr"
        conn_key = "binance_spot"
        logger.info(f"🔌 {conn_key} WS...")

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)
                    
                    try:
                        async for msg in ws:
                            if not self.running:
                                break
                            try:
                                data = json.loads(msg)
                                await self._process_binance_ticker(data, is_futures=False)
                            except json.JSONDecodeError:
                                continue
                            except Exception as e:
                                logger.error(f"Error processing {conn_key} msg: {e}")
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")
                        
            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS error: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _process_binance_ticker(self, data: list, is_futures: bool):
        if not isinstance(data, list):
            return

        market_type = "futures" if is_futures else "spot"
        conn_key = f"binance_{market_type}"

        if not self.stats['connections'].get(conn_key) and len(data) > 0:
            self.stats['connections'][conn_key] = True

        for ticker in data:
            try:
                symbol = ticker['s'].replace('USDT', '')
                if symbol not in self.prices:
                    self.prices[symbol] = {}
                if 'binance' not in self.prices[symbol]:
                    self.prices[symbol]['binance'] = {}

                if market_type not in self.prices[symbol]['binance']:
                    self.prices[symbol]['binance'][market_type] = PriceData(
                        symbol=symbol,
                        exchange='binance',
                        market_type=market_type
                    )

                pd = self.prices[symbol]['binance'][market_type]
                pd.last_price = float(ticker['c'])
                pd.bid = float(ticker.get('b', ticker['c']))
                pd.ask = float(ticker.get('a', ticker['c']))
                pd.volume_24h = float(ticker.get('q', 0))
                pd.timestamp = time.time()
            except (KeyError, ValueError):
                continue

    async def _process_binance_mark(self, data: list):
        if not isinstance(data, list):
            return

        for item in data:
            try:
                symbol = item['s'].replace('USDT', '')
                if (symbol in self.prices and
                    'binance' in self.prices[symbol] and
                    'futures' in self.prices[symbol]['binance']):

                    pd = self.prices[symbol]['binance']['futures']
                    pd.mark_price = float(item['p'])
                    pd.index_price = float(item['i'])
                    pd.funding_rate = float(item['r'])
            except (KeyError, ValueError):
                continue

    async def _fetch_binance_symbols(self, session: aiohttp.ClientSession):
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    raise Exception(f"Status {resp.status}")
                data = await resp.json()
                for s in data.get('symbols', []):
                    if s.get('status') == 'TRADING' and s.get('quoteAsset') == 'USDT':
                        self.exchange_symbols['binance'].add(s['baseAsset'])
        except Exception as e:
            raise e

    async def _bybit_ws_futures(self):
        uri = "wss://stream.bybit.com/v5/public/linear"
        conn_key = "bybit_futures"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = [s for s in list(self.exchange_symbols['bybit'])
                       if s not in {'BONK', 'FLOKI', 'PEPE', 'SHIB', 'WIF'}][:100]

        if not symbols_list:
            symbols_list = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'DOGE', 'ADA', 'TRX', 'AVAX', 'LINK']

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    await ws.send(json.dumps({"op": "ping"}))
                    await asyncio.sleep(0.5)

                    batch_size = 10
                    for i in range(0, len(symbols_list), batch_size):
                        if not self.running:
                            break
                        batch = symbols_list[i:i+batch_size]
                        args = [f"tickers.{s}USDT" for s in batch]

                        await ws.send(json.dumps({
                            "op": "subscribe",
                            "args": args,
                            "req_id": f"batch_{i//batch_size}"
                        }))
                        await asyncio.sleep(1)

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            if data.get('op') == 'pong':
                                continue

                            topic = data.get('topic', '')
                            if topic.startswith('tickers.'):
                                await self._process_bybit_ticker(data.get('data', {}), is_futures=True)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _bybit_ws_spot(self):
        uri = "wss://stream.bybit.com/v5/public/spot"
        conn_key = "bybit_spot"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = list(self.exchange_symbols['bybit'])[:50]

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    batch_size = 10
                    for i in range(0, len(symbols_list), batch_size):
                        if not self.running:
                            break
                        batch = symbols_list[i:i+batch_size]
                        args = [f"tickers.{s}USDT" for s in batch]

                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                        await asyncio.sleep(0.5)

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            topic = data.get('topic', '')
                            if topic.startswith('tickers.'):
                                await self._process_bybit_ticker(data.get('data', {}), is_futures=False)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _process_bybit_ticker(self, data: dict, is_futures: bool):
        market_type = "futures" if is_futures else "spot"

        if not data or not isinstance(data, dict):
            return

        tickers = data if isinstance(data, list) else [data]

        for ticker in tickers:
            if not isinstance(ticker, dict):
                continue

            try:
                symbol = ticker.get('symbol', '').replace('USDT', '')
                if not symbol:
                    continue

                if symbol not in self.prices:
                    self.prices[symbol] = {}
                if 'bybit' not in self.prices[symbol]:
                    self.prices[symbol]['bybit'] = {}

                volume = float(ticker.get('turnover24h', ticker.get('volume24h', 0)))
                if volume == 0:
                    volume = 10000000

                self.prices[symbol]['bybit'][market_type] = PriceData(
                    symbol=symbol,
                    exchange='bybit',
                    market_type=market_type,
                    last_price=float(ticker.get('lastPrice', ticker.get('lp', 0))),
                    mark_price=float(ticker.get('markPrice', ticker.get('mp', 0))) if is_futures else 0,
                    index_price=float(ticker.get('indexPrice', ticker.get('ip', 0))) if is_futures else 0,
                    bid=float(ticker.get('bid1Price', ticker.get('bid', 0))),
                    ask=float(ticker.get('ask1Price', ticker.get('ask', 0))),
                    funding_rate=float(ticker.get('fundingRate', ticker.get('fr', 0))) if is_futures else 0,
                    volume_24h=volume,
                    timestamp=time.time()
                )
            except (KeyError, ValueError):
                continue

    async def _fetch_bybit_symbols(self, session: aiohttp.ClientSession):
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    raise Exception(f"Status {resp.status}")
                data = await resp.json()
                for item in data.get('result', {}).get('list', []):
                    if item.get('status') == 'Trading' and item.get('quoteCoin') == 'USDT':
                        self.exchange_symbols['bybit'].add(item['baseCoin'])
        except Exception as e:
            raise e

    async def _okx_ws_futures(self):
        uri = "wss://ws.okx.com:8443/ws/v5/public"
        conn_key = "okx_futures"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = list(self.exchange_symbols['okx'])

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    batch_size = 20
                    for i in range(0, len(symbols_list), batch_size):
                        if not self.running:
                            break
                        batch = symbols_list[i:i+batch_size]
                        args = []
                        for s in batch:
                            inst_id = f"{s}-USDT-SWAP"
                            args.append({"channel": "tickers", "instId": inst_id})
                            args.append({"channel": "mark-price", "instId": inst_id})

                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                        await asyncio.sleep(0.5)

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            if data.get('event') == 'subscribe':
                                continue

                            if 'data' in data:
                                await self._process_okx_data(data, is_futures=True)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _okx_ws_spot(self):
        uri = "wss://ws.okx.com:8443/ws/v5/public"
        conn_key = "okx_spot"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = list(self.exchange_symbols['okx'])[:100]

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    batch_size = 20
                    for i in range(0, len(symbols_list), batch_size):
                        if not self.running:
                            break
                        batch = symbols_list[i:i+batch_size]
                        args = [{"channel": "tickers", "instId": f"{s}-USDT"} for s in batch]

                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                        await asyncio.sleep(0.5)

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            if data.get('event') == 'subscribe':
                                continue
                            if 'data' in data:
                                await self._process_okx_data(data, is_futures=False)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _process_okx_data(self, data: dict, is_futures: bool):
        market_type = "futures" if is_futures else "spot"

        arg = data.get('arg', {})
        channel = arg.get('channel', '')
        inst_id = arg.get('instId', '')

        for item in data.get('data', []):
            try:
                if is_futures:
                    if not inst_id.endswith('-USDT-SWAP'):
                        continue
                    symbol = inst_id.replace('-USDT-SWAP', '')
                else:
                    if not inst_id.endswith('-USDT'):
                        continue
                    symbol = inst_id.replace('-USDT', '')

                if symbol not in self.prices:
                    self.prices[symbol] = {}
                if 'okx' not in self.prices[symbol]:
                    self.prices[symbol]['okx'] = {}

                if market_type not in self.prices[symbol]['okx']:
                    self.prices[symbol]['okx'][market_type] = PriceData(
                        symbol=symbol,
                        exchange='okx',
                        market_type=market_type
                    )

                pd = self.prices[symbol]['okx'][market_type]

                if channel == 'tickers':
                    pd.last_price = float(item.get('last', 0))
                    pd.bid = float(item.get('bidPx', 0))
                    pd.ask = float(item.get('askPx', 0))
                    vol_base = float(item.get('vol24h', 0))
                    pd.volume_24h = vol_base * pd.last_price if pd.last_price else 10000000
                    pd.timestamp = time.time()
                elif channel == 'mark-price' and is_futures:
                    pd.mark_price = float(item.get('markPx', 0))
                    pd.index_price = float(item.get('idxPx', 0))
                    pd.funding_rate = float(item.get('fundingRate', 0))
            except (KeyError, ValueError):
                continue

    async def _fetch_okx_symbols(self, session: aiohttp.ClientSession):
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            async with session.get(url, timeout=15) as resp:
                data = await resp.json()
                for item in data.get('data', []):
                    if item.get('state') == 'live' and item.get('settleCcy') == 'USDT':
                        self.exchange_symbols['okx'].add(item['instId'].split('-')[0])
        except Exception as e:
            raise e

    async def _whitebit_ws_futures(self):
        uri = "wss://api.whitebit.com/ws"
        conn_key = "whitebit_futures"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = list(self.exchange_symbols['whitebit'])[:50]

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=None) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    for i, symbol in enumerate(symbols_list[:25]):
                        if not self.running:
                            break
                        msg = {
                            "id": int(time.time() * 1000) + i,
                            "method": "lastprice_subscribe",
                            "params": [f"{symbol}_USDT"],
                            "jsonrpc": "2.0"
                        }
                        await ws.send(json.dumps(msg))
                        await asyncio.sleep(0.2)

                    ping_task = asyncio.create_task(self._whitebit_ping(ws))

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            if data.get('result') == 'pong':
                                continue
                            if data.get('error'):
                                continue

                            if data.get('method') == 'lastprice_update':
                                await self._process_whitebit_price(data, as_futures=True)
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _whitebit_ping(self, ws):
        while self.running:
            try:
                await asyncio.sleep(30)
                await ws.send(json.dumps({
                    "id": int(time.time() * 1000),
                    "method": "ping",
                    "params": [],
                    "jsonrpc": "2.0"
                }))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WhiteBIT ping: {e}")
                break

    async def _process_whitebit_price(self, data: dict, as_futures: bool = True):
        params = data.get('params', [])
        if len(params) < 2:
            return

        try:
            market = params[0]
            price = float(params[1])
            symbol = market.replace('_USDT', '').replace('_', '')

            market_type = "futures" if as_futures else "spot"

            if symbol not in self.prices:
                self.prices[symbol] = {}
            if 'whitebit' not in self.prices[symbol]:
                self.prices[symbol]['whitebit'] = {}

            self.prices[symbol]['whitebit'][market_type] = PriceData(
                symbol=symbol,
                exchange='whitebit',
                market_type=market_type,
                last_price=price,
                mark_price=price,
                bid=price * 0.9995,
                ask=price * 1.0005,
                volume_24h=10000000,
                timestamp=time.time()
            )
        except (ValueError, IndexError):
            return

    async def _fetch_whitebit_symbols(self, session: aiohttp.ClientSession):
        try:
            url = "https://whitebit.com/api/v4/public/markets"
            async with session.get(url, timeout=15) as resp:
                data = await resp.json()
                if isinstance(data, dict):
                    for market in data.keys():
                        if isinstance(market, str) and market.endswith('_USDT'):
                            self.exchange_symbols['whitebit'].add(market.replace('_USDT', '').replace('_', ''))
        except Exception as e:
            raise e

    async def _mexc_ws_futures(self):
        uri = "wss://contract.mexc.com/edge"
        conn_key = "mexc_futures"
        logger.info(f"🔌 {conn_key} WS...")

        symbols_list = list(self.exchange_symbols['mexc'])[:20]

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info(f"✅ {conn_key}: connected")
                    self.stats['connections'][conn_key] = True
                    self._reset_reconnect(conn_key)

                    await ws.send(json.dumps({"method": "ping"}))
                    await asyncio.sleep(1)

                    for i, symbol in enumerate(symbols_list):
                        if not self.running:
                            break
                        msg = {
                            "method": "sub.ticker",
                            "param": {"symbol": f"{symbol}_USDT"}
                        }
                        await ws.send(json.dumps(msg))
                        if (i + 1) % 5 == 0:
                            await asyncio.sleep(1)

                    try:
                        async for msg in ws:
                            if not self.running:
                                break

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            if isinstance(data, list):
                                for item in data:
                                    await self._process_mexc_ticker(item)
                            elif isinstance(data, dict):
                                channel = data.get('channel', '')
                                if channel.startswith('ticker:'):
                                    await self._process_mexc_ticker(data)
                                elif 'data' in data:
                                    await self._process_mexc_ticker(data.get('data', {}))
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"{conn_key} WS closed")

            except Exception as e:
                delay = self._get_reconnect_delay(conn_key)
                logger.error(f"{conn_key} WS: {e}. Reconnecting in {delay}s...")
                self.stats['connections'][conn_key] = False
                await asyncio.sleep(delay)

    async def _process_mexc_ticker(self, data: dict):
        if not data:
            return

        ticker_data = data.get('data', data) if isinstance(data, dict) else data
        if not isinstance(ticker_data, dict):
            return

        try:
            symbol = ticker_data.get('symbol', '')
            if not symbol:
                channel = data.get('channel', '')
                if channel.startswith('ticker:'):
                    symbol = channel.replace('ticker:', '')

            symbol = symbol.replace('_USDT', '').replace('-USDT', '')
            if not symbol:
                return

            if symbol not in self.prices:
                self.prices[symbol] = {}
            if 'mexc' not in self.prices[symbol]:
                self.prices[symbol]['mexc'] = {}

            volume_fields = ['turnover', 'vol', 'volume24h', 'quoteVolume', 'amount']
            volume = 0
            for field in volume_fields:
                if field in ticker_data and ticker_data[field]:
                    volume = float(ticker_data[field])
                    break
            if volume == 0:
                volume = 10000000

            self.prices[symbol]['mexc']['futures'] = PriceData(
                symbol=symbol,
                exchange='mexc',
                market_type='futures',
                last_price=float(ticker_data.get('lastPrice', ticker_data.get('last', 0))),
                mark_price=float(ticker_data.get('fairPrice', ticker_data.get('markPrice', 0))),
                index_price=float(ticker_data.get('indexPrice', 0)),
                bid=float(ticker_data.get('bid1Price', ticker_data.get('bid', 0))),
                ask=float(ticker_data.get('ask1Price', ticker_data.get('ask', 0))),
                funding_rate=float(ticker_data.get('fundingRate', 0)),
                volume_24h=volume,
                timestamp=time.time()
            )
        except (KeyError, ValueError):
            return

    async def _fetch_mexc_symbols(self, session: aiohttp.ClientSession):
        try:
            url = "https://contract.mexc.com/api/v1/contract/detail"
            async with session.get(url, timeout=15) as resp:
                data = await resp.json()
                for item in data.get('data', []):
                    if item.get('quoteCoin') == 'USDT':
                        self.exchange_symbols['mexc'].add(item['baseCoin'])
        except Exception as e:
            raise e

    async def _analyze_loop(self):
        logger.info("🔍 Scanner started!")

        while self.running:
            try:
                await self.rate_limiter.acquire("analyze")
                
                available_exchanges = await self._check_exchange_health()
                
                if len(available_exchanges) < self.min_exchanges_required:
                    logger.warning(f"Not enough exchanges ({len(available_exchanges)}), skipping...")
                    await asyncio.sleep(self.check_interval * 2)
                    continue
                
                prices_copy = {}
                for symbol, exchanges in self.prices.items():
                    filtered = {ex: data for ex, data in exchanges.items() 
                               if ex not in self._unavailable_exchanges}
                    if len(filtered) >= 2:
                        prices_copy[symbol] = filtered
                
                if len(prices_copy) == 0:
                    logger.warning("No price data available")
                    await asyncio.sleep(self.check_interval)
                    continue
                
                await self._check_inter_exchange_futures(prices_copy)
                await self._check_basis_arbitrage(prices_copy)
                await self._check_cross_exchange_basis(prices_copy)

                current_time = time.time()
                if current_time - self.stats['last_status_log'] > 60:
                    active = sum(1 for v in self.stats['connections'].values() if v)
                    mode = "DEGRADED" if self._degraded_mode else "NORMAL"
                    logger.info(f"📊 Mode: {mode} | Streams: {active}/10 | Symbols: {len(prices_copy)}")
                    self.stats['last_status_log'] = current_time

                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                logger.info("Analyze loop cancelled")
                break
            except Exception as e:
                logger.error(f"Analyze loop error: {e}")
                await asyncio.sleep(5)

    async def _check_inter_exchange_futures(self, prices_copy=None):
        prices = prices_copy if prices_copy else self.prices

        for symbol, exchanges in list(prices.items()):
            futures_prices = []
            for ex, markets in list(exchanges.items()):
                if 'futures' in markets:
                    pd = markets['futures']
                    if time.time() - pd.timestamp < 30 and pd.last_price > 0:
                        futures_prices.append((ex, pd))

            if len(futures_prices) < 2:
                continue

            for i in range(len(futures_prices)):
                for j in range(i + 1, len(futures_prices)):
                    ex1, pd1 = futures_prices[i]
                    ex2, pd2 = futures_prices[j]

                    await self._evaluate_funding_arbitrage(symbol, ex1, pd1, ex2, pd2)

                    await self._evaluate_arbitrage(symbol, ex1, pd1, ex2, pd2, ArbitrageType.INTER_EXCHANGE_FUTURES)
                    await self._evaluate_arbitrage(symbol, ex2, pd2, ex1, pd1, ArbitrageType.INTER_EXCHANGE_FUTURES)

    async def _check_basis_arbitrage(self, prices_copy=None):
        prices = prices_copy if prices_copy else self.prices

        for symbol, exchanges in list(prices.items()):
            for ex, markets in exchanges.items():
                if 'spot' not in markets or 'futures' not in markets:
                    continue

                spot_pd = markets['spot']
                fut_pd = markets['futures']

                now = time.time()
                if (now - spot_pd.timestamp > 30 or now - fut_pd.timestamp > 30):
                    continue
                if spot_pd.last_price == 0 or fut_pd.last_price == 0:
                    continue

                basis = (fut_pd.effective_price - spot_pd.last_price) / spot_pd.last_price * 100

                if abs(basis) < self.basis_threshold:
                    continue

                spot_vol = spot_pd.volume_24h if spot_pd.volume_24h > 0 else 10000000
                fut_vol = fut_pd.volume_24h if fut_pd.volume_24h > 0 else 10000000

                if spot_vol < 100000 or fut_vol < 100000:
                    continue

                now_dt = datetime.now()
                alert_key = f"basis:{symbol}:{ex}"
                
                if alert_key in self.sent_alerts:
                    if (now_dt - self.sent_alerts[alert_key]).seconds < 300:
                        continue
                    self.sent_alerts.move_to_end(alert_key)
                
                self._cleanup_old_alerts()

                if basis > 0:
                    buy_pd, sell_pd = spot_pd, fut_pd
                    buy_market, sell_market = 'spot', 'futures'
                else:
                    buy_pd, sell_pd = fut_pd, spot_pd
                    buy_market, sell_market = 'futures', 'spot'

                level = 'high' if abs(basis) >= 1.0 else ('medium' if abs(basis) >= 0.5 else 'low')

                alert = SpreadAlert(
                    symbol=symbol,
                    spread_percent=abs(basis),
                    buy_exchange=f"{ex}:{buy_market}",
                    sell_exchange=f"{ex}:{sell_market}",
                    buy_price=buy_pd,
                    sell_price=sell_pd,
                    volume_24h=min(spot_vol, fut_vol),
                    funding_diff=fut_pd.funding_rate,
                    timestamp=now_dt,
                    alert_level=level,
                    arbitrage_type=ArbitrageType.BASIS_SPOT_FUTURES,
                    basis_info={
                        'raw_basis': basis,
                        'funding_annual': fut_pd.funding_rate * 3 * 365,
                        'recommended_action': 'long_spot_short_futures' if basis > 0 else 'short_spot_long_futures'
                    }
                )

                self.sent_alerts[alert_key] = now_dt
                await self.notify_subscribers(alert)

    async def _check_cross_exchange_basis(self, prices_copy=None):
        prices = prices_copy if prices_copy else self.prices

        for symbol, exchanges in list(prices.items()):
            spot_prices = []
            futures_prices = []

            for ex, markets in exchanges.items():
                if 'spot' in markets:
                    pd = markets['spot']
                    if time.time() - pd.timestamp < 30 and pd.last_price > 0:
                        spot_prices.append((ex, pd))

                if 'futures' in markets:
                    pd = markets['futures']
                    if time.time() - pd.timestamp < 30 and pd.last_price > 0:
                        futures_prices.append((ex, pd))

            for spot_ex, spot_pd in spot_prices:
                for fut_ex, fut_pd in futures_prices:
                    if spot_ex == fut_ex:
                        continue

                    spread = (fut_pd.effective_price - spot_pd.last_price) / spot_pd.last_price * 100

                    if abs(spread) < self.basis_threshold * 1.5:
                        continue

                    spot_vol = spot_pd.volume_24h if spot_pd.volume_24h > 0 else 10000000
                    fut_vol = fut_pd.volume_24h if fut_pd.volume_24h > 0 else 10000000

                    if spot_vol < 100000 or fut_vol < 100000:
                        continue

                    now = datetime.now()
                    alert_key = f"cross:{symbol}:{spot_ex}:spot:{fut_ex}:futures"
                    
                    if alert_key in self.sent_alerts:
                        if (now - self.sent_alerts[alert_key]).seconds < 300:
                            continue
                        self.sent_alerts.move_to_end(alert_key)
                    
                    self._cleanup_old_alerts()

                    if spread > 0:
                        buy_pd, sell_pd = spot_pd, fut_pd
                        buy_ex_str = f"{spot_ex}:spot"
                        sell_ex_str = f"{fut_ex}:futures"
                    else:
                        buy_pd, sell_pd = fut_pd, spot_pd
                        buy_ex_str = f"{fut_ex}:futures"
                        sell_ex_str = f"{spot_ex}:spot"

                    level = 'high' if abs(spread) >= 1.5 else ('medium' if abs(spread) >= 0.8 else 'low')

                    alert = SpreadAlert(
                        symbol=symbol,
                        spread_percent=abs(spread),
                        buy_exchange=buy_ex_str,
                        sell_exchange=sell_ex_str,
                        buy_price=buy_pd,
                        sell_price=sell_pd,
                        volume_24h=min(spot_vol, fut_vol),
                        funding_diff=fut_pd.funding_rate,
                        timestamp=now,
                        alert_level=level,
                        arbitrage_type=ArbitrageType.CROSS_EXCHANGE_BASIS,
                        basis_info={
                            'raw_spread': spread,
                            'spot_exchange': spot_ex,
                            'futures_exchange': fut_ex,
                            'transfer_risk': 'HIGH'
                        }
                    )

                    self.sent_alerts[alert_key] = now
                    await self.notify_subscribers(alert)

    async def _evaluate_funding_arbitrage(self, symbol: str, ex1: str, pd1: PriceData, ex2: str, pd2: PriceData):
        if pd1.funding_rate == 0 and pd2.funding_rate == 0:
            return

        funding_diff = abs(pd1.funding_rate - pd2.funding_rate) * 100
        min_funding_diff = settings.min_funding_diff_percent

        if funding_diff < min_funding_diff:
            return

        if pd1.funding_rate < pd2.funding_rate:
            long_ex, long_pd = ex2, pd2
            short_ex, short_pd = ex1, pd1
            long_funding = pd2.funding_rate
            short_funding = pd1.funding_rate
        else:
            long_ex, long_pd = ex1, pd1
            short_ex, short_pd = ex2, pd2
            long_funding = pd1.funding_rate
            short_funding = pd2.funding_rate

        max_hours = settings.max_funding_hours
        hours_to_funding = self._get_hours_to_funding(long_pd)
        if hours_to_funding is None:
            hours_to_funding = self._get_hours_to_funding(short_pd)

        if max_hours > 0 and hours_to_funding is not None:
            if hours_to_funding > max_hours:
                return

        price_spread = abs(long_pd.effective_price - short_pd.effective_price) / min(long_pd.effective_price, short_pd.effective_price) * 100

        min_vol = min(long_pd.volume_24h, short_pd.volume_24h)
        if min_vol < 100000:
            return

        now = datetime.now()
        alert_key = f"funding:{symbol}:{long_ex}:{short_ex}"

        if alert_key in self.sent_alerts:
            if (now - self.sent_alerts[alert_key]).seconds < 300:
                return
            self.sent_alerts.move_to_end(alert_key)
        
        self._cleanup_old_alerts()

        level = 'high' if funding_diff >= 2.0 else ('medium' if funding_diff >= 1.0 else 'low')

        alert = SpreadAlert(
            symbol=symbol,
            spread_percent=funding_diff,
            buy_exchange=short_ex,
            sell_exchange=long_ex,
            buy_price=short_pd,
            sell_price=long_pd,
            volume_24h=min_vol,
            funding_diff=long_funding - short_funding,
            timestamp=now,
            alert_level=level,
            arbitrage_type=ArbitrageType.INTER_EXCHANGE_FUTURES,
            hours_to_funding=hours_to_funding,
            basis_info={
                'funding_strategy': True,
                'long_exchange': long_ex,
                'short_exchange': short_ex,
                'long_funding_rate': long_funding,
                'short_funding_rate': short_funding,
                'funding_diff': funding_diff,
                'hours_to_funding': hours_to_funding,
                'price_spread': price_spread,
                'annual_funding_long': long_funding * 3 * 365,
                'annual_funding_short': short_funding * 3 * 365
            }
        )

        self.sent_alerts[alert_key] = now
        await self.notify_subscribers(alert)

    def _get_hours_to_funding(self, pd: PriceData) -> Optional[float]:
        funding_times = [0, 8, 16]

        now = datetime.utcnow()
        current_hour = now.hour + now.minute / 60

        next_funding = None
        for ft in funding_times:
            if ft > current_hour:
                next_funding = ft
                break

        if next_funding is None:
            next_funding = 24

        hours_left = next_funding - current_hour
        return hours_left

    async def _evaluate_arbitrage(self, symbol: str, buy_ex: str, buy_pd: PriceData, sell_ex: str, sell_pd: PriceData, arb_type: ArbitrageType):
        if buy_pd.last_price == 0:
            return

        spread = (sell_pd.effective_price - buy_pd.effective_price) / buy_pd.effective_price * 100

        min_thresh = self.basis_threshold if arb_type != ArbitrageType.INTER_EXCHANGE_FUTURES else 0.1

        if spread < min_thresh:
            return

        if buy_pd.mark_price > 0 and sell_pd.mark_price > 0:
            if buy_pd.mark_last_diff > 0.5 or sell_pd.mark_last_diff > 0.5:
                return

        buy_vol = buy_pd.volume_24h if buy_pd.volume_24h > 0 else 10000000
        sell_vol = sell_pd.volume_24h if sell_pd.volume_24h > 0 else 10000000

        if buy_vol < 100000 or sell_vol < 100000:
            return

        now = datetime.now()
        alert_key = f"{arb_type.value}:{symbol}:{buy_ex}:{sell_ex}"
        
        if alert_key in self.sent_alerts:
            if (now - self.sent_alerts[alert_key]).seconds < 300:
                return
            self.sent_alerts.move_to_end(alert_key)
        
        self._cleanup_old_alerts()

        if spread >= 1.0:
            level = 'high'
        elif spread >= 0.5:
            level = 'medium'
        else:
            level = 'low'

        funding_diff = sell_pd.funding_rate - buy_pd.funding_rate

        alert = SpreadAlert(
            symbol=symbol,
            spread_percent=spread,
            buy_exchange=buy_ex,
            sell_exchange=sell_ex,
            buy_price=buy_pd,
            sell_price=sell_pd,
            volume_24h=min(buy_vol, sell_vol),
            funding_diff=funding_diff,
            timestamp=now,
            alert_level=level,
            arbitrage_type=arb_type
        )

        self.sent_alerts[alert_key] = now
        await self.notify_subscribers(alert)

    def get_current_prices(self):
        return dict(self.prices)
    
    async def get_prices_copy(self):
        return dict(self.prices)

    async def get_top_spreads(self, limit: int = 20) -> list:
        """Получение топ-N активных спредов"""
        try:
            spreads = []
            
            # Получаем все активные спреды из active_spreads
            for spread_key, spread_info in self.active_spreads.items():
                # Check if the spread is fresh (not stale)
                if not spread_info.is_fresh:
                    continue
                spreads.append({
                    'symbol': spread_info.symbol,
                    'spread': spread_info.spread_percent,
                    'spread_percent': spread_info.spread_percent,
                    'buy_exchange': spread_info.buy_exchange,
                    'sell_exchange': spread_info.sell_exchange,
                    'buy_price': spread_info.buy_price,
                    'sell_price': spread_info.sell_price,
                    'volume_24h': spread_info.volume_24h,
                    'arbitrage_type': spread_info.arbitrage_type.value if hasattr(spread_info.arbitrage_type, 'value') else str(spread_info.arbitrage_type),
                    'timestamp': spread_info.timestamp
                })
            
            # Сортируем по размеру спреда (убывание)
            spreads.sort(key=lambda x: x['spread_percent'], reverse=True)
            
            return spreads[:limit]
            
        except Exception as e:
            logger.error(f"Error getting top spreads: {e}")
            return []
