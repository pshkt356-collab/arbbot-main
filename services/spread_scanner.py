# -*- coding: utf-8 -*-
"""
Service for scanning cryptocurrency spreads - FINAL FIX v4
Критические исправления:
1. Проверка порога алертов использует min_spread_threshold пользователя
2. Проверка типа арбитража (inter_exchange_enabled/basis_arbitrage_enabled)
3. Корректная работа с arbitrage_mode (all/inter_exchange_only/basis_only)
4. Поддержка фьючерс-фьючерс спредов
"""
import asyncio
import logging
import time
import threading
from collections import defaultdict, namedtuple, OrderedDict
from typing import Optional, List, Dict, Callable, Any, Set, Tuple
from dataclasses import dataclass, field

# ИСПРАВЛЕНО: Правильные импорты из config (нижний регистр)
from config import settings

logger = logging.getLogger(__name__)

# ИСПРАВЛЕНО: Используем значения из settings вместо констант
SPREAD_TTL_SECONDS = settings.spread_ttl_seconds
SCAN_INTERVAL = settings.scan_interval
MIN_VOLUME_24H = settings.min_volume_24h
EXCHANGE_PRIORITY = ['binance', 'bybit', 'okx', 'mexc', 'whitebit']

# Named tuple для спреда
PriceData = namedtuple('PriceData', ['last_price', 'bid', 'ask', 'volume_24h'])
SpreadAlert = namedtuple('SpreadAlert', [
    'symbol', 'spread_percent', 'buy_exchange', 'sell_exchange',
    'buy_price', 'sell_price', 'arbitrage_type', 'timestamp',
    'volume', 'funding_rate', 'market_conditions'
])

@dataclass
class ArbitrageSpread:
    """Структура для хранения информации о спреде"""
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread_percent: float
    arbitrage_type: str = "inter"  # "inter" или "basis"
    timestamp: float = field(default_factory=time.time)
    volume_24h: float = 0
    buy_funding_rate: float = 0
    sell_funding_rate: float = 0
    market_conditions: Dict = field(default_factory=dict)
    
    def is_valid(self) -> bool:
        """Проверка валидности спреда"""
        return (
            self.buy_price > 0 and 
            self.sell_price > 0 and 
            self.buy_price < self.sell_price and
            self.spread_percent > 0
        )

class SpreadScanner:
    """
    Сканер арбитражных спредов между криптовалютными биржами.
    
    Поддерживаемые режимы:
    - all: все типы арбитража
    - inter_exchange_only: только межбиржевой (фьючерс-фьючерс)
    - basis_only: только базис (спот-фьючерс)
    """
    
    def __init__(self, exchange_managers: Optional[Dict] = None):
        """Initialize the spread scanner"""
        self.exchange_managers = exchange_managers or {}
        self.subscribers: List[Tuple[Callable, int]] = []
        self.user_alerts_enabled: Dict[int, bool] = {}
        self.user_thresholds: Dict[int, float] = {}
        self.user_arbitrage_modes: Dict[int, str] = {}
        self.user_inter_exchange: Dict[int, bool] = {}
        self.user_basis: Dict[int, bool] = {}
        self._blocked_subscribers: Set[int] = set()
        self._last_alert_time: Dict[str, float] = {}
        self._alert_cooldown: float = 300  # 5 минут между алертами для одного спреда
        
        # Хранение данных
        self.prices: Dict[str, Dict[str, Dict[str, PriceData]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self.spreads_cache: OrderedDict[str, ArbitrageSpread] = OrderedDict()
        self.spreads_ttl: Dict[str, float] = {}
        self.cache_lock = threading.RLock()
        
        # Статистика
        self._total_calculations: int = 0
        self._profitable_spreads: int = 0
        self._scan_start_time: float = time.time()
        
        # Контроль работы
        self._stop_event: asyncio.Event = asyncio.Event()
        self._scanner_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._is_running: bool = False
        self._initialized: bool = False
        
        # Состояния подписчиков
        self._subscriber_last_alert: Dict[Tuple[int, str], float] = {}
        
        logger.info("SpreadScanner initialized")

    async def initialize(self) -> bool:
        """Initialize the scanner"""
        if self._initialized:
            return True
        try:
            self._initialized = True
            self._is_running = True
            self._stop_event.clear()
            logger.info("SpreadScanner initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize SpreadScanner: {e}")
            return False

    async def stop(self):
        """Gracefully stop the scanner"""
        if not self._is_running:
            return
        logger.info("Stopping SpreadScanner...")
        self._is_running = False
        self._stop_event.set()
        if self._scanner_task and not self._scanner_task.done():
            try:
                self._scanner_task.cancel()
                await asyncio.wait_for(self._scanner_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Scanner task did not stop in time")
            except asyncio.CancelledError:
                pass
        self._initialized = False
        logger.info("SpreadScanner stopped")

    def set_user_threshold(self, user_id: int, threshold: float):
        """Set alert threshold for a user"""
        self.user_thresholds[user_id] = threshold
        logger.info(f"Set threshold {threshold}% for user {user_id}")

    def set_user_arbitrage_mode(self, user_id: int, mode: str):
        """Set arbitrage mode for user"""
        self.user_arbitrage_modes[user_id] = mode
        logger.info(f"Set arbitrage mode {mode} for user {user_id}")
        
    def set_user_alert_settings(self, user_id: int, inter_enabled: bool, basis_enabled: bool):
        """Set alert type preferences for user"""
        self.user_inter_exchange[user_id] = inter_enabled
        self.user_basis[user_id] = basis_enabled
        logger.info(f"Set alert settings for user {user_id}: inter={inter_enabled}, basis={basis_enabled}")

    def subscribe(self, callback: Callable, user_id: int) -> bool:
        """Subscribe to spread alerts"""
        if any(sub[1] == user_id for sub in self.subscribers):
            return False
        self.subscribers.append((callback, user_id))
        self.user_alerts_enabled[user_id] = True
        logger.info(f"User {user_id} subscribed to alerts")
        return True

    def unsubscribe(self, user_id: int):
        """Unsubscribe from alerts"""
        self.subscribers = [s for s in self.subscribers if s[1] != user_id]
        self.user_alerts_enabled.pop(user_id, None)
        self.user_thresholds.pop(user_id, None)
        logger.info(f"User {user_id} unsubscribed from alerts")

    async def update_prices(self, prices_dict: Dict):
        """Update prices from exchange manager"""
        if not prices_dict:
            return
        with self.cache_lock:
            for symbol, exchange_data in prices_dict.items():
                if not isinstance(exchange_data, dict):
                    continue
                for exchange, data in exchange_data.items():
                    if isinstance(data, dict):
                        for market_type, price_info in data.items():
                            if price_info and hasattr(price_info, 'last_price') and price_info.last_price > 0:
                                self.prices[symbol][exchange][market_type] = price_info

    async def calculate_spreads(self) -> List[ArbitrageSpread]:
        """Calculate all arbitrage spreads"""
        spreads = []
        
        with self.cache_lock:
            prices_copy = dict(self.prices)
        
        if not prices_copy:
            return spreads

        symbols = list(prices_copy.keys())
        
        for symbol in symbols:
            try:
                symbol_spreads = await self._calculate_symbol_spreads(symbol, prices_copy)
                spreads.extend(symbol_spreads)
            except Exception as e:
                logger.error(f"Error calculating spreads for {symbol}: {e}")
        
        # Сортировка по спреду (убывание)
        spreads.sort(key=lambda x: x.spread_percent, reverse=True)
        
        # Кэширование
        with self.cache_lock:
            for spread in spreads:
                key = f"{spread.symbol}:{spread.buy_exchange}:{spread.sell_exchange}"
                self.spreads_cache[key] = spread
                self.spreads_ttl[key] = time.time() + SPREAD_TTL_SECONDS
        
        return spreads

    async def _calculate_symbol_spreads(
        self, 
        symbol: str, 
        prices_dict: Dict
    ) -> List[ArbitrageSpread]:
        """Calculate spreads for a single symbol"""
        spreads = []
        symbol_data = prices_dict.get(symbol, {})
        
        if not symbol_data:
            return spreads
        
        # Получаем все биржи для символа
        exchanges = list(symbol_data.keys())
        if len(exchanges) < 2:
            return spreads
        
        # Межбиржевой арбитраж (фьючерс-фьючерс)
        for i, buy_ex in enumerate(exchanges):
            for sell_ex in exchanges[i+1:]:
                try:
                    # Получаем цены фьючерсов
                    buy_futures = symbol_data[buy_ex].get('futures')
                    sell_futures = symbol_data[sell_ex].get('futures')
                    
                    if buy_futures and sell_futures:
                        if buy_futures.last_price > 0 and sell_futures.last_price > 0:
                            # Проверяем объем
                            if buy_futures.volume_24h < MIN_VOLUME_24H or sell_futures.volume_24h < MIN_VOLUME_24H:
                                continue
                            
                            spread_pct = ((sell_futures.last_price - buy_futures.last_price) / buy_futures.last_price) * 100
                            
                            if spread_pct > 0.1:  # Минимальный спред 0.1%
                                spread = ArbitrageSpread(
                                    symbol=symbol,
                                    buy_exchange=buy_ex,
                                    sell_exchange=sell_ex,
                                    buy_price=buy_futures.last_price,
                                    sell_price=sell_futures.last_price,
                                    spread_percent=spread_pct,
                                    arbitrage_type="inter",
                                    volume_24h=min(buy_futures.volume_24h, sell_futures.volume_24h),
                                    buy_funding_rate=0,  # TODO: add funding rate
                                    sell_funding_rate=0,
                                    market_conditions={'type': 'futures-futures'}
                                )
                                spreads.append(spread)
                                
                except Exception as e:
                    logger.debug(f"Error calculating inter spread {buy_ex}-{sell_ex} for {symbol}: {e}")
        
        # Базисный арбитраж (спот-фьючерс) - для каждой биржи отдельно
        for exchange in exchanges:
            try:
                spot = symbol_data[exchange].get('spot')
                futures = symbol_data[exchange].get('futures')
                
                if spot and futures:
                    if spot.last_price > 0 and futures.last_price > 0:
                        # Проверяем объем
                        if spot.volume_24h < MIN_VOLUME_24H or futures.volume_24h < MIN_VOLUME_24H:
                            continue
                        
                        spread_pct = abs((futures.last_price - spot.last_price) / spot.last_price) * 100
                        
                        if spread_pct > 0.1:
                            # Определяем направление
                            if futures.last_price > spot.last_price:
                                spread = ArbitrageSpread(
                                    symbol=symbol,
                                    buy_exchange=exchange,
                                    sell_exchange=exchange,
                                    buy_price=spot.last_price,
                                    sell_price=futures.last_price,
                                    spread_percent=spread_pct,
                                    arbitrage_type="basis",
                                    volume_24h=min(spot.volume_24h, futures.volume_24h),
                                    buy_funding_rate=0,
                                    sell_funding_rate=0,
                                    market_conditions={'type': 'spot-futures', 'exchange': exchange}
                                )
                            else:
                                spread = ArbitrageSpread(
                                    symbol=symbol,
                                    buy_exchange=exchange,
                                    sell_exchange=exchange,
                                    buy_price=futures.last_price,
                                    sell_price=spot.last_price,
                                    spread_percent=spread_pct,
                                    arbitrage_type="basis",
                                    volume_24h=min(spot.volume_24h, futures.volume_24h),
                                    buy_funding_rate=0,
                                    sell_funding_rate=0,
                                    market_conditions={'type': 'futures-spot', 'exchange': exchange}
                                )
                            spreads.append(spread)
                            
            except Exception as e:
                logger.debug(f"Error calculating basis spread for {symbol} on {exchange}: {e}")
        
        return spreads

    async def get_top_spreads(self, n: int = 20, min_spread: Optional[float] = None) -> List[Dict]:
        """Get top N spreads"""
        spreads = await self.calculate_spreads()
        
        if min_spread:
            spreads = [s for s in spreads if s.spread_percent >= min_spread]
        
        # Форматируем для отображения
        result = []
        for spread in spreads[:n]:
            result.append({
                'symbol': spread.symbol,
                'buy_exchange': spread.buy_exchange,
                'sell_exchange': spread.sell_exchange,
                'spread': spread.spread_percent,
                'buy_price': spread.buy_price,
                'sell_price': spread.sell_price,
                'type': spread.arbitrage_type,
                'volume_24h': spread.volume_24h
            })
        
        return result

    async def get_prices_copy(self) -> Dict:
        """Get copy of current prices"""
        with self.cache_lock:
            return dict(self.prices)

    async def notify_subscribers(self, spread: ArbitrageSpread):
        """Notify all subscribers about a spread"""
        if not spread or not spread.is_valid():
            return
        
        spread_key = f"{spread.symbol}:{spread.buy_exchange}:{spread.sell_exchange}"
        current_time = time.time()
        
        # Проверяем кулдаун для этого спреда
        last_alert = self._last_alert_time.get(spread_key, 0)
        if current_time - last_alert < self._alert_cooldown:
            return
        
        for callback, user_id in self.subscribers:
            try:
                # Проверяем, не заблокирован ли пользователь
                if user_id in self._blocked_subscribers:
                    continue
                
                # Проверяем включены ли алерты
                if not self.user_alerts_enabled.get(user_id, True):
                    continue
                
                # ИСПРАВЛЕНО: Получаем порог пользователя из self.user_thresholds
                user_threshold = self.user_thresholds.get(user_id, 2.0)
                
                # ИСПРАВЛЕНО: Проверяем порог
                if spread.spread_percent < user_threshold:
                    continue
                
                # ИСПРАВЛЕНО: Проверяем режим арбитража пользователя
                user_mode = self.user_arbitrage_modes.get(user_id, 'all')
                
                if user_mode == 'inter_exchange_only' and spread.arbitrage_type != 'inter':
                    continue
                if user_mode == 'basis_only' and spread.arbitrage_type != 'basis':
                    continue
                
                # ИСПРАВЛЕНО: Проверяем тип арбитража (inter_exchange_enabled/basis_enabled)
                if spread.arbitrage_type == 'inter' and not self.user_inter_exchange.get(user_id, True):
                    continue
                if spread.arbitrage_type == 'basis' and not self.user_basis.get(user_id, True):
                    continue
                
                # Проверяем персональный кулдаун для пользователя
                user_spread_key = (user_id, spread_key)
                user_last_alert = self._subscriber_last_alert.get(user_spread_key, 0)
                if current_time - user_last_alert < self._alert_cooldown:
                    continue
                
                # Создаем алерт
                alert = SpreadAlert(
                    symbol=spread.symbol,
                    spread_percent=spread.spread_percent,
                    buy_exchange=spread.buy_exchange,
                    sell_exchange=spread.sell_exchange,
                    buy_price=spread.buy_price,
                    sell_price=spread.sell_price,
                    arbitrage_type=spread.arbitrage_type,
                    timestamp=current_time,
                    volume=spread.volume_24h,
                    funding_rate=spread.buy_funding_rate,
                    market_conditions=spread.market_conditions
                )
                
                # Отправляем алерт
                await callback(alert, user_id)
                
                # Обновляем время последнего алерта
                self._subscriber_last_alert[user_spread_key] = current_time
                
            except Exception as e:
                logger.error(f"Error notifying user {user_id}: {e}")
        
        # Обновляем глобальное время алерта
        self._last_alert_time[spread_key] = current_time

    async def scan_task(self):
        """Background scan task"""
        while not self._stop_event.is_set():
            try:
                spreads = await self.calculate_spreads()
                
                # Отправляем алерты для всех найденных спредов
                for spread in spreads:
                    await self.notify_subscribers(spread)
                
                await asyncio.wait_for(
                    self._stop_event.wait(), 
                    timeout=SCAN_INTERVAL
                )
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in scan task: {e}")
                await asyncio.sleep(1)

    async def start_scanning(self):
        """Start background scanning"""
        if self._scanner_task is None or self._scanner_task.done():
            self._scanner_task = asyncio.create_task(self.scan_task())
            logger.info("Spread scanning started")

    def get_stats(self) -> Dict:
        """Get scanner statistics"""
        return {
            'total_calculations': self._total_calculations,
            'profitable_spreads': self._profitable_spreads,
            'subscribers': len(self.subscribers),
            'cached_spreads': len(self.spreads_cache),
            'uptime': time.time() - self._scan_start_time
        }