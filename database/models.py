import aiosqlite
import json
import asyncio
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any, Union
import logging

logger = logging.getLogger(__name__)

@dataclass
class UserSettings:
    user_id: int
    is_trading_enabled: bool = False
    api_keys: dict = field(default_factory=dict)
    commission_rates: dict = field(default_factory=lambda: {
        'binance': {'maker': 0.0002, 'taker': 0.0004},
        'bybit': {'maker': 0.0001, 'taker': 0.00055},
        'okx': {'maker': 0.0008, 'taker': 0.001}
    })
    alert_settings: dict = field(default_factory=lambda: {
        'min_spread': 0.2,
        'min_profit_usd': 5,
        'min_volume_24h': 1000000,
        'alert_levels': [
            {'spread': 0.2, 'emoji': '💡', 'sound': False},
            {'spread': 0.5, 'emoji': '⚡', 'sound': True},
            {'spread': 1.0, 'emoji': '🚀', 'sound': True}
        ],
        'exchanges': ['binance', 'bybit', 'okx', 'whitebit', 'mexc'],
        'symbols_whitelist': [],
        'symbols_blacklist': [],
        'funding_arbitrage': True,
        'test_mode': True,  # По умолчанию тестовый режим включен
        'auto_trading': False
    })
    risk_settings: dict = field(default_factory=lambda: {
        'max_position_usd': 10000,
        'max_leverage': 3,
        'max_daily_loss_usd': 500,
        'auto_close_spread': 0.05,
        'max_open_positions': 5,
        'balance_usage_percent': 95,
        'take_profit_percent': 20,
        'stop_loss_breakeven_trigger': 10,
        'trailing_stop_enabled': True,
        'trailing_stop_distance': 10,
        'max_position_hours': 24,
        'atr_multiplier': 2.0,
        'min_stop_loss_percent': 2.0,
        'emergency_stop_percent': 50.0,
        'margin_mode': 'isolated'
    })
    arbitrage_mode: str = 'all'  # 'all' или 'futures_futures_only'
    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now().isoformat()

@dataclass
class Trade:
    id: Optional[int] = None
    user_id: int = 0
    symbol: str = ""
    strategy: str = ""
    long_exchange: str = ""
    short_exchange: str = ""
    entry_spread: float = 0.0
    close_spread: Optional[float] = None
    size_usd: float = 0.0
    pnl_usd: Optional[float] = None
    pnl_percent: float = 0.0
    status: str = "open"
    opened_at: str = field(default_factory=lambda: datetime.now().isoformat())
    closed_at: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    position_size_long: float = 0.0
    position_size_short: float = 0.0
    closed_portion_percent: float = 0.0
    partial_close_count: int = 0
    entry_price_long: float = 0.0
    entry_price_short: float = 0.0
    current_price_long: float = 0.0
    current_price_short: float = 0.0
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0
    breakeven_triggered: bool = False
    trailing_enabled: bool = True
    trailing_stop_price: float = 0.0
    emergency_stop_price: float = 0.0

class Database:
    _instance: Optional['Database'] = None
    _singleton_lock = asyncio.Lock()

    def __new__(cls, db_path: str = 'arbitrage_bot.db'):
        if cls._instance is not None:
            return cls._instance

        instance = super().__new__(cls)
        instance._initialized = False
        instance._db_path = db_path
        instance._conn = None
        instance._conn_lock = asyncio.Lock()
        instance._query_lock = asyncio.Lock()
        instance._init_lock = asyncio.Lock()

        cls._instance = instance
        return instance

    async def initialize(self):
        """Инициализация БД с проверкой на повторный вызов"""
        if self._initialized:
            return

        async with self._init_lock:
            if self._initialized:
                return

            try:
                self._conn = await aiosqlite.connect(self._db_path)
                self._conn.row_factory = aiosqlite.Row
                await self._conn.execute("PRAGMA journal_mode=WAL")
                await self._conn.execute("PRAGMA foreign_keys=ON")
                await self._create_tables()
                await self._migrate_add_arbitrage_mode()
                self._initialized = True
                logger.info(f"Database initialized: {self._db_path} (WAL mode)")
            except Exception as e:
                logger.error(f"Database initialization error: {e}")
                if self._conn:
                    try:
                        await self._conn.close()
                    except:
                        pass
                    self._conn = None
                raise

    async def _migrate_add_arbitrage_mode(self):
        """Миграция: добавление колонки arbitrage_mode если её нет"""
        try:
            async with self._conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]
                if 'arbitrage_mode' not in columns:
                    await self._conn.execute("ALTER TABLE users ADD COLUMN arbitrage_mode TEXT DEFAULT 'all'")
                    await self._conn.commit()
                    logger.info("Migration: added arbitrage_mode column")
        except Exception as e:
            logger.error(f"Migration error: {e}")

    async def close(self):
        """Закрытие соединения с БД"""
        async with self._init_lock:
            if self._initialized and self._conn:
                try:
                    await self._conn.close()
                    logger.info("Database connection closed")
                except Exception as e:
                    logger.error(f"Error closing database: {e}")
                finally:
                    self._initialized = False
                    self._conn = None
                    Database._instance = None

    async def _create_tables(self):
        async with self._query_lock:
            await self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    is_trading_enabled BOOLEAN DEFAULT 0,
                    api_keys TEXT DEFAULT '{}',
                    commission_rates TEXT DEFAULT '{}',
                    alert_settings TEXT DEFAULT '{}',
                    risk_settings TEXT DEFAULT '{}',
                    arbitrage_mode TEXT DEFAULT 'all',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    symbol TEXT,
                    strategy TEXT,
                    long_exchange TEXT,
                    short_exchange TEXT,
                    entry_spread REAL,
                    close_spread REAL,
                    size_usd REAL,
                    pnl_usd REAL,
                    status TEXT DEFAULT 'open',
                    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    metadata TEXT DEFAULT '{}',
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS spread_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    exchange_1 TEXT,
                    exchange_2 TEXT,
                    spread_percent REAL,
                    price_1 REAL,
                    price_2 REAL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_spread_history_time ON spread_history(timestamp);
            """)
            await self._conn.commit()

    async def get_user(self, user_id: int) -> Optional[UserSettings]:
        if not self._initialized:
            await self.initialize()

        async with self._query_lock:
            async with self._conn.execute(
                'SELECT * FROM users WHERE user_id = ?', (user_id,)
            ) as cursor:
                row = await cursor.fetchone()

                if not row:
                    return None

                # ИСПРАВЛЕНО: sqlite3.Row не имеет .get(), используем dict(row)
                row_dict = dict(row)

                return UserSettings(
                    user_id=row_dict['user_id'],
                    is_trading_enabled=bool(row_dict['is_trading_enabled']),
                    api_keys=json.loads(row_dict['api_keys']),
                    commission_rates=json.loads(row_dict['commission_rates']),
                    alert_settings=json.loads(row_dict['alert_settings']),
                    risk_settings=json.loads(row_dict['risk_settings']),
                    arbitrage_mode=row_dict.get('arbitrage_mode', 'all'),
                    created_at=row_dict['created_at'],
                    updated_at=row_dict['updated_at']
                )

    async def create_user(self, user_id: int) -> UserSettings:
        existing = await self.get_user(user_id)
        if existing:
            return existing

        user = UserSettings(user_id=user_id)

        async with self._query_lock:
            try:
                await self._conn.execute("""
                    INSERT INTO users (user_id, api_keys, commission_rates, alert_settings, risk_settings, arbitrage_mode)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    user_id,
                    json.dumps(user.api_keys),
                    json.dumps(user.commission_rates),
                    json.dumps(user.alert_settings),
                    json.dumps(user.risk_settings),
                    user.arbitrage_mode
                ))
                await self._conn.commit()
                logger.info(f"Created new user {user_id}")
            except aiosqlite.IntegrityError:
                logger.warning(f"User {user_id} already exists (race condition)")
                return await self.get_user(user_id)

        return user

    async def get_all_users(self) -> List[UserSettings]:
        """Получить всех пользователей для рассылки алертов и автоподписки"""
        if not self._initialized:
            await self.initialize()

        async with self._query_lock:
            async with self._conn.execute('SELECT * FROM users') as cursor:
                rows = await cursor.fetchall()

                users = []
                for row in rows:
                    # ИСПРАВЛЕНО: sqlite3.Row не имеет .get(), используем dict(row)
                    row_dict = dict(row)
                    users.append(UserSettings(
                        user_id=row_dict['user_id'],
                        is_trading_enabled=bool(row_dict['is_trading_enabled']),
                        api_keys=json.loads(row_dict['api_keys']),
                        commission_rates=json.loads(row_dict['commission_rates']),
                        alert_settings=json.loads(row_dict['alert_settings']),
                        risk_settings=json.loads(row_dict['risk_settings']),
                        arbitrage_mode=row_dict.get('arbitrage_mode', 'all'),
                        created_at=row_dict['created_at'],
                        updated_at=row_dict['updated_at']
                    ))
                return users

    async def update_user(self, user: UserSettings):
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE users SET
                    is_trading_enabled = ?,
                    api_keys = ?,
                    commission_rates = ?,
                    alert_settings = ?,
                    risk_settings = ?,
                    arbitrage_mode = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (
                int(user.is_trading_enabled),
                json.dumps(user.api_keys),
                json.dumps(user.commission_rates),
                json.dumps(user.alert_settings),
                json.dumps(user.risk_settings),
                getattr(user, 'arbitrage_mode', 'all'),
                user.user_id
            ))
            await self._conn.commit()

    async def add_trade(self, trade: Trade) -> int:
        async with self._query_lock:
            cursor = await self._conn.execute("""
                INSERT INTO trades (user_id, symbol, strategy, long_exchange, short_exchange,
                                 entry_spread, size_usd, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.user_id, trade.symbol, trade.strategy, trade.long_exchange,
                trade.short_exchange, trade.entry_spread, trade.size_usd,
                trade.status, json.dumps(trade.metadata)
            ))
            await self._conn.commit()
            return cursor.lastrowid

    async def get_open_trades(self, user_id: int, test_mode: Optional[bool] = None) -> List[Trade]:
        """Получить открытые сделки с опциональным фильтром по test_mode"""
        async with self._query_lock:
            if test_mode is not None:
                # Фильтруем по JSON metadata.test_mode
                async with self._conn.execute(
                    """SELECT * FROM trades 
                    WHERE user_id = ? AND status = 'open'
                    AND json_extract(metadata, '$.test_mode') = ?""",
                    (user_id, json.dumps(test_mode))
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                async with self._conn.execute(
                    "SELECT * FROM trades WHERE user_id = ? AND status = 'open'",
                    (user_id,)
                ) as cursor:
                    rows = await cursor.fetchall()

            trades = []
            for row in rows:
                metadata = json.loads(row['metadata'])
                trade = Trade(
                    id=row['id'],
                    user_id=row['user_id'],
                    symbol=row['symbol'],
                    strategy=row['strategy'],
                    long_exchange=row['long_exchange'],
                    short_exchange=row['short_exchange'],
                    entry_spread=row['entry_spread'],
                    close_spread=row['close_spread'],
                    size_usd=row['size_usd'],
                    pnl_usd=row['pnl_usd'],
                    status=row['status'],
                    opened_at=row['opened_at'],
                    closed_at=row['closed_at'],
                    metadata=metadata,
                    position_size_long=metadata.get('position_size_long', 0),
                    position_size_short=metadata.get('position_size_short', 0),
                    closed_portion_percent=metadata.get('closed_portion_percent', 0),
                    partial_close_count=metadata.get('partial_close_count', 0),
                    entry_price_long=metadata.get('entry_price_long', 0),
                    entry_price_short=metadata.get('entry_price_short', 0),
                    current_price_long=metadata.get('current_price_long', 0),
                    current_price_short=metadata.get('current_price_short', 0),
                    stop_loss_price=metadata.get('stop_loss_price', 0),
                    take_profit_price=metadata.get('take_profit_price', 0),
                    breakeven_triggered=metadata.get('breakeven_triggered', False),
                    trailing_enabled=metadata.get('trailing_enabled', True),
                    trailing_stop_price=metadata.get('trailing_stop_price', 0),
                    emergency_stop_price=metadata.get('emergency_stop_price', 0),
                    pnl_percent=metadata.get('pnl_percent', 0)
                )
                trades.append(trade)
            return trades

    async def get_trade_stats(self, user_id: int, test_mode: Optional[bool] = None) -> Dict[str, Any]:
        """Получить статистику сделок"""
        async with self._query_lock:
            if test_mode is not None:
                query = """SELECT COUNT(*) as count, COALESCE(SUM(pnl_usd), 0) as total_pnl
                        FROM trades WHERE user_id = ? AND status = 'closed'
                        AND json_extract(metadata, '$.test_mode') = ?"""
                async with self._conn.execute(query, (user_id, json.dumps(test_mode))) as cursor:
                    row = await cursor.fetchone()
            else:
                query = """SELECT COUNT(*) as count, COALESCE(SUM(pnl_usd), 0) as total_pnl
                        FROM trades WHERE user_id = ? AND status = 'closed'"""
                async with self._conn.execute(query, (user_id,)) as cursor:
                    row = await cursor.fetchone()

            return {
                'total_trades': row['count'] or 0,
                'total_pnl': row['total_pnl'] or 0,
                'test_mode': test_mode
            }

    async def get_trade_by_id(self, trade_id: int) -> Optional[dict]:
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT * FROM trades WHERE id = ?",
                (trade_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None

    async def update_trade(self, trade: Trade):
        # Обновляем metadata с текущими значениями
        metadata = dict(trade.metadata)
        metadata.update({
            'position_size_long': trade.position_size_long,
            'position_size_short': trade.position_size_short,
            'closed_portion_percent': trade.closed_portion_percent,
            'partial_close_count': trade.partial_close_count,
            'entry_price_long': trade.entry_price_long,
            'entry_price_short': trade.entry_price_short,
            'current_price_long': trade.current_price_long,
            'current_price_short': trade.current_price_short,
            'stop_loss_price': trade.stop_loss_price,
            'take_profit_price': trade.take_profit_price,
            'breakeven_triggered': trade.breakeven_triggered,
            'trailing_enabled': trade.trailing_enabled,
            'trailing_stop_price': trade.trailing_stop_price,
            'emergency_stop_price': trade.emergency_stop_price,
            'pnl_percent': trade.pnl_percent
        })

        async with self._query_lock:
            await self._conn.execute("""
                UPDATE trades SET
                    pnl_usd = ?,
                    status = ?,
                    metadata = ?,
                    closed_at = ?,
                    close_spread = ?
                WHERE id = ?
            """, (
                trade.pnl_usd,
                trade.status,
                json.dumps(metadata),
                trade.closed_at,
                trade.close_spread,
                trade.id
            ))
            await self._conn.commit()

    async def close_trade(self, trade_id: int, close_spread: float, pnl_usd: float):
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE trades SET
                    close_spread = ?,
                    pnl_usd = ?,
                    status = 'closed',
                    closed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (close_spread, pnl_usd, trade_id))
            await self._conn.commit()

    async def log_spread(self, symbol: str, ex1: str, ex2: str, spread: float, p1: float, p2: float):
        async with self._query_lock:
            await self._conn.execute("""
                INSERT INTO spread_history (symbol, exchange_1, exchange_2, spread_percent, price_1, price_2)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, ex1, ex2, spread, p1, p2))
            await self._conn.commit()
