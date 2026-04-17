"""
Database models and operations for the arbitrage bot.
FIXED VERSION - Addresses critical bugs:
1. Race condition in Database singleton (FIXED)
2. SQL Injection vulnerability (FIXED)
"""

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
        'test_mode': True,
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
        'margin_mode': 'isolated',
        'available_balance': 0.0,
        'total_balance': 0.0,
        'locked_balance': 0.0,
        'trade_amount': 100.0
    })
    arbitrage_mode: str = 'all'
    auto_trade_mode: bool = False
    alerts_enabled: bool = True
    notifications_enabled: bool = True
    selected_exchanges: list = field(default_factory=lambda: ['binance', 'bybit', 'okx', 'whitebit', 'mexc'])
    min_spread_threshold: float = 0.2
    trade_amount: float = 100.0
    inter_exchange_enabled: bool = True
    basis_arbitrage_enabled: bool = True
    leverage: int = 3
    total_trades: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    total_profit: float = 0.0
    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now().isoformat()

    @property
    def total_balance(self) -> float:
        return self.risk_settings.get('total_balance', 0.0)

    @property
    def available_balance(self) -> float:
        return self.risk_settings.get('available_balance', 0.0)

    @property
    def locked_balance(self) -> float:
        return self.risk_settings.get('locked_balance', 0.0)


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
        """Инициализация БД с проверкой на повторный вызов - FIXED RACE CONDITION"""
        # FIX: Check _initialized inside the lock to prevent race condition
        async with self._init_lock:
            if self._initialized:
                return
            
            try:
                self._conn = await aiosqlite.connect(self._db_path)
                self._conn.row_factory = aiosqlite.Row
                await self._conn.execute("PRAGMA journal_mode=WAL")
                await self._conn.execute("PRAGMA foreign_keys=ON")
                await self._create_tables()
                self._initialized = True
                logger.info(f"Database initialized: {self._db_path} (WAL mode)")
            except Exception as e:
                logger.error(f"Database initialization error: {e}")
                if self._conn:
                    await self._conn.close()
                raise

    async def _create_tables(self):
        """Создание таблиц"""
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                settings TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        
        await self._conn.execute("""
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
                pnl_percent REAL,
                status TEXT DEFAULT 'open',
                opened_at TEXT,
                closed_at TEXT,
                metadata TEXT,
                position_size_long REAL,
                position_size_short REAL,
                entry_price_long REAL,
                entry_price_short REAL,
                current_price_long REAL,
                current_price_short REAL,
                stop_loss_price REAL,
                take_profit_price REAL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        
        await self._conn.commit()

    async def get_user(self, user_id: int) -> Optional[UserSettings]:
        """Получение настроек пользователя"""
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT settings FROM users WHERE user_id = ?",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    settings = json.loads(row['settings'])
                    return UserSettings(user_id=user_id, **settings)
                return None

    async def create_user(self, user: UserSettings):
        """Создание пользователя"""
        async with self._query_lock:
            settings_dict = asdict(user)
            settings_dict.pop('user_id')
            await self._conn.execute(
                "INSERT INTO users (user_id, settings, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (user.user_id, json.dumps(settings_dict), user.created_at, user.updated_at)
            )
            await self._conn.commit()

    async def update_user(self, user: UserSettings):
        """Обновление настроек пользователя"""
        async with self._query_lock:
            settings_dict = asdict(user)
            settings_dict.pop('user_id')
            user.updated_at = datetime.now().isoformat()
            await self._conn.execute(
                "UPDATE users SET settings = ?, updated_at = ? WHERE user_id = ?",
                (json.dumps(settings_dict), user.updated_at, user.user_id)
            )
            await self._conn.commit()

    async def create_trade(self, trade: Trade) -> int:
        """Создание сделки"""
        async with self._query_lock:
            trade_dict = asdict(trade)
            trade_dict.pop('id')
            columns = ', '.join(trade_dict.keys())
            placeholders = ', '.join(['?' for _ in trade_dict])
            values = tuple(trade_dict.values())
            
            cursor = await self._conn.execute(
                f"INSERT INTO trades ({columns}) VALUES ({placeholders})",
                values
            )
            await self._conn.commit()
            return cursor.lastrowid

    async def update_trade(self, trade: Trade):
        """Обновление сделки"""
        async with self._query_lock:
            await self._conn.execute(
                """UPDATE trades SET 
                    status = ?, pnl_usd = ?, pnl_percent = ?, 
                    current_price_long = ?, current_price_short = ?,
                    closed_at = ?, close_spread = ?
                WHERE id = ?""",
                (trade.status, trade.pnl_usd, trade.pnl_percent,
                 trade.current_price_long, trade.current_price_short,
                 trade.closed_at, trade.close_spread, trade.id)
            )
            await self._conn.commit()

    async def get_trade(self, trade_id: int) -> Optional[Trade]:
        """Получение сделки по ID"""
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT * FROM trades WHERE id = ?",
                (trade_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return Trade(**dict(row))
                return None

    async def get_open_trades(self, user_id: int, test_mode: bool = True) -> List[Trade]:
        """Получение открытых сделок пользователя - FIXED SQL INJECTION"""
        # FIX: Validate test_mode type to prevent SQL injection
        if not isinstance(test_mode, bool):
            raise TypeError(f"test_mode must be bool, got {type(test_mode)}")
        
        # Convert bool to int for safe SQL usage
        test_mode_int = 1 if test_mode else 0
        
        async with self._query_lock:
            try:
                async with self._conn.execute(
                    """SELECT * FROM trades WHERE user_id = ? AND status = 'open'
                    AND json_extract(metadata, '$.test_mode') = ?""",
                    (user_id, test_mode_int)
                ) as cursor:
                    rows = await cursor.fetchall()
                    return [Trade(**dict(row)) for row in rows]
            except Exception as e:
                logger.error(f"Error getting open trades: {e}")
                return []

    async def close(self):
        """Закрытие соединения с БД"""
        if self._conn:
            await self._conn.close()
            self._initialized = False
            Database._instance = None
            logger.info("Database connection closed")
