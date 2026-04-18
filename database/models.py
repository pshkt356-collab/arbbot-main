# -*- coding: utf-8 -*-
"""
Database models for Arbitrage Bot - FINAL FIX v4
Исправлено:
1. Добавлен arbitrage_mode в UserSettings
2. Корректное сохранение inter_exchange_enabled и basis_arbitrage_enabled
3. Добавлен метод get_all_users для main.py
"""
import json
import logging
import threading
import aiosqlite
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class UserSettings:
    """User settings and state"""
    user_id: int
    alerts_enabled: bool = True
    min_spread_threshold: float = 2.0
    selected_exchanges: List[str] = field(default_factory=list)
    api_keys: Dict[str, Any] = field(default_factory=dict)
    total_balance: float = 0.0
    trade_amount: float = 100.0
    leverage: int = 1
    stop_loss_percent: float = 2.0
    take_profit_percent: float = 5.0
    notifications_enabled: bool = True
    auto_trade_mode: bool = False
    risk_settings: Dict[str, Any] = field(default_factory=dict)
    alert_settings: Dict[str, Any] = field(default_factory=dict)
    total_trades: int = 0
    successful_trades: int = 0
    failed_trades: int = 0
    total_profit: float = 0.0
    # ИСПРАВЛЕНО: Добавлены поля для типов арбитража
    inter_exchange_enabled: bool = True
    basis_arbitrage_enabled: bool = True
    arbitrage_mode: str = 'all'  # 'all', 'inter_exchange_only', 'basis_only'
    bot_blocked: bool = False
    _cached_balances: Dict[str, Any] = field(default_factory=dict, repr=False)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def add_api_key(self, exchange: str, api_key: str, api_secret: str, testnet: bool = True):
        """Add API key for exchange"""
        self.api_keys[exchange] = {
            'api_key': api_key,
            'api_secret': api_secret,
            'testnet': testnet,
            'added_at': datetime.now().isoformat()
        }

    def update_exchange_balance(self, exchange: str, total: float, free: float, used: float):
        """Update cached balance for exchange"""
        if not hasattr(self, '_cached_balances'):
            self._cached_balances = {}
        self._cached_balances[exchange] = {
            'total': total,
            'free': free,
            'used': used,
            'updated_at': datetime.now().isoformat()
        }
        # Обновляем общий баланс
        self.total_balance = sum(b.get('total', 0) for b in self._cached_balances.values() if isinstance(b, dict))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'user_id': self.user_id,
            'alerts_enabled': self.alerts_enabled,
            'min_spread_threshold': self.min_spread_threshold,
            'selected_exchanges': json.dumps(self.selected_exchanges),
            'api_keys': json.dumps(self.api_keys),
            'total_balance': self.total_balance,
            'trade_amount': self.trade_amount,
            'leverage': self.leverage,
            'stop_loss_percent': self.stop_loss_percent,
            'take_profit_percent': self.take_profit_percent,
            'notifications_enabled': self.notifications_enabled,
            'auto_trade_mode': self.auto_trade_mode,
            'risk_settings': json.dumps(self.risk_settings),
            'alert_settings': json.dumps(self.alert_settings),
            'total_trades': self.total_trades,
            'successful_trades': self.successful_trades,
            'failed_trades': self.failed_trades,
            'total_profit': self.total_profit,
            'inter_exchange_enabled': self.inter_exchange_enabled,
            'basis_arbitrage_enabled': self.basis_arbitrage_enabled,
            'arbitrage_mode': self.arbitrage_mode,
            'bot_blocked': self.bot_blocked,
            '_cached_balances': json.dumps(self._cached_balances) if self._cached_balances else '{}',
            'created_at': self.created_at or datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

    @classmethod
    def from_row(cls, row: aiosqlite.Row) -> 'UserSettings':
        """Create UserSettings from database row"""
        try:
            return cls(
                user_id=row['user_id'],
                alerts_enabled=bool(row['alerts_enabled']),
                min_spread_threshold=float(row['min_spread_threshold']),
                selected_exchanges=json.loads(row['selected_exchanges']) if row['selected_exchanges'] else [],
                api_keys=json.loads(row['api_keys']) if row['api_keys'] else {},
                total_balance=float(row['total_balance']) if row['total_balance'] else 0.0,
                trade_amount=float(row['trade_amount']) if row['trade_amount'] else 100.0,
                leverage=int(row['leverage']) if row['leverage'] else 1,
                stop_loss_percent=float(row['stop_loss_percent']) if row['stop_loss_percent'] else 2.0,
                take_profit_percent=float(row['take_profit_percent']) if row['take_profit_percent'] else 5.0,
                notifications_enabled=bool(row['notifications_enabled']) if row['notifications_enabled'] is not None else True,
                auto_trade_mode=bool(row['auto_trade_mode']) if row['auto_trade_mode'] is not None else False,
                risk_settings=json.loads(row['risk_settings']) if row['risk_settings'] else {},
                alert_settings=json.loads(row['alert_settings']) if row['alert_settings'] else {},
                total_trades=int(row['total_trades']) if row['total_trades'] else 0,
                successful_trades=int(row['successful_trades']) if row['successful_trades'] else 0,
                failed_trades=int(row['failed_trades']) if row['failed_trades'] else 0,
                total_profit=float(row['total_profit']) if row['total_profit'] else 0.0,
                inter_exchange_enabled=bool(row['inter_exchange_enabled']) if row['inter_exchange_enabled'] is not None else True,
                basis_arbitrage_enabled=bool(row['basis_arbitrage_enabled']) if row['basis_arbitrage_enabled'] is not None else True,
                arbitrage_mode=row['arbitrage_mode'] if row['arbitrage_mode'] else 'all',
                bot_blocked=bool(row['bot_blocked']) if row['bot_blocked'] is not None else False,
                _cached_balances=json.loads(row['_cached_balances']) if row.get('_cached_balances') else {},
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
        except Exception as e:
            logger.error(f"Error creating UserSettings from row: {e}, row: {dict(row)}")
            # Return default user on error
            return cls(user_id=row['user_id'])

@dataclass
class Trade:
    """Trade record"""
    id: int
    user_id: int
    symbol: str
    long_exchange: str
    short_exchange: str
    entry_price_long: float
    entry_price_short: float
    current_price_long: float = 0.0
    current_price_short: float = 0.0
    position_size_long: float = 0.0
    position_size_short: float = 0.0
    size_usd: float = 0.0
    entry_spread: float = 0.0
    current_spread: float = 0.0
    pnl_usd: float = 0.0
    pnl_percent: float = 0.0
    stop_loss_price: float = 0.0
    take_profit_price: float = 0.0
    trailing_stop_price: float = 0.0
    trailing_enabled: bool = False
    status: str = 'open'  # 'open', 'closed'
    opened_at: str = None
    closed_at: str = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    # ИСПРАВЛЕНО: Добавлены поля для позиций
    leverage: int = 1
    margin_used: float = 0.0
    funding_fees: float = 0.0
    closing_in_progress: bool = False

class Database:
    """Thread-safe database operations with WAL mode"""
    
    _instance: Optional['Database'] = None
    _instance_lock = threading.Lock()
    _lock = threading.RLock()
    
    def __new__(cls, db_file: Optional[str] = None):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_file: Optional[str] = None):
        if self._initialized:
            return
        self.db_file = db_file or "/app/data/arbitrage_bot.db"
        self._conn: Optional[aiosqlite.Connection] = None
        self._query_lock = asyncio.Lock() if hasattr(asyncio, 'Lock') else None
        self._initialized = False
        self._local = threading.local()
    
    @property
    def is_initialized(self) -> bool:
        """Проверка инициализации (исправлено для main.py)"""
        return self._initialized

    async def initialize(self) -> bool:
        """Initialize database with WAL mode"""
        if self._initialized:
            return True
        
        try:
            # Ensure directory exists
            Path(self.db_file).parent.mkdir(parents=True, exist_ok=True)
            
            self._conn = await aiosqlite.connect(self.db_file)
            self._conn.row_factory = aiosqlite.Row
            
            # Enable WAL mode for better concurrency
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            await self._conn.execute("PRAGMA cache_size=10000")
            await self._conn.execute("PRAGMA temp_store=memory")
            
            await self._create_tables()
            await self._conn.commit()
            
            self._initialized = True
            logger.info(f"Database initialized: {self.db_file} (WAL mode)")
            return True
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            return False

    async def _create_tables(self):
        """Create database tables"""
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                alerts_enabled INTEGER DEFAULT 1,
                min_spread_threshold REAL DEFAULT 2.0,
                selected_exchanges TEXT DEFAULT '[]',
                api_keys TEXT DEFAULT '{}',
                total_balance REAL DEFAULT 0,
                trade_amount REAL DEFAULT 100,
                leverage INTEGER DEFAULT 1,
                stop_loss_percent REAL DEFAULT 2,
                take_profit_percent REAL DEFAULT 5,
                notifications_enabled INTEGER DEFAULT 1,
                auto_trade_mode INTEGER DEFAULT 0,
                risk_settings TEXT DEFAULT '{}',
                alert_settings TEXT DEFAULT '{}',
                total_trades INTEGER DEFAULT 0,
                successful_trades INTEGER DEFAULT 0,
                failed_trades INTEGER DEFAULT 0,
                total_profit REAL DEFAULT 0,
                inter_exchange_enabled INTEGER DEFAULT 1,
                basis_arbitrage_enabled INTEGER DEFAULT 1,
                arbitrage_mode TEXT DEFAULT 'all',
                bot_blocked INTEGER DEFAULT 0,
                _cached_balances TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                symbol TEXT,
                long_exchange TEXT,
                short_exchange TEXT,
                entry_price_long REAL DEFAULT 0,
                entry_price_short REAL DEFAULT 0,
                current_price_long REAL DEFAULT 0,
                current_price_short REAL DEFAULT 0,
                position_size_long REAL DEFAULT 0,
                position_size_short REAL DEFAULT 0,
                size_usd REAL DEFAULT 0,
                entry_spread REAL DEFAULT 0,
                current_spread REAL DEFAULT 0,
                pnl_usd REAL DEFAULT 0,
                pnl_percent REAL DEFAULT 0,
                stop_loss_price REAL DEFAULT 0,
                take_profit_price REAL DEFAULT 0,
                trailing_stop_price REAL DEFAULT 0,
                trailing_enabled INTEGER DEFAULT 0,
                status TEXT DEFAULT 'open',
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                metadata TEXT DEFAULT '{}',
                leverage INTEGER DEFAULT 1,
                margin_used REAL DEFAULT 0,
                funding_fees REAL DEFAULT 0,
                closing_in_progress INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_user_status ON trades(user_id, status)
        """)
        
        # Migration: add new columns if they don't exist
        await self._migrate_users_table()

    async def _migrate_users_table(self):
        """Add missing columns to users table"""
        columns = [
            ('inter_exchange_enabled', 'INTEGER DEFAULT 1'),
            ('basis_arbitrage_enabled', 'INTEGER DEFAULT 1'),
            ('arbitrage_mode', "TEXT DEFAULT 'all'"),
            ('bot_blocked', 'INTEGER DEFAULT 0'),
            ('_cached_balances', "TEXT DEFAULT '{}'"),
        ]
        
        for col_name, col_def in columns:
            try:
                await self._conn.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
                logger.info(f"Added column {col_name} to users table")
            except Exception as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning(f"Migration error for {col_name}: {e}")

    async def get_user(self, user_id: int) -> Optional[UserSettings]:
        """Get user by ID"""
        if not self._initialized:
            return None
        try:
            async with self._conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return UserSettings.from_row(row)
                return None
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None

    async def create_user(self, user_id: int) -> UserSettings:
        """Create new user"""
        if not self._initialized:
            raise Exception("Database not initialized")
        
        user = UserSettings(user_id=user_id)
        
        try:
            await self._conn.execute("""
                INSERT INTO users (
                    user_id, alerts_enabled, min_spread_threshold, selected_exchanges,
                    api_keys, total_balance, trade_amount, leverage, stop_loss_percent,
                    take_profit_percent, notifications_enabled, auto_trade_mode,
                    risk_settings, alert_settings, total_trades, successful_trades,
                    failed_trades, total_profit, inter_exchange_enabled,
                    basis_arbitrage_enabled, arbitrage_mode, bot_blocked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user.user_id, int(user.alerts_enabled), user.min_spread_threshold,
                json.dumps(user.selected_exchanges), json.dumps(user.api_keys),
                user.total_balance, user.trade_amount, user.leverage,
                user.stop_loss_percent, user.take_profit_percent,
                int(user.notifications_enabled), int(user.auto_trade_mode),
                json.dumps(user.risk_settings), json.dumps(user.alert_settings),
                user.total_trades, user.successful_trades, user.failed_trades,
                user.total_profit, int(user.inter_exchange_enabled),
                int(user.basis_arbitrage_enabled), user.arbitrage_mode, int(user.bot_blocked)
            ))
            await self._conn.commit()
            logger.info(f"Created new user: {user_id}")
            return user
        except Exception as e:
            logger.error(f"Error creating user {user_id}: {e}")
            raise

    async def update_user(self, user: UserSettings):
        """Update user settings"""
        if not self._initialized:
            return
        try:
            data = user.to_dict()
            await self._conn.execute("""
                UPDATE users SET
                    alerts_enabled = ?,
                    min_spread_threshold = ?,
                    selected_exchanges = ?,
                    api_keys = ?,
                    total_balance = ?,
                    trade_amount = ?,
                    leverage = ?,
                    stop_loss_percent = ?,
                    take_profit_percent = ?,
                    notifications_enabled = ?,
                    auto_trade_mode = ?,
                    risk_settings = ?,
                    alert_settings = ?,
                    total_trades = ?,
                    successful_trades = ?,
                    failed_trades = ?,
                    total_profit = ?,
                    inter_exchange_enabled = ?,
                    basis_arbitrage_enabled = ?,
                    arbitrage_mode = ?,
                    bot_blocked = ?,
                    _cached_balances = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (
                int(data['alerts_enabled']), data['min_spread_threshold'],
                data['selected_exchanges'], data['api_keys'], data['total_balance'],
                data['trade_amount'], data['leverage'], data['stop_loss_percent'],
                data['take_profit_percent'], int(data['notifications_enabled']),
                int(data['auto_trade_mode']), data['risk_settings'], data['alert_settings'],
                data['total_trades'], data['successful_trades'], data['failed_trades'],
                data['total_profit'], int(data['inter_exchange_enabled']),
                int(data['basis_arbitrage_enabled']), data['arbitrage_mode'],
                int(data['bot_blocked']), data['_cached_balances'],
                user.user_id
            ))
            await self._conn.commit()
        except Exception as e:
            logger.error(f"Error updating user {user.user_id}: {e}")
            raise

    # ИСПРАВЛЕНО: Добавлен метод get_all_users для main.py
    async def get_all_users(self) -> List[UserSettings]:
        """Get all users from database"""
        if not self._initialized:
            return []
        try:
            async with self._conn.execute("SELECT * FROM users") as cursor:
                rows = await cursor.fetchall()
                return [UserSettings.from_row(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []

    async def get_open_trades(self, user_id: int) -> List[Trade]:
        """Get open trades for user"""
        if not self._initialized:
            return []
        try:
            async with self._conn.execute(
                "SELECT * FROM trades WHERE user_id = ? AND status = 'open' ORDER BY opened_at DESC",
                (user_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                trades = []
                for row in rows:
                    trade = Trade(
                        id=row['id'],
                        user_id=row['user_id'],
                        symbol=row['symbol'],
                        long_exchange=row['long_exchange'],
                        short_exchange=row['short_exchange'],
                        entry_price_long=row['entry_price_long'] or 0,
                        entry_price_short=row['entry_price_short'] or 0,
                        current_price_long=row['current_price_long'] or 0,
                        current_price_short=row['current_price_short'] or 0,
                        position_size_long=row['position_size_long'] or 0,
                        position_size_short=row['position_size_short'] or 0,
                        size_usd=row['size_usd'] or 0,
                        entry_spread=row['entry_spread'] or 0,
                        current_spread=row['current_spread'] or 0,
                        pnl_usd=row['pnl_usd'] or 0,
                        pnl_percent=row['pnl_percent'] or 0,
                        stop_loss_price=row['stop_loss_price'] or 0,
                        take_profit_price=row['take_profit_price'] or 0,
                        trailing_stop_price=row['trailing_stop_price'] or 0,
                        trailing_enabled=bool(row['trailing_enabled']),
                        status=row['status'],
                        opened_at=row['opened_at'],
                        closed_at=row['closed_at'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {},
                        leverage=row['leverage'] or 1,
                        margin_used=row['margin_used'] or 0,
                        funding_fees=row['funding_fees'] or 0,
                        closing_in_progress=bool(row['closing_in_progress'])
                    )
                    trades.append(trade)
                return trades
        except Exception as e:
            logger.error(f"Error getting open trades for user {user_id}: {e}")
            return []

    async def get_trade_by_id(self, trade_id: int) -> Optional[Dict]:
        """Get trade by ID"""
        if not self._initialized:
            return None
        try:
            async with self._conn.execute(
                "SELECT * FROM trades WHERE id = ?", (trade_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"Error getting trade {trade_id}: {e}")
            return None

    async def update_trade_field(self, trade_id: int, field: str, value: Any):
        """Update single trade field"""
        if not self._initialized:
            return
        try:
            # Sanitize field name to prevent SQL injection
            allowed_fields = ['stop_loss_price', 'take_profit_price', 'trailing_stop_price',
                            'current_price_long', 'current_price_short', 'pnl_usd', 'pnl_percent',
                            'status', 'closed_at', 'metadata', 'closing_in_progress']
            if field not in allowed_fields:
                raise ValueError(f"Field {field} not allowed")
            
            if field == 'updated_at':
                await self._conn.execute(
                    f"UPDATE trades SET {field} = CURRENT_TIMESTAMP WHERE id = ?",
                    (trade_id,)
                )
            else:
                await self._conn.execute(
                    f"UPDATE trades SET {field} = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (value, trade_id)
                )
            await self._conn.commit()
        except Exception as e:
            logger.error(f"Error updating trade {trade_id} field {field}: {e}")
            raise

    async def close(self):
        """Close database connection"""
        if self._conn:
            await self._conn.close()
            self._conn = None
            self._initialized = False
            Database._instance = None
            logger.info("Database connection closed")

# Backwards compatibility
User = UserSettings