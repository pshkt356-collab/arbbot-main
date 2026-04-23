import aiosqlite
import json
import asyncio
import os
import base64
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any, Union
import logging

from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)

class ApiKeyEncryption:
    """Шифрование/дешифрование API ключей через Fernet"""
    _instance = None
    _fernet = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            key = os.environ.get('API_ENCRYPTION_KEY')
            if key:
                # Убедимся что ключ в правильном формате для Fernet (32 bytes, base64-encoded)
                try:
                    cls._instance._fernet = Fernet(key.encode())
                except Exception:
                    # Если ключ невалидный — генерируем из него валидный
                    import hashlib
                    derived_key = base64.urlsafe_b64encode(hashlib.sha256(key.encode()).digest())
                    cls._instance._fernet = Fernet(derived_key)
            else:
                logger.warning("API_ENCRYPTION_KEY not set — API keys will be stored UNENCRYPTED!")
        return cls._instance

    def encrypt(self, value: str) -> str:
        if not value or not self._fernet:
            return value
        try:
            return self._fernet.encrypt(value.encode()).decode()
        except Exception:
            return value

    def decrypt(self, value: str) -> str:
        if not value or not self._fernet:
            return value
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except Exception:
            # Если не удалось расшифровать — возможно значение не зашифровано
            return value

    def encrypt_dict(self, data: dict) -> dict:
        """Шифрует api_key и api_secret внутри словаря"""
        if not self._fernet or not data:
            return data
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = {}
                for k, v in value.items():
                    if k in ('api_key', 'api_secret', 'password') and v and isinstance(v, str):
                        result[key][k] = self.encrypt(v)
                    else:
                        result[key][k] = v
            else:
                result[key] = value
        return result

    def decrypt_dict(self, data: dict) -> dict:
        """Расшифровывает api_key и api_secret внутри словаря"""
        if not self._fernet or not data:
            return data
        result = {}
        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = {}
                for k, v in value.items():
                    if k in ('api_key', 'api_secret', 'password') and v and isinstance(v, str):
                        result[key][k] = self.decrypt(v)
                    else:
                        result[key][k] = v
            else:
                result[key] = value
        return result

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
    scan_type: str = 'all'  # 'all', 'inter', 'basis', 'funding'
    funding_arbitrage_enabled: bool = True  # Фандинг арбитраж включен
    
    # Дополнительные поля для совместимости с callbacks.py
    auto_trade_mode: bool = False  # Режим авто-трейдинга
    alerts_enabled: bool = True    # Включены ли алерты
    notifications_enabled: bool = True  # Уведомления включены
    selected_exchanges: list = field(default_factory=lambda: ['binance', 'bybit', 'okx', 'whitebit', 'mexc'])  # Выбранные биржи
    min_spread_threshold: float = 0.2  # Минимальный порог спреда
    trade_amount: float = 100.0  # Объем сделки в USDT
    inter_exchange_enabled: bool = True  # Межбиржевой арбитраж включен
    basis_arbitrage_enabled: bool = True  # Базисный арбитраж включен
    leverage: int = 3  # Плечо торговли
    total_trades: int = 0  # Всего сделок
    successful_trades: int = 0  # Успешных сделок
    failed_trades: int = 0  # Неудачных сделок
    total_profit: float = 0.0  # Общая прибыль
    bot_blocked: bool = False  # Пользователь заблокировал бота

    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now().isoformat()

    # Internal storage for cached exchange balances
    _cached_balances: dict = field(default_factory=dict, repr=False)

    @property
    def total_balance(self) -> float:
        """Общий баланс: сумма всех кешированных балансов бирж"""
        if hasattr(self, '_cached_balances') and self._cached_balances:
            return sum(
                bal.get('total', 0) 
                for bal in self._cached_balances.values() 
                if isinstance(bal, dict)
            )
        # Fallback to legacy storage in risk_settings
        return self.risk_settings.get('total_balance', 0.0)

    @total_balance.setter
    def total_balance(self, value: float):
        """Установить общий баланс (сохраняет в risk_settings)"""
        self.risk_settings['total_balance'] = value
    
    @property
    def available_balance(self) -> float:
        """Доступный баланс: сумма доступных средств на всех биржах"""
        if hasattr(self, '_cached_balances') and self._cached_balances:
            return sum(
                bal.get('free', 0) 
                for bal in self._cached_balances.values() 
                if isinstance(bal, dict)
            )
        return self.risk_settings.get('available_balance', 0.0)
    
    @property
    def locked_balance(self) -> float:
        """Заблокированный баланс: сумма средств в ордерах"""
        if hasattr(self, '_cached_balances') and self._cached_balances:
            return sum(
                bal.get('used', 0) 
                for bal in self._cached_balances.values() 
                if isinstance(bal, dict)
            )
        return self.risk_settings.get('locked_balance', 0.0)

    @property
    def test_mode(self) -> bool:
        """Тестовый режим торговли — читает из alert_settings"""
        return self.alert_settings.get('test_mode', True)

    @test_mode.setter
    def test_mode(self, value: bool):
        """Установить тестовый режим — сохраняет в alert_settings"""
        self.alert_settings['test_mode'] = value

    def update_exchange_balance(self, exchange_id: str, total: float = 0, free: float = 0, used: float = 0):
        """Обновить баланс конкретной биржи"""
        if not hasattr(self, '_cached_balances'):
            self._cached_balances = {}
        self._cached_balances[exchange_id] = {'total': total, 'free': free, 'used': used}

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

@dataclass
class FlipSettings:
    """Настройки MEXC Flip Trading для пользователя"""
    user_id: int
    enabled: bool = False
    selected_symbols: list = field(default_factory=lambda: ['BTC', 'ETH', 'SOL', 'TAO', 'ASTER', 'BCH'])  # Символы для торговли
    leverage: int = 200  # Плечо (50-300x)
    position_size_usd: float = 100.0  # Размер позиции в USDT
    max_daily_flips: int = 300  # Максимум флипов в день
    max_daily_loss_usd: float = 50.0  # Макс дневной убыток
    min_price_movement_pct: float = 0.01  # Мин движение цены для входа (%)
    close_on_reverse: bool = True  # Закрывать при развороте
    test_mode: bool = True  # Тестовый режим
    mexc_api_key: str = ""  # API ключ MEXC (переопределяет глобальный)
    mexc_api_secret: str = ""  # API секрет MEXC
    created_at: str = None
    updated_at: str = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now().isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now().isoformat()

@dataclass 
class FlipTrade:
    """Одна сделка MEXC Flip Trading"""
    id: Optional[int] = None
    user_id: int = 0
    symbol: str = ""  # Символ (BTC, ETH и т.д.)
    direction: str = "long"  # Только лонг
    entry_price: float = 0.0
    exit_price: float = 0.0
    pnl_usd: float = 0.0
    pnl_percent: float = 0.0
    leverage: int = 200
    position_size_usd: float = 100.0
    quantity: float = 0.0  # Количество монет
    status: str = "open"  # open, closed, error
    close_reason: str = ""  # reverse, take_profit, stop_loss, manual, error
    binance_entry_price: float = 0.0  # Цена Binance при входе
    binance_exit_price: float = 0.0  # Цена Binance при выходе
    opened_at: str = field(default_factory=lambda: datetime.now().isoformat())
    closed_at: Optional[str] = None
    duration_ms: int = 0  # Длительность сделки в миллисекундах
    metadata: dict = field(default_factory=dict)  # Доп данные

class Database:
    _instance: Optional['Database'] = None
    _singleton_lock = asyncio.Lock()

    def __new__(cls, db_path: str = None):
        if db_path is None:
            from config import settings
            db_path = settings.db_file

        if cls._instance is not None:
            return cls._instance

        # Блокируем создание нескольких инстансов
        # Note: __new__ не может быть async, используем синхронный lock
        import threading
        _thread_lock = threading.Lock()
        with _thread_lock:
            if cls._instance is not None:
                return cls._instance

            instance = super().__new__(cls)
            instance._initialized = False
            instance._db_path = db_path
            instance._conn = None
            instance._conn_lock = asyncio.Lock()
            instance._query_lock = asyncio.Lock()
            instance._init_lock = asyncio.Lock()
            instance._key_enc = ApiKeyEncryption()

            cls._instance = instance
            return instance

    async def initialize(self):
        """Инициализация БД с проверкой на повторный вызов"""
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
                await self._migrate_add_selected_exchanges()
                await self._migrate_add_user_settings_columns()
                await self._migrate_add_bot_blocked_column()
                await self._migrate_add_scan_type_column()
                await self._migrate_add_flip_tables()
                await self._migrate_add_flip_api_columns()
                await self._migrate_add_uid_flip_tables()
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

    async def _migrate_add_selected_exchanges(self):
        """Миграция: добавление колонки selected_exchanges если её нет"""
        try:
            async with self._conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]
                if 'selected_exchanges' not in columns:
                    await self._conn.execute("ALTER TABLE users ADD COLUMN selected_exchanges TEXT DEFAULT '[\"binance\", \"bybit\", \"okx\", \"whitebit\", \"mexc\"]'")
                    await self._conn.commit()
                    logger.info("Migration: added selected_exchanges column")
        except Exception as e:
            logger.error(f"Migration error for selected_exchanges: {e}")

    async def _migrate_add_user_settings_columns(self):
        """Migration: add columns for extended user settings"""
        new_columns = [
            ('min_spread_threshold', 'REAL DEFAULT 0.2'),
            ('alerts_enabled', 'BOOLEAN DEFAULT 1'),
            ('inter_exchange_enabled', 'BOOLEAN DEFAULT 1'),
            ('basis_arbitrage_enabled', 'BOOLEAN DEFAULT 1'),
            ('auto_trade_mode', 'BOOLEAN DEFAULT 0'),
            ('trade_amount', 'REAL DEFAULT 100.0'),
            ('leverage', 'INTEGER DEFAULT 3'),
            ('notifications_enabled', 'BOOLEAN DEFAULT 1'),
            ('total_trades', 'INTEGER DEFAULT 0'),
            ('successful_trades', 'INTEGER DEFAULT 0'),
            ('failed_trades', 'INTEGER DEFAULT 0'),
            ('total_profit', 'REAL DEFAULT 0.0'),
        ]
        try:
            async with self._conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]
            for col_name, col_type in new_columns:
                if col_name not in columns:
                    await self._conn.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                    await self._conn.commit()
                    logger.info(f"Migration: added {col_name} column")
        except Exception as e:
            logger.error(f"Migration error for user settings columns: {e}")


    async def _migrate_add_bot_blocked_column(self):
        """Миграция: добавление колонки bot_blocked если её нет"""
        try:
            async with self._conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]
                if 'bot_blocked' not in columns:
                    await self._conn.execute("ALTER TABLE users ADD COLUMN bot_blocked BOOLEAN DEFAULT 0")
                    await self._conn.commit()
                    logger.info("Migration: added bot_blocked column")
        except Exception as e:
            logger.error(f"Migration error for bot_blocked: {e}")

    async def _migrate_add_scan_type_column(self):
        """Миграция: добавление колонок scan_type и funding_arbitrage_enabled"""
        try:
            async with self._conn.execute("PRAGMA table_info(users)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]
                if 'scan_type' not in columns:
                    await self._conn.execute("ALTER TABLE users ADD COLUMN scan_type TEXT DEFAULT 'all'")
                    await self._conn.commit()
                    logger.info("Migration: added scan_type column")
                if 'funding_arbitrage_enabled' not in columns:
                    await self._conn.execute("ALTER TABLE users ADD COLUMN funding_arbitrage_enabled BOOLEAN DEFAULT 1")
                    await self._conn.commit()
                    logger.info("Migration: added funding_arbitrage_enabled column")
        except Exception as e:
            logger.error(f"Migration error for scan_type/funding: {e}")

    async def _migrate_add_flip_tables(self):
        """Миграция: создание таблиц для MEXC Flip Trading"""
        try:
            async with self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='flip_settings'"
            ) as cursor:
                if not await cursor.fetchone():
                    await self._conn.executescript("""
                        CREATE TABLE IF NOT EXISTS flip_settings (
                            user_id INTEGER PRIMARY KEY,
                            enabled BOOLEAN DEFAULT 0,
                            selected_symbols TEXT DEFAULT '["BTC", "ETH", "SOL", "TAO", "ASTER", "BCH"]',
                            leverage INTEGER DEFAULT 200,
                            position_size_usd REAL DEFAULT 100.0,
                            max_daily_flips INTEGER DEFAULT 300,
                            max_daily_loss_usd REAL DEFAULT 50.0,
                            min_price_movement_pct REAL DEFAULT 0.01,
                            close_on_reverse BOOLEAN DEFAULT 1,
                            test_mode BOOLEAN DEFAULT 1,
                            mexc_api_key TEXT DEFAULT '',
                            mexc_api_secret TEXT DEFAULT '',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (user_id) REFERENCES users(user_id)
                        );

                        CREATE TABLE IF NOT EXISTS flip_trades (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER,
                            symbol TEXT,
                            direction TEXT DEFAULT 'long',
                            entry_price REAL,
                            exit_price REAL,
                            pnl_usd REAL DEFAULT 0,
                            pnl_percent REAL DEFAULT 0,
                            leverage INTEGER DEFAULT 200,
                            position_size_usd REAL DEFAULT 100,
                            quantity REAL DEFAULT 0,
                            status TEXT DEFAULT 'open',
                            close_reason TEXT DEFAULT '',
                            binance_entry_price REAL DEFAULT 0,
                            binance_exit_price REAL DEFAULT 0,
                            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            closed_at TIMESTAMP,
                            duration_ms INTEGER DEFAULT 0,
                            metadata TEXT DEFAULT '{}',
                            FOREIGN KEY (user_id) REFERENCES users(user_id)
                        );

                        CREATE INDEX IF NOT EXISTS idx_flip_trades_user ON flip_trades(user_id, status);
                        CREATE INDEX IF NOT EXISTS idx_flip_trades_time ON flip_trades(opened_at);
                    """)
                    await self._conn.commit()
                    logger.info("Migration: created flip_settings and flip_trades tables")
        except Exception as e:
            logger.error(f"Migration error for flip tables: {e}")

    async def _migrate_add_flip_api_columns(self):
        """Миграция: добавление колонок mexc_api_key/secret в существующую flip_settings"""
        try:
            # Проверяем существование таблицы
            async with self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='flip_settings'"
            ) as cursor:
                if not await cursor.fetchone():
                    return  # Таблицы нет - будет создана полностью другой миграцией

            # Проверяем существование колонки mexc_api_key
            async with self._conn.execute("PRAGMA table_info(flip_settings)") as cursor:
                columns = [row['name'] for row in await cursor.fetchall()]

            if 'mexc_api_key' not in columns:
                await self._conn.execute("ALTER TABLE flip_settings ADD COLUMN mexc_api_key TEXT DEFAULT ''")
                logger.info("Migration: added mexc_api_key column to flip_settings")
            if 'mexc_api_secret' not in columns:
                await self._conn.execute("ALTER TABLE flip_settings ADD COLUMN mexc_api_secret TEXT DEFAULT ''")
                logger.info("Migration: added mexc_api_secret column to flip_settings")

            await self._conn.commit()
        except Exception as e:
            logger.error(f"Migration error for flip API columns: {e}")

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
                    scan_type TEXT DEFAULT 'all',
                    funding_arbitrage_enabled BOOLEAN DEFAULT 1,
                    selected_exchanges TEXT DEFAULT '["binance", "bybit", "okx", "whitebit", "mexc"]',
                    min_spread_threshold REAL DEFAULT 0.2,
                    alerts_enabled BOOLEAN DEFAULT 1,
                    inter_exchange_enabled BOOLEAN DEFAULT 1,
                    basis_arbitrage_enabled BOOLEAN DEFAULT 1,
                    auto_trade_mode BOOLEAN DEFAULT 0,
                    trade_amount REAL DEFAULT 100.0,
                    leverage INTEGER DEFAULT 3,
                    notifications_enabled BOOLEAN DEFAULT 1,
                    total_trades INTEGER DEFAULT 0,
                    successful_trades INTEGER DEFAULT 0,
                    failed_trades INTEGER DEFAULT 0,
                    total_profit REAL DEFAULT 0.0,
                    bot_blocked BOOLEAN DEFAULT 0,
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

                CREATE TABLE IF NOT EXISTS flip_settings (
                    user_id INTEGER PRIMARY KEY,
                    enabled BOOLEAN DEFAULT 0,
                    selected_symbols TEXT DEFAULT '["BTC", "ETH", "SOL", "TAO", "ASTER", "BCH"]',
                    leverage INTEGER DEFAULT 200,
                    position_size_usd REAL DEFAULT 100.0,
                    max_daily_flips INTEGER DEFAULT 300,
                    max_daily_loss_usd REAL DEFAULT 50.0,
                    min_price_movement_pct REAL DEFAULT 0.01,
                    close_on_reverse BOOLEAN DEFAULT 1,
                    test_mode BOOLEAN DEFAULT 1,
                    mexc_api_key TEXT DEFAULT '',
                    mexc_api_secret TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS flip_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    symbol TEXT,
                    direction TEXT DEFAULT 'long',
                    entry_price REAL,
                    exit_price REAL,
                    pnl_usd REAL DEFAULT 0,
                    pnl_percent REAL DEFAULT 0,
                    leverage INTEGER DEFAULT 200,
                    position_size_usd REAL DEFAULT 100,
                    quantity REAL DEFAULT 0,
                    status TEXT DEFAULT 'open',
                    close_reason TEXT DEFAULT '',
                    binance_entry_price REAL DEFAULT 0,
                    binance_exit_price REAL DEFAULT 0,
                    opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    duration_ms INTEGER DEFAULT 0,
                    metadata TEXT DEFAULT '{}',
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_spread_history_time ON spread_history(timestamp);
                CREATE INDEX IF NOT EXISTS idx_flip_trades_user ON flip_trades(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_flip_trades_time ON flip_trades(opened_at);
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

                # Direct column access (more efficient than dict(row))
                selected_exchanges = json.loads(row['selected_exchanges']) if row['selected_exchanges'] else ['binance', 'bybit', 'okx', 'whitebit', 'mexc']
                return UserSettings(
                    user_id=row['user_id'],
                    is_trading_enabled=bool(row['is_trading_enabled']),
                    api_keys=self._key_enc.decrypt_dict(json.loads(row['api_keys'])),
                    commission_rates=json.loads(row['commission_rates']),
                    alert_settings=json.loads(row['alert_settings']),
                    risk_settings=json.loads(row['risk_settings']),
                    arbitrage_mode=row['arbitrage_mode'] if row['arbitrage_mode'] else 'all',
                    scan_type=row['scan_type'] if row['scan_type'] is not None else 'all',
                    funding_arbitrage_enabled=bool(row['funding_arbitrage_enabled']) if row['funding_arbitrage_enabled'] is not None else True,
                    selected_exchanges=selected_exchanges,
                    min_spread_threshold=row['min_spread_threshold'] if row['min_spread_threshold'] is not None else 0.2,
                    alerts_enabled=bool(row['alerts_enabled']) if row['alerts_enabled'] is not None else True,
                    inter_exchange_enabled=bool(row['inter_exchange_enabled']) if row['inter_exchange_enabled'] is not None else True,
                    basis_arbitrage_enabled=bool(row['basis_arbitrage_enabled']) if row['basis_arbitrage_enabled'] is not None else True,
                    auto_trade_mode=bool(row['auto_trade_mode']) if row['auto_trade_mode'] is not None else False,
                    trade_amount=row['trade_amount'] if row['trade_amount'] is not None else 100.0,
                    leverage=row['leverage'] if row['leverage'] is not None else 3,
                    notifications_enabled=bool(row['notifications_enabled']) if row['notifications_enabled'] is not None else True,
                    total_trades=row['total_trades'] if row['total_trades'] is not None else 0,
                    successful_trades=row['successful_trades'] if row['successful_trades'] is not None else 0,
                    failed_trades=row['failed_trades'] if row['failed_trades'] is not None else 0,
                    total_profit=row['total_profit'] if row['total_profit'] is not None else 0.0,
                    bot_blocked=bool(row['bot_blocked']) if row['bot_blocked'] is not None else False,
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )

    async def create_user(self, user_id: int) -> UserSettings:
        existing = await self.get_user(user_id)
        if existing:
            return existing

        user = UserSettings(user_id=user_id)

        async with self._query_lock:
            try:
                await self._conn.execute("""
                    INSERT INTO users (user_id, api_keys, commission_rates, alert_settings, risk_settings, arbitrage_mode, selected_exchanges,
                        min_spread_threshold, alerts_enabled, inter_exchange_enabled, basis_arbitrage_enabled,
                        auto_trade_mode, trade_amount, leverage, notifications_enabled,
                        total_trades, successful_trades, failed_trades, total_profit, bot_blocked,
                        scan_type, funding_arbitrage_enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id,
                    json.dumps(self._key_enc.encrypt_dict(user.api_keys)),
                    json.dumps(user.commission_rates),
                    json.dumps(user.alert_settings),
                    json.dumps(user.risk_settings),
                    user.arbitrage_mode,
                    json.dumps(user.selected_exchanges),
                    user.min_spread_threshold,
                    int(user.alerts_enabled),
                    int(user.inter_exchange_enabled),
                    int(user.basis_arbitrage_enabled),
                    int(user.auto_trade_mode),
                    user.trade_amount,
                    user.leverage,
                    int(user.notifications_enabled),
                    user.total_trades,
                    user.successful_trades,
                    user.failed_trades,
                    user.total_profit,
                    int(user.bot_blocked),
                    user.scan_type,
                    int(user.funding_arbitrage_enabled)
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
                    # Direct column access (more efficient than dict(row))
                    selected_exchanges = json.loads(row['selected_exchanges']) if row['selected_exchanges'] else ['binance', 'bybit', 'okx', 'whitebit', 'mexc']
                    users.append(UserSettings(
                        user_id=row['user_id'],
                        is_trading_enabled=bool(row['is_trading_enabled']),
                        api_keys=self._key_enc.decrypt_dict(json.loads(row['api_keys'])),
                        commission_rates=json.loads(row['commission_rates']),
                        alert_settings=json.loads(row['alert_settings']),
                        risk_settings=json.loads(row['risk_settings']),
                        arbitrage_mode=row['arbitrage_mode'] if row['arbitrage_mode'] else 'all',
                        selected_exchanges=selected_exchanges,
                        min_spread_threshold=row['min_spread_threshold'] if row['min_spread_threshold'] is not None else 0.2,
                        alerts_enabled=bool(row['alerts_enabled']) if row['alerts_enabled'] is not None else True,
                        inter_exchange_enabled=bool(row['inter_exchange_enabled']) if row['inter_exchange_enabled'] is not None else True,
                        basis_arbitrage_enabled=bool(row['basis_arbitrage_enabled']) if row['basis_arbitrage_enabled'] is not None else True,
                        auto_trade_mode=bool(row['auto_trade_mode']) if row['auto_trade_mode'] is not None else False,
                        trade_amount=row['trade_amount'] if row['trade_amount'] is not None else 100.0,
                        leverage=row['leverage'] if row['leverage'] is not None else 3,
                        notifications_enabled=bool(row['notifications_enabled']) if row['notifications_enabled'] is not None else True,
                        total_trades=row['total_trades'] if row['total_trades'] is not None else 0,
                        successful_trades=row['successful_trades'] if row['successful_trades'] is not None else 0,
                        failed_trades=row['failed_trades'] if row['failed_trades'] is not None else 0,
                        total_profit=row['total_profit'] if row['total_profit'] is not None else 0.0,
                        bot_blocked=bool(row['bot_blocked']) if row['bot_blocked'] is not None else False,
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
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
                    selected_exchanges = ?,
                    min_spread_threshold = ?,
                    alerts_enabled = ?,
                    inter_exchange_enabled = ?,
                    basis_arbitrage_enabled = ?,
                    auto_trade_mode = ?,
                    trade_amount = ?,
                    leverage = ?,
                    notifications_enabled = ?,
                    total_trades = ?,
                    successful_trades = ?,
                    failed_trades = ?,
                    total_profit = ?,
                    bot_blocked = ?,
                    scan_type = ?,
                    funding_arbitrage_enabled = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (
                int(user.is_trading_enabled),
                json.dumps(self._key_enc.encrypt_dict(user.api_keys)),
                json.dumps(user.commission_rates),
                json.dumps(user.alert_settings),
                json.dumps(user.risk_settings),
                getattr(user, 'arbitrage_mode', 'all'),
                json.dumps(getattr(user, 'selected_exchanges', ['binance', 'bybit', 'okx', 'whitebit', 'mexc'])),
                getattr(user, 'min_spread_threshold', 0.2),
                int(getattr(user, 'alerts_enabled', True)),
                int(getattr(user, 'inter_exchange_enabled', True)),
                int(getattr(user, 'basis_arbitrage_enabled', True)),
                int(getattr(user, 'auto_trade_mode', False)),
                getattr(user, 'trade_amount', 100.0),
                getattr(user, 'leverage', 3),
                int(getattr(user, 'notifications_enabled', True)),
                getattr(user, 'total_trades', 0),
                getattr(user, 'successful_trades', 0),
                getattr(user, 'failed_trades', 0),
                getattr(user, 'total_profit', 0.0),
                int(getattr(user, 'bot_blocked', False)),
                getattr(user, 'scan_type', 'all'),
                int(getattr(user, 'funding_arbitrage_enabled', True)),
                user.user_id
            ))
            await self._conn.commit()

    async def add_trade(self, trade: Trade) -> int:
        # Ensure all extended fields are in metadata
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
            cursor = await self._conn.execute("""
                INSERT INTO trades (user_id, symbol, strategy, long_exchange, short_exchange,
                                 entry_spread, size_usd, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.user_id, trade.symbol, trade.strategy, trade.long_exchange,
                trade.short_exchange, trade.entry_spread, trade.size_usd,
                trade.status, json.dumps(metadata)
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
                    (user_id, int(test_mode))
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
                async with self._conn.execute(query, (user_id, int(test_mode))) as cursor:
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
                    result = dict(row)
                    # Parse metadata JSON
                    metadata = {}
                    if isinstance(result.get('metadata'), str):
                        try:
                            metadata = json.loads(result['metadata'])
                        except (json.JSONDecodeError, TypeError):
                            metadata = {}
                    result['metadata'] = metadata
                    # Extract extended fields from metadata for Trade(**result) compatibility
                    result['position_size_long'] = metadata.get('position_size_long', 0)
                    result['position_size_short'] = metadata.get('position_size_short', 0)
                    result['closed_portion_percent'] = metadata.get('closed_portion_percent', 0)
                    result['partial_close_count'] = metadata.get('partial_close_count', 0)
                    result['entry_price_long'] = metadata.get('entry_price_long', 0)
                    result['entry_price_short'] = metadata.get('entry_price_short', 0)
                    result['current_price_long'] = metadata.get('current_price_long', 0)
                    result['current_price_short'] = metadata.get('current_price_short', 0)
                    result['stop_loss_price'] = metadata.get('stop_loss_price', 0)
                    result['take_profit_price'] = metadata.get('take_profit_price', 0)
                    result['breakeven_triggered'] = metadata.get('breakeven_triggered', False)
                    result['trailing_enabled'] = metadata.get('trailing_enabled', True)
                    result['trailing_stop_price'] = metadata.get('trailing_stop_price', 0)
                    result['emergency_stop_price'] = metadata.get('emergency_stop_price', 0)
                    result['pnl_percent'] = metadata.get('pnl_percent', 0)
                    return result
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

    async def _migrate_add_uid_flip_tables(self):
        """Миграция: создание таблиц для MEXC UID Flip Trading"""
        try:
            async with self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='uid_flip_settings'"
            ) as cursor:
                if not await cursor.fetchone():
                    await self._conn.executescript("""
                        CREATE TABLE IF NOT EXISTS uid_flip_settings (
                            user_id INTEGER PRIMARY KEY,
                            enabled BOOLEAN DEFAULT 0,
                            selected_symbols TEXT DEFAULT '["BTC", "ETH", "SOL"]',
                            leverage INTEGER DEFAULT 200,
                            position_size_usd REAL DEFAULT 100.0,
                            max_daily_flips INTEGER DEFAULT 300,
                            max_daily_loss_usd REAL DEFAULT 50.0,
                            min_price_movement_pct REAL DEFAULT 0.01,
                            test_mode BOOLEAN DEFAULT 1,
                            uid TEXT DEFAULT '',
                            web_token TEXT DEFAULT '',
                            cookies TEXT DEFAULT '',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (user_id) REFERENCES users(user_id)
                        );

                        CREATE TABLE IF NOT EXISTS uid_flip_trades (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            user_id INTEGER,
                            symbol TEXT,
                            direction TEXT DEFAULT 'long',
                            entry_price REAL,
                            exit_price REAL,
                            pnl_usd REAL DEFAULT 0,
                            pnl_percent REAL DEFAULT 0,
                            leverage INTEGER DEFAULT 200,
                            position_size_usd REAL DEFAULT 100,
                            quantity REAL DEFAULT 0,
                            status TEXT DEFAULT 'open',
                            close_reason TEXT DEFAULT '',
                            binance_entry_price REAL DEFAULT 0,
                            binance_exit_price REAL DEFAULT 0,
                            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            closed_at TIMESTAMP,
                            duration_ms INTEGER DEFAULT 0,
                            metadata TEXT DEFAULT '{}',
                            FOREIGN KEY (user_id) REFERENCES users(user_id)
                        );

                        CREATE INDEX IF NOT EXISTS idx_uid_flip_trades_user ON uid_flip_trades(user_id, status);
                        CREATE INDEX IF NOT EXISTS idx_uid_flip_trades_time ON uid_flip_trades(opened_at);
                    """)
                    await self._conn.commit()
                    logger.info("Migration: created uid_flip_settings and uid_flip_trades tables")
        except Exception as e:
            logger.error(f"Migration error for UID flip tables: {e}")

    async def get_uid_flip_settings(self, user_id: int):
        """Получить настройки UID flip trading пользователя"""
        if not self._initialized:
            await self.initialize()
        from services.mexc_uid_trader import UIDFlipSettings
        async with self._query_lock:
            async with self._conn.execute(
                'SELECT * FROM uid_flip_settings WHERE user_id = ?', (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return UIDFlipSettings(
                    user_id=row['user_id'],
                    enabled=bool(row['enabled']),
                    selected_symbols=json.loads(row['selected_symbols']) if row['selected_symbols'] else ['BTC', 'ETH', 'SOL'],
                    leverage=row['leverage'] if row['leverage'] is not None else 200,
                    position_size_usd=row['position_size_usd'] if row['position_size_usd'] is not None else 100.0,
                    max_daily_flips=row['max_daily_flips'] if row['max_daily_flips'] is not None else 300,
                    max_daily_loss_usd=row['max_daily_loss_usd'] if row['max_daily_loss_usd'] is not None else 50.0,
                    min_price_movement_pct=row['min_price_movement_pct'] if row['min_price_movement_pct'] is not None else 0.01,
                    test_mode=bool(row['test_mode']) if row['test_mode'] is not None else True,
                    uid=row['uid'] if row['uid'] else '',
                    web_token=row['web_token'] if row['web_token'] else '',
                    cookies=row['cookies'] if row['cookies'] else '',
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )

    async def create_uid_flip_settings(self, user_id: int):
        """Создать настройки UID flip trading по умолчанию"""
        existing = await self.get_uid_flip_settings(user_id)
        if existing:
            return existing
        from services.mexc_uid_trader import UIDFlipSettings
        settings = UIDFlipSettings(user_id=user_id)
        async with self._query_lock:
            try:
                await self._conn.execute("""
                    INSERT INTO uid_flip_settings (user_id, enabled, selected_symbols, leverage,
                        position_size_usd, max_daily_flips, max_daily_loss_usd,
                        min_price_movement_pct, test_mode, uid, web_token, cookies)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id, int(settings.enabled), json.dumps(settings.selected_symbols),
                    settings.leverage, settings.position_size_usd, settings.max_daily_flips,
                    settings.max_daily_loss_usd, settings.min_price_movement_pct,
                    int(settings.test_mode), settings.uid, settings.web_token, settings.cookies
                ))
                await self._conn.commit()
                logger.info(f"Created UID flip settings for user {user_id}")
            except aiosqlite.IntegrityError:
                logger.warning(f"UID flip settings for user {user_id} already exist")
                return await self.get_uid_flip_settings(user_id)
        return settings

    async def update_uid_flip_settings(self, settings):
        """Обновить настройки UID flip trading"""
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE uid_flip_settings SET
                    enabled = ?, selected_symbols = ?, leverage = ?,
                    position_size_usd = ?, max_daily_flips = ?,
                    max_daily_loss_usd = ?, min_price_movement_pct = ?,
                    test_mode = ?, uid = ?, web_token = ?, cookies = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (
                int(settings.enabled), json.dumps(settings.selected_symbols),
                settings.leverage, settings.position_size_usd, settings.max_daily_flips,
                settings.max_daily_loss_usd, settings.min_price_movement_pct,
                int(settings.test_mode), settings.uid, settings.web_token, settings.cookies,
                settings.user_id
            ))
            await self._conn.commit()

    async def add_uid_flip_trade(self, trade) -> int:
        """Добавить UID flip сделку"""
        from services.mexc_uid_trader import UIDFlipTrade
        async with self._query_lock:
            cursor = await self._conn.execute("""
                INSERT INTO uid_flip_trades (user_id, symbol, direction, entry_price,
                    leverage, position_size_usd, quantity, status,
                    binance_entry_price, opened_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.user_id, trade.symbol, trade.direction, trade.entry_price,
                trade.leverage, trade.position_size_usd, trade.quantity, trade.status,
                trade.binance_entry_price, trade.opened_at, json.dumps(trade.metadata)
            ))
            await self._conn.commit()
            return cursor.lastrowid

    async def close_uid_flip_trade(self, trade_id: int, exit_price: float, pnl_usd: float,
                                    pnl_percent: float, close_reason: str,
                                    binance_exit_price: float, duration_ms: int):
        """Закрыть UID flip сделку"""
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE uid_flip_trades SET
                    exit_price = ?, pnl_usd = ?, pnl_percent = ?,
                    status = 'closed', close_reason = ?,
                    binance_exit_price = ?, closed_at = CURRENT_TIMESTAMP,
                    duration_ms = ?
                WHERE id = ?
            """, (exit_price, pnl_usd, pnl_percent, close_reason,
                  binance_exit_price, duration_ms, trade_id))
            await self._conn.commit()

    async def get_open_uid_flip_trades(self, user_id: int):
        """Получить открытые UID flip сделки пользователя"""
        from services.mexc_uid_trader import UIDFlipTrade
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT * FROM uid_flip_trades WHERE user_id = ? AND status = 'open'",
                (user_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                trades = []
                for row in rows:
                    trades.append(UIDFlipTrade(
                        id=row['id'], user_id=row['user_id'], symbol=row['symbol'],
                        direction=row['direction'], entry_price=row['entry_price'],
                        exit_price=row['exit_price'], pnl_usd=row['pnl_usd'],
                        pnl_percent=row['pnl_percent'], leverage=row['leverage'],
                        position_size_usd=row['position_size_usd'], quantity=row['quantity'],
                        status=row['status'], close_reason=row['close_reason'],
                        binance_entry_price=row['binance_entry_price'],
                        binance_exit_price=row['binance_exit_price'],
                        opened_at=row['opened_at'], closed_at=row['closed_at'],
                        duration_ms=row['duration_ms'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    ))
                return trades

    async def get_uid_flip_trade_stats(self, user_id: int, since: str = None) -> dict:
        """Получить статистику UID flip trading за период"""
        async with self._query_lock:
            query = """SELECT COUNT(*) as count,
                        COALESCE(SUM(pnl_usd), 0) as total_pnl,
                        COALESCE(AVG(duration_ms), 0) as avg_duration,
                        COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 0) as wins,
                        COALESCE(SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END), 0) as losses
                    FROM uid_flip_trades WHERE user_id = ? AND status = 'closed'"""
            params = [user_id]
            if since:
                query += " AND opened_at >= ?"
                params.append(since)
            async with self._conn.execute(query, params) as cursor:
                row = await cursor.fetchone()
                return {
                    'total_trades': row['count'] or 0,
                    'total_pnl': row['total_pnl'] or 0,
                    'avg_duration_ms': row['avg_duration'] or 0,
                    'wins': row['wins'] or 0,
                    'losses': row['losses'] or 0,
                    'win_rate': (row['wins'] / row['count'] * 100) if row['count'] else 0
                }

    async def get_today_uid_flip_count(self, user_id: int) -> int:
        """Количество UID флипов сегодня"""
        today = datetime.now().strftime('%Y-%m-%d')
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT COUNT(*) as count FROM uid_flip_trades WHERE user_id = ? AND status = 'closed' AND opened_at >= ?",
                (user_id, today)
            ) as cursor:
                row = await cursor.fetchone()
                return row['count'] or 0

    async def get_today_uid_flip_pnl(self, user_id: int) -> float:
        """PnL UID флипов сегодня"""
        today = datetime.now().strftime('%Y-%m-%d')
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT COALESCE(SUM(pnl_usd), 0) as pnl FROM uid_flip_trades WHERE user_id = ? AND status = 'closed' AND opened_at >= ?",
                (user_id, today)
            ) as cursor:
                row = await cursor.fetchone()
                return row['pnl'] or 0

    async def get_flip_settings(self, user_id: int) -> Optional[FlipSettings]:
        """Получить настройки flip trading пользователя"""
        if not self._initialized:
            await self.initialize()
        async with self._query_lock:
            async with self._conn.execute(
                'SELECT * FROM flip_settings WHERE user_id = ?', (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return FlipSettings(
                    user_id=row['user_id'],
                    enabled=bool(row['enabled']),
                    selected_symbols=json.loads(row['selected_symbols']) if row['selected_symbols'] else ['BTC', 'ETH', 'SOL', 'TAO', 'ASTER', 'BCH'],
                    leverage=row['leverage'] if row['leverage'] is not None else 200,
                    position_size_usd=row['position_size_usd'] if row['position_size_usd'] is not None else 100.0,
                    max_daily_flips=row['max_daily_flips'] if row['max_daily_flips'] is not None else 300,
                    max_daily_loss_usd=row['max_daily_loss_usd'] if row['max_daily_loss_usd'] is not None else 50.0,
                    min_price_movement_pct=row['min_price_movement_pct'] if row['min_price_movement_pct'] is not None else 0.01,
                    close_on_reverse=bool(row['close_on_reverse']) if row['close_on_reverse'] is not None else True,
                    test_mode=bool(row['test_mode']) if row['test_mode'] is not None else True,
                    mexc_api_key=self._key_enc.decrypt(row['mexc_api_key']) if 'mexc_api_key' in row.keys() and row['mexc_api_key'] is not None else '',
                    mexc_api_secret=self._key_enc.decrypt(row['mexc_api_secret']) if 'mexc_api_secret' in row.keys() and row['mexc_api_secret'] is not None else '',
                    created_at=row['created_at'],
                    updated_at=row['updated_at']
                )

    async def create_flip_settings(self, user_id: int) -> FlipSettings:
        """Создать настройки flip trading по умолчанию"""
        existing = await self.get_flip_settings(user_id)
        if existing:
            return existing
        settings = FlipSettings(user_id=user_id)
        async with self._query_lock:
            try:
                await self._conn.execute("""
                    INSERT INTO flip_settings (user_id, enabled, selected_symbols, leverage,
                        position_size_usd, max_daily_flips, max_daily_loss_usd,
                        min_price_movement_pct, close_on_reverse, test_mode,
                        mexc_api_key, mexc_api_secret)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id, int(settings.enabled), json.dumps(settings.selected_symbols),
                    settings.leverage, settings.position_size_usd, settings.max_daily_flips,
                    settings.max_daily_loss_usd, settings.min_price_movement_pct,
                    int(settings.close_on_reverse), int(settings.test_mode),
                    self._key_enc.encrypt(settings.mexc_api_key), self._key_enc.encrypt(settings.mexc_api_secret)
                ))
                await self._conn.commit()
                logger.info(f"Created flip settings for user {user_id}")
            except aiosqlite.IntegrityError:
                logger.warning(f"Flip settings for user {user_id} already exist")
                return await self.get_flip_settings(user_id)
        return settings

    async def update_flip_settings(self, settings: FlipSettings):
        """Обновить настройки flip trading"""
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE flip_settings SET
                    enabled = ?, selected_symbols = ?, leverage = ?,
                    position_size_usd = ?, max_daily_flips = ?,
                    max_daily_loss_usd = ?, min_price_movement_pct = ?,
                    close_on_reverse = ?, test_mode = ?,
                    mexc_api_key = ?, mexc_api_secret = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (
                int(settings.enabled), json.dumps(settings.selected_symbols),
                settings.leverage, settings.position_size_usd, settings.max_daily_flips,
                settings.max_daily_loss_usd, settings.min_price_movement_pct,
                int(settings.close_on_reverse), int(settings.test_mode),
                self._key_enc.encrypt(settings.mexc_api_key), self._key_enc.encrypt(settings.mexc_api_secret),
                settings.user_id
            ))
            await self._conn.commit()

    async def add_flip_trade(self, trade: FlipTrade) -> int:
        """Добавить flip сделку"""
        async with self._query_lock:
            cursor = await self._conn.execute("""
                INSERT INTO flip_trades (user_id, symbol, direction, entry_price,
                    leverage, position_size_usd, quantity, status,
                    binance_entry_price, opened_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade.user_id, trade.symbol, trade.direction, trade.entry_price,
                trade.leverage, trade.position_size_usd, trade.quantity, trade.status,
                trade.binance_entry_price, trade.opened_at, json.dumps(trade.metadata)
            ))
            await self._conn.commit()
            return cursor.lastrowid

    async def close_flip_trade(self, trade_id: int, exit_price: float, pnl_usd: float,
                                pnl_percent: float, close_reason: str,
                                binance_exit_price: float, duration_ms: int):
        """Закрыть flip сделку"""
        async with self._query_lock:
            await self._conn.execute("""
                UPDATE flip_trades SET
                    exit_price = ?, pnl_usd = ?, pnl_percent = ?,
                    status = 'closed', close_reason = ?,
                    binance_exit_price = ?, closed_at = CURRENT_TIMESTAMP,
                    duration_ms = ?
                WHERE id = ?
            """, (exit_price, pnl_usd, pnl_percent, close_reason,
                  binance_exit_price, duration_ms, trade_id))
            await self._conn.commit()

    async def get_open_flip_trades(self, user_id: int) -> List[FlipTrade]:
        """Получить открытые flip сделки пользователя"""
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT * FROM flip_trades WHERE user_id = ? AND status = 'open'",
                (user_id,)
            ) as cursor:
                rows = await cursor.fetchall()
                trades = []
                for row in rows:
                    trades.append(FlipTrade(
                        id=row['id'], user_id=row['user_id'], symbol=row['symbol'],
                        direction=row['direction'], entry_price=row['entry_price'],
                        exit_price=row['exit_price'], pnl_usd=row['pnl_usd'],
                        pnl_percent=row['pnl_percent'], leverage=row['leverage'],
                        position_size_usd=row['position_size_usd'], quantity=row['quantity'],
                        status=row['status'], close_reason=row['close_reason'],
                        binance_entry_price=row['binance_entry_price'],
                        binance_exit_price=row['binance_exit_price'],
                        opened_at=row['opened_at'], closed_at=row['closed_at'],
                        duration_ms=row['duration_ms'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    ))
                return trades

    async def get_flip_trade_stats(self, user_id: int, since: str = None) -> dict:
        """Получить статистику flip trading за период"""
        async with self._query_lock:
            query = """SELECT COUNT(*) as count,
                        COALESCE(SUM(pnl_usd), 0) as total_pnl,
                        COALESCE(AVG(duration_ms), 0) as avg_duration,
                        COALESCE(SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END), 0) as wins,
                        COALESCE(SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END), 0) as losses
                    FROM flip_trades WHERE user_id = ? AND status = 'closed'"""
            params = [user_id]
            if since:
                query += " AND opened_at >= ?"
                params.append(since)
            async with self._conn.execute(query, params) as cursor:
                row = await cursor.fetchone()
                return {
                    'total_trades': row['count'] or 0,
                    'total_pnl': row['total_pnl'] or 0,
                    'avg_duration_ms': row['avg_duration'] or 0,
                    'wins': row['wins'] or 0,
                    'losses': row['losses'] or 0,
                    'win_rate': (row['wins'] / row['count'] * 100) if row['count'] else 0
                }

    async def get_today_flip_count(self, user_id: int) -> int:
        """Количество флипов сегодня"""
        today = datetime.now().strftime('%Y-%m-%d')
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT COUNT(*) as count FROM flip_trades WHERE user_id = ? AND status = 'closed' AND opened_at >= ?",
                (user_id, today)
            ) as cursor:
                row = await cursor.fetchone()
                return row['count'] or 0

    async def get_today_flip_pnl(self, user_id: int) -> float:
        """PnL флипов сегодня"""
        today = datetime.now().strftime('%Y-%m-%d')
        async with self._query_lock:
            async with self._conn.execute(
                "SELECT COALESCE(SUM(pnl_usd), 0) as pnl FROM flip_trades WHERE user_id = ? AND status = 'closed' AND opened_at >= ?",
                (user_id, today)
            ) as cursor:
                row = await cursor.fetchone()
                return row['pnl'] or 0
