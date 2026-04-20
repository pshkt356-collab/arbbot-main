from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # Telegram
    telegram_bot_token: str
    telegram_admin_id: Optional[int] = None

    # Trading
    auto_trading_default: bool = False
    balance_usage_percent: float = Field(95.0, ge=1, le=100)
    min_spread_auto: float = 0.5
    spread_ttl_seconds: int = 120
    test_mode_default: bool = True

    # Funding settings
    max_funding_hours: float = 2.0
    min_funding_diff_percent: float = 1.0

    # Risk
    max_open_positions: int = 5
    daily_loss_limit: float = 500
    breakeven_close: bool = True
    pnl_check_interval: int = 30
    pnl_notification_threshold: float = 5.0

    # Scanner
    scan_interval: int = 5
    price_cache_ttl: int = 10
    min_volume_24h: float = 500000

    # Database - Railway-compatible path
    db_file: str = "/app/data/arbitrage_bot.db"
    
    # FSM Storage - SQLite database path for FSM state persistence
    fsm_storage_path: str = "/app/data/fsm_storage.db"

    # Logging
    log_level: str = "INFO"
    
    # Proxy (опционально, безопасно - из env)
    use_proxy: bool = False
    proxy_url: Optional[str] = None
    binance_proxy_url: Optional[str] = None

    # MEXC API (для Flip Trading)
    mexc_api_key: Optional[str] = None
    mexc_api_secret: Optional[str] = None
    
    # MEXC Flip Trading defaults
    flip_leverage_default: int = 200  # Плечо по умолчанию (50-300)
    flip_position_size_default: float = 100.0  # Размер позиции в USDT
    flip_max_daily_flips: int = 300  # Максимум сделок в день
    flip_max_daily_loss_usd: float = 50.0  # Макс дневной убыток
    flip_min_price_movement_pct: float = 0.01  # Минимальное движение цены % для входа
    flip_close_on_reverse: bool = True  # Закрывать при развороте
    
    # WebSocket endpoints (не менять через env, встроенные)
    binance_ws_futures: str = "wss://fstream.binance.com/stream?streams=!ticker@arr/!markPrice@arr"
    mexc_ws_futures: str = "wss://contract.mexc.com/ws"
    
    # Flip Trading execution
    flip_price_history_window: int = 20  # Количество тиков для определения направления
    flip_tick_interval_ms: int = 100  # Интервал между тиками (мс)
    flip_order_timeout_ms: int = 5000  # Таймаут на исполнение ордера

settings = Settings()
