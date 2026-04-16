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

    # Database
    db_file: str = "/app/data/arbitrage_bot.db"

    # Logging
    log_level: str = "INFO"
    
    # Proxy (опционально, безопасно - из env)
    use_proxy: bool = False
    proxy_url: Optional[str] = None
    binance_proxy_url: Optional[str] = None

settings = Settings()
