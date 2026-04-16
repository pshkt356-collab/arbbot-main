import asyncio
import logging
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta

from database.models import Database, Trade
from services.trading_engine import trading_engine

logger = logging.getLogger(__name__)

@dataclass
class RiskCheck:
    trade_id: int
    user_id: int
    current_spread: float
    entry_spread: float
    unrealized_pnl: float
    funding_pnl: float
    time_open: timedelta
    recommendation: str

class RiskManager:
    def __init__(self, check_interval: int = 30):
        self.db = Database('arbitrage_bot.db')
        self.check_interval = check_interval
        self.running = False
        self.user_risk_settings: Dict[int, dict] = {}

    async def start_monitoring(self):
        self.running = True
        logger.info("Risk manager started")

        while self.running:
            try:
                await self._check_all_positions()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Risk check error: {e}")
                await asyncio.sleep(5)

    async def _check_all_positions(self):
        """Проверка всех открытых позиций"""
        try:
            await self.db.initialize()
            # Получаем все открытые сделки из всех пользователей
            # Упрощенная реализация - в реальности нужен метод get_all_open_trades
            pass
        except Exception as e:
            logger.error(f"Error checking positions: {e}")

    def stop(self):
        self.running = False

risk_manager = RiskManager()
