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
        from config import settings
        self.db = Database(settings.db_file)
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
            async with self.db._conn.execute(
                "SELECT DISTINCT user_id FROM trades WHERE status = 'open'"
            ) as cursor:
                rows = await cursor.fetchall()
            for row in rows:
                await self._check_user_positions(row['user_id'])
        except Exception as e:
            logger.error(f"Error checking positions: {e}")

    async def _check_user_positions(self, user_id: int):
        """Проверка позиций конкретного пользователя"""
        try:
            user = await self.db.get_user(user_id)
            if not user:
                return
            open_trades = await self.db.get_open_trades(user_id)
            if not open_trades:
                return
            total_pnl = sum((t.pnl_usd or 0) for t in open_trades)
            daily_loss_limit = user.risk_settings.get('max_daily_loss_usd', 500)
            if total_pnl <= -daily_loss_limit:
                logger.warning(f"RiskManager: Daily loss limit reached for user {user_id}: ${total_pnl:.2f}")
            max_positions = user.risk_settings.get('max_open_positions', 5)
            if len(open_trades) >= max_positions:
                logger.warning(f"RiskManager: Max positions reached for user {user_id}: {len(open_trades)}/{max_positions}")
            symbol_counts = {}
            for t in open_trades:
                symbol_counts[t.symbol] = symbol_counts.get(t.symbol, 0) + 1
            for symbol, count in symbol_counts.items():
                if count >= 3:
                    logger.warning(f"RiskManager: Concentration risk for {symbol}: {count} positions")
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            max_hours = user.risk_settings.get('max_position_hours', 24)
            for trade in open_trades:
                try:
                    opened_at = datetime.fromisoformat(trade.opened_at.replace('Z', '+00:00'))
                    hours_open = (now - opened_at).total_seconds() / 3600
                    if hours_open > max_hours:
                        logger.warning(f"RiskManager: Position #{trade.id} open for {hours_open:.1f}h (limit: {max_hours}h)")
                except (ValueError, TypeError):
                    pass
        except Exception as e:
            logger.error(f"Error checking user {user_id} positions: {e}")

    def stop(self):
        self.running = False

risk_manager = RiskManager()
