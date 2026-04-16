import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import html

from aiogram import Bot
from config import settings

logger = logging.getLogger(__name__)

@dataclass
class Alert:
    level: str
    message: str
    timestamp: datetime
    source: str
    correlation_id: Optional[str] = None

class CriticalAlertManager:
    def __init__(self, bot: Bot, admin_id: int, 
                 critical_cooldown: int = 300,
                 warning_cooldown: int = 600):
        self.bot = bot
        self.admin_id = admin_id
        self.critical_cooldown = critical_cooldown
        self.warning_cooldown = warning_cooldown
        
        self._last_alert: Dict[str, datetime] = {}
        self._alert_history: List[Alert] = []
        self._max_history = 100
        self._lock = asyncio.Lock()
        
    def _get_alert_key(self, level: str, source: str) -> str:
        return f"{level}:{source}"
    
    async def can_send(self, level: str, source: str) -> bool:
        key = self._get_alert_key(level, source)
        now = datetime.now()
        
        if key in self._last_alert:
            elapsed = (now - self._last_alert[key]).total_seconds()
            if level == 'critical' and elapsed < self.critical_cooldown:
                return False
            if level == 'warning' and elapsed < self.warning_cooldown:
                return False
        
        return True
    
    async def send(self, level: str, message: str, source: str = "system", 
                   correlation_id: Optional[str] = None) -> bool:
        async with self._lock:
            if not await self.can_send(level, source):
                logger.debug(f"Alert suppressed ({level}/{source}): {message[:50]}")
                return False
            
            alert = Alert(level, message, datetime.now(), source, correlation_id)
            self._alert_history.append(alert)
            if len(self._alert_history) > self._max_history:
                self._alert_history.pop(0)
            
            self._last_alert[self._get_alert_key(level, source)] = datetime.now()
        
        emoji = {"critical": "🚨", "warning": "⚠️", "info": "ℹ️"}.get(level, "📢")
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        safe_message = html.escape(message)
        safe_source = html.escape(source)
        
        formatted = f"{emoji} <b>{level.upper()} ALERT</b> | {timestamp}\\n<b>Source:</b> {safe_source}\\n\\n{safe_message}"
        
        if correlation_id:
            formatted += f"\\n\\n<code>ID: {correlation_id}</code>"
        
        for attempt in range(3):
            try:
                await self.bot.send_message(
                    self.admin_id, 
                    formatted, 
                    parse_mode='HTML',
                    disable_notification=(level != 'critical')
                )
                logger.info(f"Alert sent ({level}): {message[:50]}")
                return True
            except Exception as e:
                logger.error(f"Alert send attempt {attempt+1} failed: {e}")
                await asyncio.sleep(1)
        
        return False
    
    async def critical(self, message: str, source: str = "system", 
                       correlation_id: Optional[str] = None):
        await self.send('critical', message, source, correlation_id)
    
    async def warning(self, message: str, source: str = "system",
                      correlation_id: Optional[str] = None):
        await self.send('warning', message, source, correlation_id)
    
    async def info(self, message: str, source: str = "system",
                   correlation_id: Optional[str] = None):
        emoji = "ℹ️"
        timestamp = datetime.now().strftime("%H:%M:%S")
        safe_message = html.escape(message)
        
        formatted = f"{emoji} <b>INFO</b> | {timestamp}\\n\\n{safe_message}"
        
        try:
            await self.bot.send_message(self.admin_id, formatted, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Info alert failed: {e}")
    
    def get_recent_alerts(self, level: Optional[str] = None, 
                         minutes: int = 60) -> List[Alert]:
        cutoff = datetime.now() - timedelta(minutes=minutes)
        alerts = [a for a in self._alert_history if a.timestamp > cutoff]
        
        if level:
            alerts = [a for a in alerts if a.level == level]
        
        return alerts

alert_manager: Optional[CriticalAlertManager] = None

def init_alert_manager(bot: Bot, admin_id: int):
    global alert_manager
    alert_manager = CriticalAlertManager(bot, admin_id)
    logger.info(f"Alert manager initialized for admin {admin_id}")
