import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)

@dataclass
class ExchangeHealth:
    name: str
    last_ping: float = 0
    last_data: float = 0
    reconnects: int = 0
    errors: List[str] = field(default_factory=list)
    messages_received: int = 0
    messages_sent: int = 0
    latency_ms: float = 0
    status: str = "unknown"  # connected, disconnected, error, stale
    
    @property
    def is_healthy(self) -> bool:
        now = time.time()
        # Если данные не приходили более 60 секунд - проблема
        if now - self.last_data > 60:
            return False
        return self.status == "connected"
    
    @property
    def data_age_sec(self) -> float:
        return time.time() - self.last_data
    
    def log_error(self, error: str):
        self.errors.append(f"{datetime.now().strftime('%H:%M:%S')}: {error}")
        if len(self.errors) > 50:  # Храним последние 50 ошибок
            self.errors.pop(0)

class ExchangeDiagnostics:
    def __init__(self):
        self.health: Dict[str, ExchangeHealth] = {}
        self._lock = asyncio.Lock()
        self._last_report = 0
        
    async def register_exchange(self, name: str):
        async with self._lock:
            self.health[name] = ExchangeHealth(name=name)
            logger.info(f"📊 Diagnostics registered: {name}")
    
    async def update_ping(self, name: str):
        async with self._lock:
            if name in self.health:
                self.health[name].last_ping = time.time()
    
    async def update_data(self, name: str, latency_ms: Optional[float] = None):
        async with self._lock:
            if name in self.health:
                h = self.health[name]
                h.last_data = time.time()
                h.messages_received += 1
                h.status = "connected"
                if latency_ms:
                    h.latency_ms = latency_ms
    
    async def record_reconnect(self, name: str):
        async with self._lock:
            if name in self.health:
                self.health[name].reconnects += 1
    
    async def record_error(self, name: str, error: str):
        async with self._lock:
            if name in self.health:
                self.health[name].log_error(error)
                self.health[name].status = "error"
    
    async def set_disconnected(self, name: str):
        async with self._lock:
            if name in self.health:
                self.health[name].status = "disconnected"
    
    def get_status_report(self) -> str:
        """HTML отчет для Telegram"""
        lines = ["📊 <b>Статус бирж</b>\n"]
        
        for name, h in sorted(self.health.items()):
            status_emoji = {
                "connected": "🟢",
                "disconnected": "🔴", 
                "error": "⚠️",
                "stale": "🟡",
                "unknown": "⚪"
            }.get(h.status, "⚪")
            
            age = h.data_age_sec
            age_str = f"{age:.0f}с" if age < 60 else f"{age/60:.1f}м"
            
            lines.append(
                f"{status_emoji} <b>{name.upper()}</b>\n"
                f"   Данные: {age_str} назад\n"
                f"   Сообщений: {h.messages_received}\n"
                f"   Переподключений: {h.reconnects}\n"
                f"   Задержка: {h.latency_ms:.0f}мс\n"
            )
            
            if h.errors and len(h.errors) > 0:
                lines.append(f"   ⚠️ Ошибок: {len(h.errors)} (последние 3):\n")
                for err in h.errors[-3:]:
                    lines.append(f"   • {err}\n")
        
        return "".join(lines)
    
    def get_detailed_log(self) -> str:
        """Подробный текстовый лог для файла"""
        lines = [f"=== Exchange Diagnostics {datetime.now()} ===\n"]
        
        for name, h in sorted(self.health.items()):
            lines.append(f"\n--- {name.upper()} ---")
            lines.append(f"Status: {h.status}")
            lines.append(f"Last data: {h.data_age_sec:.1f}s ago")
            lines.append(f"Messages: {h.messages_received}")
            lines.append(f"Reconnects: {h.reconnects}")
            lines.append(f"Latency: {h.latency_ms:.1f}ms")
            
            if h.errors:
                lines.append(f"Recent errors ({len(h.errors)} total):")
                for err in h.errors[-5:]:
                    lines.append(f"  - {err}")
        
        return "\n".join(lines)

# Глобальный инстанс
diagnostics = ExchangeDiagnostics()
