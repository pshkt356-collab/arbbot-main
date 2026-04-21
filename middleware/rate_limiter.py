import asyncio
import time
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
import logging

logger = logging.getLogger(__name__)

class UserRateLimiter(BaseMiddleware):
    """Rate limiting и защита от double-submit"""
    
    def __init__(self, max_requests: int = 10, window: int = 60):
        super().__init__()
        self.max_requests = max_requests
        self.window = window
        self.requests: Dict[int, list] = {}
        self.locks: Dict[int, asyncio.Lock] = {}
        self.cooldowns: Dict[str, float] = {}
        self._cleanup_interval = 300  # 5 minutes
        self._last_cleanup = time.time()
        
    def _cleanup_old_entries(self):
        """Очистка старых записей пользователей"""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return
        self._last_cleanup = now
        cutoff = now - 600  # 10 minutes
        expired_users = [uid for uid, times in self.requests.items()
                         if not times or max(times) < cutoff]
        for uid in expired_users:
            del self.requests[uid]
            self.locks.pop(uid, None)
        if expired_users:
            logger.debug(f"RateLimiter: cleaned up {len(expired_users)} expired entries")

    def _get_lock(self, user_id: int) -> asyncio.Lock:
        if user_id not in self.locks:
            self.locks[user_id] = asyncio.Lock()
        return self.locks[user_id]
    
    def _check_rate_limit(self, user_id: int) -> bool:
        now = time.time()
        if user_id not in self.requests:
            self.requests[user_id] = []
        
        self.requests[user_id] = [t for t in self.requests[user_id] if now - t < self.window]
        
        if len(self.requests[user_id]) >= self.max_requests:
            return False
        
        self.requests[user_id].append(time.time())
        return True
    
    def _check_cooldown(self, key: str, cooldown: float = 2.0) -> bool:
        now = time.time()
        if key in self.cooldowns:
            if now - self.cooldowns[key] < cooldown:
                return False
        self.cooldowns[key] = now
        return True
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        if isinstance(event, Message):
            user_id = event.from_user.id
            action_key = f"msg_{user_id}_{event.text or event.content_type}"
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id
            action_key = f"cb_{user_id}_{event.data}"
        else:
            return await handler(event, data)
        
        self._cleanup_old_entries()

        if not self._check_rate_limit(user_id):
            logger.warning(f"Rate limit exceeded for user {user_id}")
            if isinstance(event, Message):
                await event.answer("⚠️ Слишком много запросов. Подождите минуту.")
            elif isinstance(event, CallbackQuery):
                await event.answer("⚠️ Слишком быстро!", show_alert=True)
            return None
        
        if isinstance(event, CallbackQuery):
            if not self._check_cooldown(action_key, cooldown=3.0):
                logger.info(f"Double-submit blocked for user {user_id}")
                await event.answer("⏳ Подождите завершения операции...", show_alert=True)
                return None
        
        if isinstance(event, CallbackQuery) and event.data:
            if any(x in event.data for x in ['trade:open', 'trade:close', 'partial:', 'trailing:toggle']):
                lock = self._get_lock(user_id)
                if lock.locked():
                    await event.answer("⏳ Операция уже выполняется...", show_alert=True)
                    return None
                
                async with lock:
                    return await handler(event, data)
        
        return await handler(event, data)

class DoubleSubmitProtection(BaseMiddleware):
    """Простая защита от повторных нажатий"""
    
    def __init__(self, cooldown: float = 2.0):
        super().__init__()
        self.cooldown = cooldown
        self.last_click: Dict[str, float] = {}
    
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        
        if isinstance(event, CallbackQuery):
            key = f"{event.from_user.id}_{event.data}"
            now = time.time()
            
            if key in self.last_click:
                if now - self.last_click[key] < self.cooldown:
                    await event.answer("⏳ Пожалуйста, подождите...", show_alert=True)
                    return None
            
            self.last_click[key] = now
        
        return await handler(event, data)
