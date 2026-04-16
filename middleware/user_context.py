import logging
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from database.models import Database, UserSettings

logger = logging.getLogger(__name__)

class UserContextMiddleware(BaseMiddleware):
    """Middleware для добавления user и db в хендлеры"""

    def __init__(self, scanner=None, db=None):
        super().__init__()
        self.scanner = scanner
        self._db = db  # Можно передать из main.py

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Получаем user_id в зависимости от типа события
        if isinstance(event, Message):
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id
        else:
            user_id = getattr(event, 'from_user', None)
            if user_id:
                user_id = user_id.id

        if not user_id:
            logger.warning("No user_id in event")
            return await handler(event, data)

        # Инициализация БД
        try:
            # Используем переданный экземпляр или создаем новый
            db = self._db
            if db is None:
                db = Database()
            
            # Проверяем что db не None и инициализируем если нужно
            if db is None:
                raise Exception("Failed to create Database instance")
                
            if not db._initialized:
                await db.initialize()

            # Получаем или создаем пользователя
            user = await db.get_user(user_id)
            if not user:
                logger.info(f"Creating new user: {user_id}")
                user = await db.create_user(user_id)

            # Добавляем в data для хендлеров
            data['user'] = user
            data['db'] = db
            data['scanner'] = self.scanner

        except Exception as e:
            logger.error(f"Middleware error for user {user_id}: {e}")
            # В случае ошибки продолжаем без user, но логируем
            data['user'] = None
            data['db'] = None
            data['scanner'] = self.scanner

        try:
            # Вызываем хендлер
            return await handler(event, data)
        except Exception as e:
            logger.error(f"Handler error: {e}", exc_info=True)
            # Если это сообщение или callback, отправляем ошибку пользователю
            if isinstance(event, Message):
                await event.answer("❌ Произошла ошибка. Попробуйте /start")
            elif isinstance(event, CallbackQuery):
                await event.answer("Ошибка обработки", show_alert=True)
            raise  # Перебрасываем для полного лога

class ScannerMiddleware(BaseMiddleware):
    """Отдельный middleware только для передачи scanner"""

    def __init__(self, scanner):
        super().__init__()
        self.scanner = scanner

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        data['scanner'] = self.scanner
        return await handler(event, data)
