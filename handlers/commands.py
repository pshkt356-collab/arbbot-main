# -*- coding: utf-8 -*-
"""
Command handlers for Telegram bot - FINAL FIX v4
Исправлено:
1. /balance теперь получает реальные данные с бирж
2. Правильное обращение к полям баланса (используем _cached_balances)
"""
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
import logging
import html

from database.models import UserSettings, Database
from handlers.states import SetupStates

logger = logging.getLogger(__name__)
commands_router = Router()

# Доступные биржи
AVAILABLE_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc', 'whitebit']

def validate_api_key(key: str) -> bool:
    return len(key) >= 10 if key else False

def validate_api_secret(secret: str) -> bool:
    return len(secret) >= 10 if secret else False

def escape_html(text: str) -> str:
    return html.escape(str(text)) if text else ""

# ==================== START COMMAND ====================

@commands_router.message(Command("start"))
async def cmd_start(message: Message, user: UserSettings):
    """Обработка команды /start"""
    # Импортируем здесь чтобы избежать circular import
    from handlers.callbacks import show_main_menu

    # Создаем фейковый callback для совместимости
    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = FakeMessage(msg)
        async def answer(self, **kwargs):
            pass

    class FakeMessage:
        def __init__(self, msg):
            self._msg = msg
        async def edit_text(self, text, **kwargs):
            # Если нельзя редактировать, отправляем новое сообщение
            await self._msg.answer(text, **kwargs)
        async def answer(self, text, **kwargs):
            await self._msg.answer(text, **kwargs)

    fake_callback = FakeCallback(message)
    await show_main_menu(fake_callback, user)

@commands_router.message(Command("help"))
async def cmd_help(message: Message):
    """Справка"""
    text = (
        "<b>📚 Команды бота:</b>\\n\\n"
        
        "/start — Главное меню\\n"
        "/help — Эта справка\\n"
        "/settings — Настройки алертов\\n"
        "/balance — Твой баланс\\n"
        "/testapi — Проверка API ключей\\n"
        "/exchanges — Управление биржами\\n\\n"
        
        "<b>🔥 Основные функции:</b>\\n"
        "• Авто-торговля спредами\\n"
        "• Алерты на межбиржевой и базисный арбитраж\\n"
        "• Мониторинг позиций в реальном времени\\n\\n"
        
        "Вопросы? Пиши в поддержку."
    )
    await message.answer(text, parse_mode="HTML")

@commands_router.message(Command("menu"))
async def cmd_menu(message: Message, user: UserSettings):
    """Команда /menu"""
    from handlers.callbacks import show_main_menu

    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = FakeMessage(msg)
        async def answer(self, **kwargs):
            pass

    class FakeMessage:
        def __init__(self, msg):
            self._msg = msg
        async def edit_text(self, text, **kwargs):
            await self._msg.answer(text, **kwargs)
        async def answer(self, text, **kwargs):
            await self._msg.answer(text, **kwargs)

    fake_callback = FakeCallback(message)
    await show_main_menu(fake_callback, user)

@commands_router.message(Command("profile"))
async def cmd_profile(message: Message, user: UserSettings, db: Database = None):
    """Команда /profile"""
    from handlers.callbacks import show_profile_menu

    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = msg
        async def answer(self, **kwargs):
            pass

    fake_callback = FakeCallback(message)
    await show_profile_menu(fake_callback, user, db)

@commands_router.message(Command("settings"))
async def cmd_settings(message: Message, user: UserSettings):
    """Быстрые настройки"""
    from handlers.callbacks import show_alert_settings

    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = msg
        async def answer(self, **kwargs):
            pass

    fake_callback = FakeCallback(message)
    await show_alert_settings(fake_callback, user)

@commands_router.message(Command("exchanges"))
async def cmd_exchanges(message: Message, user: UserSettings, state: FSMContext):
    """Управление биржами"""
    from handlers.callbacks import show_exchanges

    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = msg
        async def answer(self, **kwargs):
            pass

    fake_callback = FakeCallback(message)
    await show_exchanges(fake_callback, user, state)

@commands_router.message(Command("balance"))
async def cmd_balance(message: Message, user: UserSettings, db: Database = None):
    """Показать баланс с обновлением с бирж"""
    # ИСПРАВЛЕНО: Получаем актуальные балансы с бирж
    total_balance = 0.0
    available_balance = 0.0
    locked_balance = 0.0
    exchange_balances = []

    if user.api_keys:
        from services.trading_engine import trading_engine
        for exchange_id, api_data in user.api_keys.items():
            if not api_data.get('api_key'):
                continue
            try:
                result = await trading_engine.test_api_connection(
                    exchange_id,
                    api_data['api_key'],
                    api_data.get('api_secret', ''),
                    api_data.get('testnet', True)
                )
                if result.get('success'):
                    bal = result.get('balance_usdt', 0)
                    total_balance += bal
                    available_balance += bal
                    exchange_balances.append((exchange_id, bal))
                    user.update_exchange_balance(exchange_id, total=bal, free=bal, used=0)
                else:
                    exchange_balances.append((exchange_id, None))
            except Exception as e:
                logger.warning(f"Balance fetch error for {exchange_id}: {e}")
                exchange_balances.append((exchange_id, None))

    # Если не удалось получить с бирж, используем кешированные значения
    if total_balance == 0 and hasattr(user, '_cached_balances') and user._cached_balances:
        total_balance = sum(b.get('total', 0) for b in user._cached_balances.values() if isinstance(b, dict))
        available_balance = sum(b.get('free', 0) for b in user._cached_balances.values() if isinstance(b, dict))
        locked_balance = sum(b.get('used', 0) for b in user._cached_balances.values() if isinstance(b, dict))

    # Сохраняем обновленные балансы в БД
    if db and total_balance > 0:
        try:
            await db.update_user(user)
        except Exception as e:
            logger.warning(f"Failed to save balances to DB: {e}")

    # Формируем текст
    text = (
        f"<b>💰 Твой баланс</b>\\n\\n"
        f"📊 <b>Общий:</b> {total_balance:.2f} USDT\\n"
        f"💵 <b>Доступно:</b> {available_balance:.2f} USDT\\n"
        f"🔒 <b>В сделках:</b> {locked_balance:.2f} USDT\\n\\n"
    )

    if exchange_balances:
        text += "<b>По биржам:</b>\\n"
        for ex, bal in exchange_balances:
            if bal is not None:
                text += f"• {ex.upper()}: {bal:.2f} USDT\\n"
            else:
                text += f"• {ex.upper()}: ❌ ошибка\\n"
    else:
        text += "<i>Нет подключенных бирж. Добавь API в Профиле.</i>"

    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardBuilder().button(text="🔄 Обновить", callback_data="profile:balance"),
        InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main")
    )
    await message.answer(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

@commands_router.message(Command("stop"))
async def cmd_stop(message: Message):
    """Остановка бота"""
    text = (
        "🛑 <b>Бот остановлен</b>\\n\\n"
        "Чтобы перезапустить, отправь /start"
    )
    await message.answer(text, parse_mode="HTML")

@commands_router.message(Command("testapi"))
async def cmd_test_api(message: Message, user: UserSettings):
    """Проверка API ключей"""
    if not user.api_keys:
        await message.answer(
            "❌ <b>API ключи не настроены</b>\\n\\n"
            "Перейди в Профиль → Мои биржи для настройки."
        )
        return

    text = "🔌 <b>Проверка API ключей:</b>\\n\\n"
    from services.trading_engine import trading_engine

    for exchange_id, api_data in user.api_keys.items():
        if not api_data.get('api_key'):
            continue

        try:
            # ИСПРАВЛЕНО: Используем test_api_connection
            result = await trading_engine.test_api_connection(
                exchange_id,
                api_data['api_key'],
                api_data.get('api_secret', ''),
                api_data.get('testnet', True)
            )

            if result.get('success'):
                text += f"✅ <b>{exchange_id.upper()}</b>: {result.get('balance_usdt', 0):.2f} USDT\\n"
            else:
                text += f"❌ <b>{exchange_id.upper()}</b>: {result.get('error', 'Ошибка подключения')}\\n"

        except Exception as e:
            logger.error(f"Test API error for {exchange_id}: {e}")
            text += f"❌ <b>{exchange_id.upper()}</b>: {str(e)[:50]}\\n"

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="📱 Меню", callback_data="menu:main")
    await message.answer(text, reply_markup=keyboard.as_markup(), parse_mode="HTML")

# ==================== STATE HANDLERS ====================

# NOTE: API key/secret handlers moved to states.py (states_router)
# to avoid duplicate handler conflicts. Do not add API handlers here.

@commands_router.message(StateFilter(SetupStates.waiting_for_trade_amount))
async def process_trade_amount_cmd(message: Message, state: FSMContext, user: UserSettings, db: Database = None):
    """Обработка объема сделки - делегируем в states.py"""
    # Этот обработчик оставлен для совместимости, основная логика в states.py
    pass

# ==================== TEXT MESSAGES (только без состояния и не команды) ====================

@commands_router.message(StateFilter(None), F.text)
async def any_text(message: Message):
    """Обработка любого текста вне состояний - только если это не команда"""
    # Проверяем что это не команда
    if message.text and message.text.startswith('/'):
        return  # Пропускаем команды, они обработаются другими хендлерами

    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Открыть меню", callback_data="menu:main")
    await message.answer(
        "Я не понимаю текстовые команды. Используй меню:",
        reply_markup=builder.as_markup()
    )