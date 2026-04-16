"""
Command handlers for Telegram bot
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
        "**📚 Команды бота:**\n\n"
        "/start — Главное меню\n"
        "/help — Эта справка\n"
        "/settings — Настройки алертов\n"
        "/balance — Твой баланс\n"
        "/testapi — Проверка API ключей\n"
        "/stop — Остановить бота\n\n"
        "**🔥 Основные функции:**\n"
        "• Авто-торговля спредами\n"
        "• Алерты на межбиржевой арбитраж\n"
        "• Мониторинг позиций\n\n"
        "Вопросы? Пиши в поддержку."
    )
    await message.answer(text)

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
async def cmd_profile(message: Message, user: UserSettings):
    """Команда /profile"""
    from handlers.callbacks import show_profile_menu
    
    class FakeCallback:
        def __init__(self, msg):
            self.from_user = msg.from_user
            self.message = msg
        async def answer(self, **kwargs):
            pass
    
    fake_callback = FakeCallback(message)
    await show_profile_menu(fake_callback, user)

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

@commands_router.message(Command("balance"))
async def cmd_balance(message: Message, user: UserSettings):
    """Показать баланс"""
    text = (
        f"**💰 Твой баланс**\n\n"
        f"📊 **Общий:** {user.total_balance:.2f} USDT\n"
        f"💵 **Доступно:** {user.available_balance:.2f} USDT\n"
        f"🔒 **В сделках:** {user.locked_balance:.2f} USDT\n\n"
        f"_Обновляется автоматически._"
    )
    await message.answer(text)

@commands_router.message(Command("stop"))
async def cmd_stop(message: Message):
    """Остановка бота"""
    text = (
        "🛑 **Бот остановлен**\n\n"
        "Чтобы перезапустить, отправь /start"
    )
    await message.answer(text)

@commands_router.message(Command("testapi"))
async def cmd_test_api(message: Message, user: UserSettings):
    """Проверка API ключей"""
    if not user.api_keys:
        await message.answer(
            "❌ **API ключи не настроены**\n\n"
            "Перейди в Профиль → Мои биржи для настройки."
        )
        return

    text = "🔌 **Проверка API ключей:**\n\n"
    from services.trading_engine import trading_engine

    for exchange_id, api_data in user.api_keys.items():
        if not api_data.get('api_key'):
            continue

        try:
            api_secret = api_data.get('api_secret', '') or ''
            exchange = await trading_engine._get_exchange(
                exchange_id,
                api_data['api_key'],
                api_secret,
                api_data.get('testnet', True)
            )

            if exchange:
                balance = await exchange.fetch_balance()
                usdt = trading_engine._get_usdt_balance(balance)
                text += f"✅ **{exchange_id.upper()}**: {usdt:.2f} USDT\n"
                await exchange.close()
            else:
                text += f"❌ **{exchange_id.upper()}**: Ошибка подключения\n"

        except Exception as e:
            logger.error(f"Test API error for {exchange_id}: {e}")
            text += f"❌ **{exchange_id.upper()}**: {str(e)[:50]}\n"

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="📱 Меню", callback_data="menu:main")
    await message.answer(text, reply_markup=keyboard.as_markup())

# ==================== STATE HANDLERS ====================

@commands_router.message(StateFilter(SetupStates.waiting_for_api_key))
async def process_api_key(message: Message, state: FSMContext, user: UserSettings):
    """Обработка ввода API ключа"""
    try:
        api_key = message.text.strip()
        if not validate_api_key(api_key):
            await message.answer(
                "❌ **Некорректный API Key**\n\n"
                "Введи корректный ключ (минимум 10 символов):"
            )
            return

        await state.update_data(api_key=api_key, step='api_secret')
        await state.set_state(SetupStates.waiting_for_api_secret)

        await message.answer(
            "✅ **API Key принят!**\n\n"
            "Теперь введи **API Secret**:",
            reply_markup=InlineKeyboardBuilder().button(text="❌ Отмена", callback_data="menu:main").as_markup()
        )

    except Exception as e:
        logger.error(f"Error processing API key: {e}")
        await message.answer("❌ Ошибка. Попробуй снова.")

@commands_router.message(StateFilter(SetupStates.waiting_for_api_secret))
async def process_api_secret(message: Message, state: FSMContext, user: UserSettings, db: Database = None):
    """Обработка ввода API Secret"""
    try:
        api_secret = message.text.strip()
        if not validate_api_secret(api_secret):
            await message.answer(
                "❌ **Некорректный API Secret**\n\n"
                "Введи корректный секрет (минимум 10 символов):"
            )
            return

        data = await state.get_data()
        exchange_id = data.get('exchange_id')
        api_key = data.get('api_key')

        if not exchange_id or not api_key:
            await message.answer("❌ **Ошибка сессии**. Начни заново.")
            await state.clear()
            return

        # Проверяем API через CCXT
        is_valid = False
        balance = 0.0
        try:
            from services.trading_engine import trading_engine
            exchange = await trading_engine._get_exchange(exchange_id, api_key, api_secret)
            if exchange:
                bal = await exchange.fetch_balance()
                balance = trading_engine._get_usdt_balance(bal)
                await exchange.close()
                is_valid = True
        except Exception as e:
            logger.error(f"API validation error: {e}")

        if not is_valid:
            await message.answer(
                "❌ **API ключи неверные**\n\n"
                "Проверь ключи и попробуй снова.",
                reply_markup=InlineKeyboardBuilder().button(text="🔄 Повторить", callback_data=f"api:add:{exchange_id}").as_markup()
            )
            await state.clear()
            return

        # Сохраняем в БД
        if not user.api_keys:
            user.api_keys = {}

        user.api_keys[exchange_id] = {
            'api_key': api_key,
            'api_secret': api_secret,
            'testnet': True,
            'balance': balance
        }

        if db:
            await db.update_user(user)

        await state.clear()

        await message.answer(
            f"✅ **API для {escape_html(exchange_id.upper())} сохранен!**\n\n"
            f"💰 Баланс: {balance:.2f} USDT\n\n"
            f"Теперь можешь включить авто-торговлю.",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

    except Exception as e:
        logger.error(f"Error processing API secret: {e}")
        await message.answer("❌ Ошибка сохранения. Попробуй снова.")
        await state.clear()

@commands_router.message(StateFilter(SetupStates.waiting_for_trade_amount))
async def process_trade_amount(message: Message, state: FSMContext, user: UserSettings, db: Database = None):
    """Обработка объема сделки"""
    try:
        amount_str = message.text.strip()
        try:
            amount = float(amount_str)
            if amount < 10 or amount > 100000:
                raise ValueError()
        except ValueError:
            await message.answer(
                "❌ **Некорректный объем**\n\n"
                "Введи число от 10 до 100000 USDT:"
            )
            return

        user.trade_amount = amount
        if db:
            await db.update_user(user)

        await state.clear()
        await message.answer(f"✅ **Объем сделки: {amount} USDT**")

    except Exception as e:
        logger.error(f"Error processing trade amount: {e}")
        await message.answer("❌ Ошибка. Попробуй снова.")
        await state.clear()

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
