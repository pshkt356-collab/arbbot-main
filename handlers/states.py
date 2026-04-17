import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from database.models import UserSettings, Database
from services.trading_engine import trading_engine

logger = logging.getLogger(__name__)
states_router = Router()

class SetupStates(StatesGroup):
    waiting_for_api_exchange = State()
    waiting_for_api_key = State()
    waiting_for_api_secret = State()
    waiting_for_custom_threshold = State()
    waiting_for_max_position = State()
    waiting_for_commission_exchange = State()
    waiting_for_commission_value = State()
    waiting_for_min_spread = State()
    waiting_for_symbol_whitelist = State()
    waiting_for_partial_close = State()
    waiting_for_take_profit = State()
    waiting_for_breakeven_trigger = State()
    waiting_for_trailing_distance = State()
    waiting_for_max_position_hours = State()
    waiting_for_custom_leverage = State()
    waiting_for_balance_usage = State()
    waiting_for_trade_amount = State()

@states_router.message(SetupStates.waiting_for_api_key)
async def process_api_key(message: Message, state: FSMContext, user: UserSettings):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    api_key = message.text.strip()
    if len(api_key) < 10:
        await message.answer("❌ API Key слишком короткий. Попробуйте еще:")
        return

    data = await state.get_data()
    exchange = data.get('current_exchange')

    if not exchange:
        logger.error(f"No current_exchange in FSM data for user {message.from_user.id}")
        await message.answer("❌ Ошибка сессии. Начните заново: /menu")
        await state.clear()
        return

    await state.update_data(api_key=api_key)
    await state.set_state(SetupStates.waiting_for_api_secret)

    await message.answer(
        f"✅ API Key сохранен\n\n"
        f"Теперь введите API Secret для {exchange.upper()}:\n\n"
        f"(или /cancel для отмены)"
    )

@states_router.message(SetupStates.waiting_for_api_secret)
async def process_api_secret(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    api_secret = message.text.strip()
    data = await state.get_data()

    exchange = data.get('current_exchange')
    api_key = data.get('api_key')

    if not exchange or not api_key:
        logger.error(f"Missing data in FSM: exchange={exchange}, api_key exists={bool(api_key)}")
        await message.answer("❌ Ошибка данных. Начните заново.")
        await state.clear()
        return

    await message.answer(f"⏳ Проверка API ключей {exchange.upper()}...")

    success, msg = await trading_engine.test_api_connection(exchange, api_key, api_secret, testnet=True)

    if not success:
        await message.answer(f"❌ Ошибка проверки API:\n `{msg}`\n\nПопробуйте заново: /menu")
        await state.clear()
        return

    try:
        if not isinstance(user.api_keys, dict):
            user.api_keys = {}

        user.api_keys[exchange] = {
            'api_key': api_key,
            'api_secret': api_secret,
            'testnet': True,
            'created_at': str(datetime.now())
        }
        user.is_trading_enabled = True

        await db.update_user(user)
        logger.info(f"API saved for user {user.user_id}: {exchange} (verified)")

        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔌 Добавить еще биржу", callback_data="setup:trading")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        keyboard.adjust(1)

        await message.answer(
            f"✅ **{exchange.upper()} успешно подключена!**\n\n"
            f"Проверка: {msg}\n"
            f"API Key: {api_key[:8]}...{api_key[-4:]}\n"
            f"Режим: Testnet (безопасный)\n\n"
            f"Теперь вы можете:\n"
            f"• Получать алерты о спредах\n"
            f"• Торговать в тестовом режиме\n"
            f"• Настроить автоторговлю",
            reply_markup=keyboard.as_markup()
        )

    except Exception as e:
        logger.error(f"Error saving API: {e}")
        await message.answer(f"❌ Ошибка сохранения: {str(e)[:100]}")
        await state.clear()

@states_router.message(SetupStates.waiting_for_max_position)
async def process_max_position(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        amount = float(message.text.strip())
        if amount < 100:
            await message.answer("❌ Минимум $100. Введите сумму:")
            return
        if amount > 1000000:
            await message.answer("❌ Слишком большая сумма. Введите реалистичное значение:")
            return

        user.risk_settings['max_position_usd'] = amount
        await db.update_user(user)
        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")

        await message.answer(
            f"✅ Макс. позиция установлена: ${amount:,.0f}",
            reply_markup=keyboard.as_markup()
        )
    except ValueError:
        await message.answer("❌ Введите число (например: 5000):")

@states_router.message(SetupStates.waiting_for_min_spread)
async def process_custom_spread(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        spread = float(message.text.strip())
        if spread < 0.05 or spread > 10:
            await message.answer("❌ Допустимый диапазон: 0.05% - 10%. Введите значение:")
            return

        user.alert_settings['min_spread'] = spread
        await db.update_user(user)
        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔔 Настройки алертов", callback_data="setup:alerts")
        keyboard.button(text="📱 Меню", callback_data="menu:main")

        await message.answer(
            f"✅ Порог алертов установлен: {spread}%",
            reply_markup=keyboard.as_markup()
        )
    except ValueError:
        await message.answer("❌ Введите число (например: 0.5):")

@states_router.message(SetupStates.waiting_for_custom_leverage)
async def process_custom_leverage(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка произвольного плеча"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
        
    try:
        lev = int(message.text.strip())
        if lev < 1 or lev > 125:
            await message.answer("❌ Плечо должно быть от 1 до 125")
            return
            
        user.risk_settings['max_leverage'] = lev
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        await message.answer(f"✅ Плечо установлено: {lev}x", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите число (например: 20):")

@states_router.message(SetupStates.waiting_for_take_profit)
async def process_take_profit(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        tp = float(message.text.strip())
        if tp < 5 or tp > 100:
            await message.answer("❌ Допустимый диапазон: 5% - 100%")
            return

        user.risk_settings['take_profit_percent'] = tp
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        await message.answer(f"✅ Тейк-профит установлен: {tp}%", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите число:")

@states_router.message(SetupStates.waiting_for_breakeven_trigger)
async def process_breakeven(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        be = float(message.text.strip())
        if be < 3 or be > 50:
            await message.answer("❌ Допустимый диапазон: 3% - 50%")
            return

        user.risk_settings['stop_loss_breakeven_trigger'] = be
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        await message.answer(f"✅ Триггер безубытка: {be}%", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите число:")

@states_router.message(SetupStates.waiting_for_trailing_distance)
async def process_trailing(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        dist = float(message.text.strip())
        if dist < 2 or dist > 30:
            await message.answer("❌ Допустимый диапазон: 2% - 30%")
            return

        user.risk_settings['trailing_stop_distance'] = dist
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        await message.answer(f"✅ Дистанция трейлинга: {dist}%", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите число:")

@states_router.message(SetupStates.waiting_for_max_position_hours)
async def process_max_hours(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        hours = int(message.text.strip())
        if hours < 0 or hours > 168:
            await message.answer("❌ Допустимый диапазон: 0 - 168 часов")
            return

        user.risk_settings['max_position_hours'] = hours
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        if hours == 0:
            await message.answer("✅ Авто-закрытие отключено", reply_markup=keyboard.as_markup())
        else:
            await message.answer(f"✅ Авто-закрытие через: {hours}ч", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите целое число:")

@states_router.message(SetupStates.waiting_for_balance_usage)
async def process_balance_usage(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        pct = float(message.text.strip())
        if pct < 1 or pct > 100:
            await message.answer("❌ Допустимый диапазон: 1% - 100%")
            return

        user.risk_settings['balance_usage_percent'] = pct
        await db.update_user(user)
        await state.clear()
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Риск-меню", callback_data="setup:risk")
        keyboard.button(text="📱 Меню", callback_data="menu:main")
        
        await message.answer(f"✅ Использование баланса: {pct}%", reply_markup=keyboard.as_markup())
    except ValueError:
        await message.answer("❌ Введите число (например: 95):")

@states_router.callback_query(F.data.startswith("partial:"))
async def partial_close_start(callback: CallbackQuery, state: FSMContext):
    trade_id = callback.data.split(":")[1]
    await state.update_data(partial_trade_id=int(trade_id))
    await state.set_state(SetupStates.waiting_for_partial_close)

    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="25%", callback_data="partial_pct:25")
    keyboard.button(text="50%", callback_data="partial_pct:50")
    keyboard.button(text="75%", callback_data="partial_pct:75")
    keyboard.button(text="🔙 Отмена", callback_data="menu:trades")
    keyboard.adjust(3, 1)

    await callback.message.edit_text(
        f"💰 **Частичное закрытие сделки #{trade_id}**\n\n"
        f"Введите процент для закрытия (1-100):\n"
        f"Или выберите быстрый вариант:",
        reply_markup=keyboard.as_markup()
    )
    await callback.answer()

@states_router.callback_query(F.data.startswith("partial_pct:"))
async def partial_close_pct(callback: CallbackQuery, state: FSMContext, user: UserSettings, db: Database):
    pct = float(callback.data.split(":")[1])
    data = await state.get_data()
    trade_id = data.get('partial_trade_id')

    if not trade_id:
        await callback.answer("Ошибка: не найден ID сделки")
        return

    await callback.message.edit_text(f"⏳ Закрытие {pct}% позиции #{trade_id}...")

    result = await trading_engine.partial_close(trade_id, user, pct)

    if result.success:
        remaining = result.metadata.get('remaining', 0)
        profit = result.metadata.get('profit', 0)

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💼 Мои сделки", callback_data="menu:trades")
        keyboard.button(text="📱 Меню", callback_data="menu:main")

        text = (f"✅ **Частично закрыто {pct}%**\n\n"
                f"Сделка #{trade_id}\n"
                f"Прибыль: ${profit:.2f}\n")
        if remaining > 0:
            text += f"Осталось открыто: {remaining:.0f}%"
        else:
            text += "Сделка полностью закрыта"

        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
    else:
        await callback.message.edit_text(f"❌ Ошибка: {result.error}")

    await state.clear()
    await callback.answer()

@states_router.message(SetupStates.waiting_for_partial_close)
async def process_partial_close(message: Message, state: FSMContext, user: UserSettings, db: Database):
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        pct = float(message.text.strip())
        if pct <= 0 or pct > 100:
            await message.answer("❌ Введите процент от 1 до 100:")
            return

        data = await state.get_data()
        trade_id = data.get('partial_trade_id')

        if not trade_id:
            await message.answer("❌ Ошибка: не найден ID сделки")
            await state.clear()
            return

        await message.answer(f"⏳ Закрытие {pct}% позиции...")

        result = await trading_engine.partial_close(trade_id, user, pct)

        if result.success:
            remaining = result.metadata.get('remaining', 0)
            profit = result.metadata.get('profit', 0)

            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="💼 Мои сделки", callback_data="menu:trades")

            text = (f"✅ **Частично закрыто {pct}%**\n\n"
                    f"Прибыль: ${profit:.2f}\n")
            if remaining > 0:
                text += f"Осталось: {remaining:.0f}%"

            await message.answer(text, reply_markup=keyboard.as_markup())
        else:
            await message.answer(f"❌ Ошибка: {result.error}")

        await state.clear()

    except ValueError:
        await message.answer("❌ Введите число (например: 50):")
