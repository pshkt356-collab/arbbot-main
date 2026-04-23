import logging
import html
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
    # Торговые состояния
    waiting_for_trade_size = State()
    waiting_for_sl_price = State()
    waiting_for_tp_price = State()
    waiting_for_partial_percent = State()
    # MEXC Flip Trading states (API)
    waiting_for_flip_leverage = State()
    waiting_for_flip_position_size = State()
    waiting_for_flip_symbols = State()
    waiting_for_flip_api_key = State()      # Ввод API ключа MEXC
    waiting_for_flip_api_secret = State()   # Ввод API секрета MEXC
    # MEXC UID Flip Trading states
    waiting_for_uid_flip_leverage = State()
    waiting_for_uid_flip_position_size = State()
    waiting_for_uid_flip_symbols = State()
    waiting_for_uid_input = State()         # Ввод UID MEXC
    waiting_for_uid_web_token = State()     # Ввод WEB token (из cookie u_id)
    waiting_for_uid_cookies = State()       # Ввод Cookies

@states_router.message(SetupStates.waiting_for_api_key)
async def process_api_key(message: Message, state: FSMContext, user: UserSettings):
    user_id = message.from_user.id
    logger.info(f"[FSM] process_api_key called for user={user_id}")

    if message.text == "/cancel":
        logger.debug(f"[FSM DEBUG] User {user_id} cancelled")
        await state.clear()
        await message.answer("❌ Отменено")
        return

    api_key = message.text.strip()
    if len(api_key) < 10:
        await message.answer("❌ API Key слишком короткий. Попробуйте еще:")
        return

    # Debug: check state
    current_state = await state.get_state()
    logger.info(f"[FSM] Current state for user={user_id}: {current_state}")

    data = await state.get_data()
    logger.info(f"[FSM] FSM data for user={user_id}: {data}")

    exchange = data.get('current_exchange')
    logger.info(f"[FSM] Extracted exchange='{exchange}' for user={user_id}")

    if not exchange:
        # Log to stdout (visible in Railway logs)
        logger.warning(f"[FSM SESSION ERROR] user={user_id}, state={current_state}, data={data}")
        
        # Show debug info directly to user for troubleshooting
        debug_info = f"state={current_state}, keys={list(data.keys()) if data else 'empty'}"
        await message.answer(
            f"❌ Ошибка сессии. Данные: {debug_info}\n\n"
            f"Начните заново: /menu"
        )
        await state.clear()
        return

    await state.update_data(api_key=api_key)
    logger.info(f"[FSM] api_key saved for user={user_id}")
    await state.set_state(SetupStates.waiting_for_api_secret)
    logger.info(f"[FSM] State changed to waiting_for_api_secret for user={user_id}")

    await message.answer(
        f"✅ API Key сохранен\n\n"
        f"Теперь введите API Secret для {exchange.upper()}:\n\n"
        f"(или /cancel для отмены)"
    )

@states_router.message(SetupStates.waiting_for_api_secret)
async def process_api_secret(message: Message, state: FSMContext, user: UserSettings, db: Database):
    user_id = message.from_user.id
    logger.info(f"[FSM] process_api_secret called for user={user_id}")

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

    try:
        result = await trading_engine.test_api_connection(exchange, api_key, api_secret, testnet=True)
        if isinstance(result, dict):
            success = result.get('success', False)
            msg = result.get('message', str(result))
        elif isinstance(result, tuple) and len(result) == 2:
            success, msg = result
        else:
            success = bool(result)
            msg = str(result)
    except Exception as e:
        logger.error(f"[FSM] test_api_connection error: {e}")
        success = False
        msg = f"Ошибка проверки: {str(e)[:100]}"

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


# ==================== TRADE SIZE HANDLER ====================

@states_router.message(SetupStates.waiting_for_trade_size)
async def process_trade_size_input(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода размера сделки"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    try:
        size = float(message.text.strip())
        if size < 10 or size > 100000:
            await message.answer("❌ Допустимый диапазон: $10 - $100000. Введите сумму:")
            return
        data = await state.get_data()
        symbol = data.get('trade_symbol')
        exchange = data.get('trade_exchange')
        side = data.get('trade_side', 'long')
        if not symbol:
            await message.answer("❌ Ошибка: не найден символ. Начните заново.")
            await state.clear()
            return
        from services.trading_engine import trading_engine
        await message.answer(f"⏳ Открываю сделку {symbol} на ${size:.0f}...")
        if side == 'long' and exchange:
            result = await trading_engine.open_single_exchange_trade(
                user=user, symbol=symbol, exchange_id=exchange,
                side='long', size_usd=size, test_mode=user.alert_settings.get('test_mode', True)
            )
        elif side == 'short' and exchange:
            result = await trading_engine.open_single_exchange_trade(
                user=user, symbol=symbol, exchange_id=exchange,
                side='short', size_usd=size, test_mode=user.alert_settings.get('test_mode', True)
            )
        else:
            await message.answer("❌ Ошибка параметров сделки. Начните заново.")
            await state.clear()
            return
        await state.clear()
        if result.success:
            user.total_trades += 1
            if db:
                await db.update_user(user)
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="📊 Мои позиции", callback_data="positions:menu")
            keyboard.button(text="📱 Меню", callback_data="menu:main")
            await message.answer(
                f"✅ **Сделка #{result.trade_id} открыта!**\n\n"
                f"💎 {html.escape(symbol)}\n"
                f"💰 Размер: ${size:.0f}\n"
                f"📊 SL: {result.stop_loss:.4f if result.stop_loss else 'авто'}\n"
                f"📊 TP: {result.take_profit:.4f if result.take_profit else 'авто'}",
                reply_markup=keyboard.as_markup()
            )
        else:
            await message.answer(
                f"❌ **Ошибка открытия сделки**\n\n"
                f"{html.escape(result.error or 'Неизвестная ошибка')}"
            )
    except ValueError:
        await message.answer("❌ Введите число (например: 100):")
    except Exception as e:
        logger.error(f"Trade size error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()

# ==================== SL/TP MODIFICATION HANDLERS ====================

@states_router.message(SetupStates.waiting_for_sl_price)
async def process_sl_price(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода цены стоп-лосса"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    try:
        sl_price = float(message.text.strip())
        if sl_price <= 0:
            await message.answer("❌ Цена должна быть положительной. Введите цену SL:")
            return
        data = await state.get_data()
        trade_id = data.get('modify_trade_id')
        if not trade_id:
            await message.answer("❌ Ошибка: не найден ID сделки.")
            await state.clear()
            return
        from services.trading_engine import trading_engine
        tp_price = data.get('new_tp_price')
        result = await trading_engine.modify_sl_tp(
            trade_id=trade_id, user=user,
            stop_loss=sl_price, take_profit=tp_price
        )
        await state.clear()
        if result.success:
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="📊 К позиции", callback_data=f"position:details:{trade_id}")
            keyboard.button(text="📱 Меню", callback_data="menu:main")
            await message.answer(
                f"✅ **Стоп-лосс обновлён!**\n\n"
                f"📊 SL: {sl_price:.4f}\n"
                f"📊 TP: {tp_price:.4f if tp_price else 'без изменений'}",
                reply_markup=keyboard.as_markup()
            )
        else:
            await message.answer(f"❌ Ошибка: {html.escape(result.error or 'Не удалось изменить SL')}")
    except ValueError:
        await message.answer("❌ Введите числовое значение цены:")
    except Exception as e:
        logger.error(f"SL modification error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()

@states_router.message(SetupStates.waiting_for_tp_price)
async def process_tp_price(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода цены тейк-профита"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    try:
        tp_price = float(message.text.strip())
        if tp_price <= 0:
            await message.answer("❌ Цена должна быть положительной. Введите цену TP:")
            return
        data = await state.get_data()
        trade_id = data.get('modify_trade_id')
        if not trade_id:
            await message.answer("❌ Ошибка: не найден ID сделки.")
            await state.clear()
            return
        from services.trading_engine import trading_engine
        sl_price = data.get('new_sl_price')
        result = await trading_engine.modify_sl_tp(
            trade_id=trade_id, user=user,
            stop_loss=sl_price, take_profit=tp_price
        )
        await state.clear()
        if result.success:
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="📊 К позиции", callback_data=f"position:details:{trade_id}")
            keyboard.button(text="📱 Меню", callback_data="menu:main")
            await message.answer(
                f"✅ **Тейк-профит обновлён!**\n\n"
                f"📊 SL: {sl_price:.4f if sl_price else 'без изменений'}\n"
                f"📊 TP: {tp_price:.4f}",
                reply_markup=keyboard.as_markup()
            )
        else:
            await message.answer(f"❌ Ошибка: {html.escape(result.error or 'Не удалось изменить TP')}")
    except ValueError:
        await message.answer("❌ Введите числовое значение цены:")
    except Exception as e:
        logger.error(f"TP modification error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()

# ==================== PARTIAL CLOSE HANDLER ====================

@states_router.message(SetupStates.waiting_for_partial_percent)
async def process_partial_percent(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода процента частичного закрытия"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return
    try:
        text = message.text.strip().replace('%', '')
        percentage = float(text)
        if percentage <= 0 or percentage > 100:
            await message.answer("❌ Допустимый диапазон: 1% - 100%. Введите процент:")
            return
        data = await state.get_data()
        trade_id = data.get('partial_trade_id')
        if not trade_id:
            await message.answer("❌ Ошибка: не найден ID сделки.")
            await state.clear()
            return
        from services.trading_engine import trading_engine
        await message.answer(f"⏳ Закрываю {percentage:.0f}% позиции #{trade_id}...")
        result = await trading_engine.partial_close(trade_id=trade_id, user=user, percentage=percentage)
        await state.clear()
        if result.success:
            metadata = result.metadata or {}
            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="📊 Мои позиции", callback_data="positions:menu")
            keyboard.button(text="📱 Меню", callback_data="menu:main")
            await message.answer(
                f"✅ **Частичное закрытие выполнено!**\n\n"
                f"💰 P&L: ${metadata.get('partial_pnl', 0):.2f}\n"
                f"📊 Остаток: ${metadata.get('remaining_size', 0):.2f}",
                reply_markup=keyboard.as_markup()
            )
        else:
            await message.answer(f"❌ Ошибка: {html.escape(result.error or 'Не удалось закрыть часть')}")
    except ValueError:
        await message.answer("❌ Введите число от 1 до 100:")
    except Exception as e:
        logger.error(f"Partial close error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


# ==================== MEXC FLIP TRADING STATE HANDLERS ====================

@states_router.message(SetupStates.waiting_for_flip_leverage)
async def process_flip_leverage(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода плеча для flip trading"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        leverage = int(message.text.strip())
        if leverage < 1 or leverage > 300:
            await message.answer("❌ Плечо должно быть от 1 до 300. Попробуй снова:")
            return

        if db:
            flip_settings = await db.get_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_flip_settings(user.user_id)
            flip_settings.leverage = leverage
            await db.update_flip_settings(flip_settings)

        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚡ К плечу", callback_data="flip:leverage")
        keyboard.button(text="🔥 Flip меню", callback_data="flip:menu")
        keyboard.adjust(1)

        await message.answer(
            f"✅ **Плечо установлено: {leverage}x**",
            reply_markup=keyboard.as_markup()
        )

    except ValueError:
        await message.answer("❌ Введите целое число (например: 200):")
    except Exception as e:
        logger.error(f"Flip leverage error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


@states_router.message(SetupStates.waiting_for_flip_position_size)
async def process_flip_position_size(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода размера позиции для flip trading"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        size = float(message.text.strip())
        if size < 1 or size > 10000:
            await message.answer("❌ Размер позиции должен быть от $1 до $10000. Попробуй снова:")
            return

        if db:
            flip_settings = await db.get_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_flip_settings(user.user_id)
            flip_settings.position_size_usd = size
            await db.update_flip_settings(flip_settings)

        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💰 К размеру", callback_data="flip:position_size")
        keyboard.button(text="🔥 Flip меню", callback_data="flip:menu")
        keyboard.adjust(1)

        await message.answer(
            f"✅ **Размер позиции: ${size:.0f}**",
            reply_markup=keyboard.as_markup()
        )

    except ValueError:
        await message.answer("❌ Введите число (например: 100):")
    except Exception as e:
        logger.error(f"Flip position size error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


@states_router.message(SetupStates.waiting_for_flip_api_key)
async def process_flip_api_key(message: Message, state: FSMContext, user: UserSettings):
    """Обработка ввода API ключа MEXC"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    api_key = message.text.strip()
    if len(api_key) < 10:
        await message.answer("❌ API Key слишком короткий. Попробуй снова:")
        return

    # Сохраняем ключ во временные данные FSM
    await state.update_data(mexc_api_key=api_key)
    await state.set_state(SetupStates.waiting_for_flip_api_secret)

    await message.answer(
        "**🔐 Шаг 2/2: Введи API Secret:**\n\n"
        "_(Отправь /cancel для отмены)_",
        reply_markup=InlineKeyboardBuilder().button(text="🔙 Назад", callback_data="flip:api_menu").as_markup()
    )


@states_router.message(SetupStates.waiting_for_flip_api_secret)
async def process_flip_api_secret(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода API секрета MEXC и сохранение"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    api_secret = message.text.strip()
    if len(api_secret) < 10:
        await message.answer("❌ API Secret слишком короткий. Попробуй снова:")
        return

    try:
        # Получаем ранее сохраненный ключ
        data = await state.get_data()
        api_key = data.get('mexc_api_key', '')

        if not api_key:
            await message.answer("❌ Ошибка: API Key не найден. Начни заново.")
            await state.clear()
            return

        # Сохраняем ключи в БД ДО проверки подключения — даже если сеть/время проблемы
        if db:
            flip_settings = await db.get_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_flip_settings(user.user_id)

            flip_settings.mexc_api_key = api_key
            flip_settings.mexc_api_secret = api_secret
            await db.update_flip_settings(flip_settings)

        # Пробуем подключиться к MEXC для проверки
        await message.answer("🔄 Проверяю подключение к MEXC...")

        from services.mexc_flip_trader import MexcAPI
        mexc = MexcAPI(api_key, api_secret)
        try:
            conn = await mexc.test_connection()

            await state.clear()

            if conn.get('success'):
                bal = conn.get('balance_usdt', 0)
                keyboard = InlineKeyboardBuilder()
                keyboard.button(text="🔑 API меню", callback_data="flip:api_menu")
                keyboard.button(text="🔥 Flip меню", callback_data="flip:menu")
                keyboard.adjust(1)

                await message.answer(
                    f"✅ **API MEXC сохранены и подключены!**\n\n"
                    f"💳 **Баланс:** ${bal:.2f} USDT\n\n"
                    f"Теперь можешь переключиться в реальный режим.",
                    reply_markup=keyboard.as_markup()
                )
            else:
                # Ключи уже сохранены — показываем предупреждение
                keyboard = InlineKeyboardBuilder()
                keyboard.button(text="🔑 API меню", callback_data="flip:api_menu")
                keyboard.button(text="🔥 Flip меню", callback_data="flip:menu")
                keyboard.adjust(1)

                await message.answer(
                    f"⚠️ **API сохранены, но проверка не пройдена**\n\n"
                    f"Причина: {conn.get('error', 'Неизвестно')[:100]}\n\n"
                    f"Проверь ключи позже через меню API.",
                    reply_markup=keyboard.as_markup()
                )
        finally:
            await mexc.close()

    except Exception as e:
        logger.error(f"API secret save error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


# ==================== MEXC UID FLIP TRADING STATE HANDLERS ====================

@states_router.message(SetupStates.waiting_for_uid_input)
async def process_uid_input(message: Message, state: FSMContext, user: UserSettings):
    """Обработка ввода MEXC UID"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    uid = message.text.strip()
    if not uid or len(uid) < 3:
        await message.answer("❌ UID слишком короткий. Введите корректный UID:")
        return

    await state.update_data(uid_flip_uid=uid)
    await state.set_state(SetupStates.waiting_for_uid_web_token)

    await message.answer(
        f"✅ **UID сохранён:** `{uid[:20]}...`\n\n"
        f"**🔐 Шаг 2/3: Введите WEB Token**\n\n"
        f"Откройте DevTools (F12) → Application → Cookies → futures.mexc.com\n"
        f"Найдите cookie `u_id` и скопируйте его значение (начинается с `WEB_`):\n\n"
        f"_(Отправьте /cancel для отмены)_"
    )


@states_router.message(SetupStates.waiting_for_uid_web_token)
async def process_uid_web_token(message: Message, state: FSMContext, user: UserSettings):
    """Обработка ввода WEB Token"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    web_token = message.text.strip()
    if not web_token.startswith("WEB"):
        await message.answer(
            "❌ WEB Token должен начинаться с `WEB`.\n\n"
            "Убедитесь что вы скопировали значение cookie `u_id` из DevTools:",
            parse_mode="Markdown"
        )
        return

    await state.update_data(uid_flip_web_token=web_token)
    await state.set_state(SetupStates.waiting_for_uid_cookies)

    await message.answer(
        f"✅ **WEB Token сохранён**\n\n"
        f"**🍪 Шаг 3/3: Введите Cookies (опционально)**\n\n"
        f"Откройте DevTools → Application → Cookies, скопируйте все cookies\n"
        f"в формате `key1=value1; key2=value2`.\n\n"
        f"Если не хотите вводить cookies — отправьте прочерк `-`:\n\n"
        f"_(Отправьте /cancel для отмены)_"
    )


@states_router.message(SetupStates.waiting_for_uid_cookies)
async def process_uid_cookies(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода Cookies и сохранение всех UID данных"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    cookies = message.text.strip()
    if cookies == "-":
        cookies = ""

    data = await state.get_data()
    uid = data.get('uid_flip_uid', '')
    web_token = data.get('uid_flip_web_token', '')

    if not uid or not web_token:
        await message.answer("❌ Ошибка: UID или WEB Token не найдены. Начните заново.")
        await state.clear()
        return

    try:
        flip_settings = await db.get_uid_flip_settings(user.user_id)
        if not flip_settings:
            flip_settings = await db.create_uid_flip_settings(user.user_id)

        flip_settings.uid = uid
        flip_settings.web_token = web_token
        flip_settings.cookies = cookies
        await db.update_uid_flip_settings(flip_settings)

        await state.clear()

        # Пробуем подключиться
        await message.answer("🔄 Проверяю подключение через UID...")

        from services.mexc_uid_trader import MexcUIDClient
        client = MexcUIDClient(uid=uid, web_token=web_token, cookies=cookies)
        try:
            conn = await client.test_connection()

            keyboard = InlineKeyboardBuilder()
            keyboard.button(text="🔑 UID меню", callback_data="uid_flip:menu")
            keyboard.button(text="📱 Меню", callback_data="menu:main")
            keyboard.adjust(1)

            if conn.get('success'):
                bal = conn.get('balance_usdt', 0)
                await message.answer(
                    f"✅ **MEXC UID подключён!**\n\n"
                    f"💳 Баланс: `${bal:.2f} USDT`\n"
                    f"🆔 UID: `{uid[:20]}...`\n"
                    f"🔑 WEB Token: `{web_token[:25]}...`\n"
                    f"{'🍪 Cookies: сохранены' if cookies else '🍪 Cookies: не указаны'}\n\n"
                    f"Теперь вы можете запускать UID Flip Trading.",
                    reply_markup=keyboard.as_markup()
                )
            else:
                await message.answer(
                    f"⚠️ **UID данные сохранены, но проверка не пройдена**\n\n"
                    f"Причина: `{conn.get('error', 'Неизвестно')[:100]}`\n\n"
                    f"Возможно, WEB token истёк. Обновите его через меню.",
                    reply_markup=keyboard.as_markup()
                )
        finally:
            await client.close()

    except Exception as e:
        logger.error(f"UID cookies save error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


@states_router.message(SetupStates.waiting_for_uid_flip_leverage)
async def process_uid_flip_leverage(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода плеча для UID flip trading"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        leverage = int(message.text.strip())
        if leverage < 1 or leverage > 300:
            await message.answer("❌ Плечо от 1 до 300. Попробуйте:")
            return

        if db:
            flip_settings = await db.get_uid_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_uid_flip_settings(user.user_id)
            flip_settings.leverage = leverage
            await db.update_uid_flip_settings(flip_settings)

        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚡ К плечу", callback_data="uid_flip:leverage")
        keyboard.button(text="🔑 UID меню", callback_data="uid_flip:menu")
        keyboard.adjust(1)

        await message.answer(f"✅ **Плечо UID установлено: {leverage}x**", reply_markup=keyboard.as_markup())

    except ValueError:
        await message.answer("❌ Введите целое число (например: 200):")
    except Exception as e:
        logger.error(f"UID flip leverage error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


@states_router.message(SetupStates.waiting_for_uid_flip_position_size)
async def process_uid_flip_position_size(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода размера позиции для UID flip trading"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        size = float(message.text.strip())
        if size < 1 or size > 10000:
            await message.answer("❌ Размер позиции от $1 до $10000:")
            return

        if db:
            flip_settings = await db.get_uid_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_uid_flip_settings(user.user_id)
            flip_settings.position_size_usd = size
            await db.update_uid_flip_settings(flip_settings)

        await state.clear()

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💰 К размеру", callback_data="uid_flip:position_size")
        keyboard.button(text="🔑 UID меню", callback_data="uid_flip:menu")
        keyboard.adjust(1)

        await message.answer(f"✅ **Размер позиции UID: ${size:.0f}**", reply_markup=keyboard.as_markup())

    except ValueError:
        await message.answer("❌ Введите число (например: 100):")
    except Exception as e:
        logger.error(f"UID flip position size error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()


@states_router.message(SetupStates.waiting_for_uid_flip_symbols)
async def process_uid_flip_symbols(message: Message, state: FSMContext, user: UserSettings, db: Database):
    """Обработка ввода символов для UID flip trading"""
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Отменено")
        return

    try:
        raw = message.text.strip()
        symbols = [s.strip().upper() for s in raw.replace(" ", ",").split(",") if s.strip()]

        if not symbols:
            await message.answer("❌ Введите хотя бы один символ (например: BTC, ETH, SOL):")
            return

        if len(symbols) > 20:
            symbols = symbols[:20]

        if db:
            flip_settings = await db.get_uid_flip_settings(user.user_id)
            if not flip_settings:
                flip_settings = await db.create_uid_flip_settings(user.user_id)
            flip_settings.selected_symbols = symbols
            await db.update_uid_flip_settings(flip_settings)

        await state.clear()

        symbols_str = ", ".join(symbols)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="📋 К символам", callback_data="uid_flip:symbols")
        keyboard.button(text="🔑 UID меню", callback_data="uid_flip:menu")
        keyboard.adjust(1)

        await message.answer(f"✅ **Символы UID: {symbols_str}**", reply_markup=keyboard.as_markup())

    except Exception as e:
        logger.error(f"UID flip symbols error: {e}")
        await message.answer(f"❌ Ошибка: {html.escape(str(e))[:200]}")
        await state.clear()
