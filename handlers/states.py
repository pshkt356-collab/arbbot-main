# -*- coding: utf-8 -*-
"""
State handlers for Telegram bot FSM - FINAL FIX v4
Исправлено:
1. Проверка наличия exchange в FSM data
2. Корректная обработка waiting_for_api_secret
3. Fallback для отсутствующих ключей
"""
from aiogram import Router, F
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton
import logging

from database.models import Database
from handlers.callbacks import AVAILABLE_EXCHANGES, validate_exchange

logger = logging.getLogger(__name__)

states_router = Router()

# Состояния
class SetupStates:
    """FSM States для настройки"""
    waiting_for_api_key = "waiting_for_api_key"
    waiting_for_api_secret = "waiting_for_api_secret"
    waiting_for_trade_amount = "waiting_for_trade_amount"
    waiting_for_sl_price = "waiting_for_sl_price"
    waiting_for_tp_price = "waiting_for_tp_price"

@states_router.message(F.text, SetupStates.waiting_for_api_key)
async def process_api_key(message: Message, state: FSMContext, user=None, db=None):
    """Process API key input with validation"""
    try:
        api_key = message.text.strip()
        
        if len(api_key) < 10:
            await message.answer("❌ <b>API ключ слишком короткий</b>. Попробуй еще раз:")
            return
        
        data = await state.get_data()
        exchange_id = data.get('current_exchange')
        
        if not exchange_id:
            logger.error(f"[FSM] process_api_key: exchange_id is None! Data: {data}")
            await state.clear()
            await message.answer(
                "❌ <b>Ошибка сессии</b>. Начни заново: /exchanges",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return
        
        if not validate_exchange(exchange_id):
            await state.clear()
            await message.answer(
                f"❌ <b>Неверная биржа:</b> {exchange_id}",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return
        
        # Сохраняем API ключ
        await state.update_data(api_key=api_key)
        
        # ИСПРАВЛЕНО: Правильное переключение состояния
        await state.set_state(SetupStates.waiting_for_api_secret)
        await state.update_data(step='api_secret')
        
        logger.info(f"[FSM] API key received for {exchange_id}, waiting for secret")
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="❌ Отмена", callback_data="menu:main")
        
        await message.answer(
            f"<b>🔑 Добавление API для {exchange_id.upper()}</b>\\n\\n"
            f"API Key сохранен.\\n"
            f"Теперь введи <b>API Secret</b>:",
            reply_markup=keyboard.as_markup(),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"[FSM] Error in process_api_key: {e}")
        await state.clear()
        await message.answer(
            "❌ <b>Ошибка обработки</b>. Попробуй /start",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

@states_router.message(F.text, SetupStates.waiting_for_api_secret)
async def process_api_secret(message: Message, state: FSMContext, user=None, db=None):
    """Process API secret and test connection"""
    try:
        api_secret = message.text.strip()
        
        data = await state.get_data()
        exchange_id = data.get('current_exchange')
        api_key = data.get('api_key')
        
        # ИСПРАВЛЕНО: Проверяем наличие всех необходимых данных
        if not exchange_id or not api_key:
            logger.error(f"[FSM] Missing data in process_api_secret: exchange={exchange_id}, api_key exists={bool(api_key)}")
            await state.clear()
            await message.answer(
                "❌ <b>Ошибка сессии</b>. Данные утеряны. Начни заново: /exchanges",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return
        
        if not validate_exchange(exchange_id):
            await state.clear()
            await message.answer(
                f"❌ <b>Неверная биржа:</b> {exchange_id}",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return
        
        # Удаляем сообщение с секретом для безопасности
        try:
            await message.delete()
        except:
            pass
        
        processing_msg = await message.answer("⏳ <b>Проверяю API ключ...</b>", parse_mode="HTML")
        
        # Тестируем подключение
        from services.trading_engine import trading_engine
        result = await trading_engine.test_api_connection(
            exchange_id, api_key, api_secret, testnet=True
        )
        
        if result.get('success'):
            # Сохраняем API данные
            if user and db:
                user.add_api_key(exchange_id, api_key, api_secret, testnet=True)
                await db.update_user(user)
                
                # Добавляем биржу в выбранные
                if exchange_id not in user.selected_exchanges:
                    user.selected_exchanges.append(exchange_id)
                    await db.update_user(user)
            
            await state.clear()
            
            balance = result.get('balance_usdt', 0)
            await processing_msg.edit_text(
                f"✅ <b>API ключ добавлен!</b>\\n\\n"
                f"Биржа: <b>{exchange_id.upper()}</b>\\n"
                f"Баланс: <b>{balance:.2f} USDT</b>\\n\\n"
                f"Теперь ты можешь торговать на этой бирже.",
                reply_markup=InlineKeyboardBuilder()
                    .row(
                        InlineKeyboardButton(text="💼 Биржи", callback_data="profile:exchanges"),
                        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
                    ).as_markup(),
                parse_mode="HTML"
            )
            logger.info(f"[FSM] API key added successfully for {exchange_id}, balance: {balance}")
        else:
            error_msg = result.get('error', 'Неизвестная ошибка')
            await processing_msg.edit_text(
                f"❌ <b>Ошибка подключения:</b>\\n\\n"
                f"{error_msg}\\n\\n"
                f"Попробуй еще раз: /exchanges",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup(),
                parse_mode="HTML"
            )
            await state.clear()
            
    except Exception as e:
        logger.error(f"[FSM] Error in process_api_secret: {e}")
        await state.clear()
        await message.answer(
            "❌ <b>Ошибка обработки секрета</b>. Попробуй /start",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

@states_router.message(F.text, SetupStates.waiting_for_trade_amount)
async def process_trade_amount(message: Message, state: FSMContext, user=None, db=None):
    """Process trade amount input"""
    try:
        amount = float(message.text.strip())
        if amount < 10:
            await message.answer("❌ <b>Минимальный объем: 10 USDT</b>. Попробуй еще раз:")
            return
        if amount > 100000:
            await message.answer("❌ <b>Максимальный объем: 100000 USDT</b>. Попробуй еще раз:")
            return
        
        if user and db:
            user.trade_amount = amount
            await db.update_user(user)
        
        await state.clear()
        
        await message.answer(
            f"✅ <b>Объем сделки установлен: ${amount}</b>",
            reply_markup=InlineKeyboardBuilder()
                .row(
                    InlineKeyboardButton(text="⚙️ Настройки", callback_data="auto_trade:settings"),
                    InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
                ).as_markup(),
            parse_mode="HTML"
        )
        logger.info(f"[FSM] Trade amount set: {amount}")
    except ValueError:
        await message.answer("❌ <b>Введи число</b> (например: 100):")
    except Exception as e:
        logger.error(f"[FSM] Error in process_trade_amount: {e}")
        await state.clear()
        await message.answer(
            "❌ <b>Ошибка</b>. Попробуй /start",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

@states_router.message(F.text, SetupStates.waiting_for_sl_price)
async def process_sl_price(message: Message, state: FSMContext, user=None, db=None):
    """Process stop-loss price input"""
    try:
        sl_price = float(message.text.strip())
        if sl_price <= 0:
            await message.answer("❌ <b>Цена должна быть больше 0</b>. Попробуй еще раз:")
            return
        
        data = await state.get_data()
        trade_id = data.get('modify_trade_id')
        
        if not trade_id or not db:
            await state.clear()
            await message.answer("❌ <b>Ошибка сессии</b>. Попробуй снова.")
            return
        
        # Обновляем SL в БД
        await db.update_trade_field(trade_id, 'stop_loss_price', sl_price)
        await db.update_trade_field(trade_id, 'updated_at', 'CURRENT_TIMESTAMP')
        
        await state.clear()
        await message.answer(
            f"✅ <b>Stop-Loss обновлен: {sl_price}</b>",
            reply_markup=InlineKeyboardBuilder()
                .button(text="📊 Позиция", callback_data=f"position:details:{trade_id}")
                .as_markup(),
            parse_mode="HTML"
        )
        logger.info(f"[FSM] SL updated for trade {trade_id}: {sl_price}")
    except ValueError:
        await message.answer("❌ <b>Введи число</b> (например: 65000):")
    except Exception as e:
        logger.error(f"[FSM] Error in process_sl_price: {e}")
        await state.clear()

@states_router.message(F.text, SetupStates.waiting_for_tp_price)
async def process_tp_price(message: Message, state: FSMContext, user=None, db=None):
    """Process take-profit price input"""
    try:
        tp_price = float(message.text.strip())
        if tp_price <= 0:
            await message.answer("❌ <b>Цена должна быть больше 0</b>. Попробуй еще раз:")
            return
        
        data = await state.get_data()
        trade_id = data.get('modify_trade_id')
        
        if not trade_id or not db:
            await state.clear()
            await message.answer("❌ <b>Ошибка сессии</b>. Попробуй снова.")
            return
        
        # Обновляем TP в БД
        await db.update_trade_field(trade_id, 'take_profit_price', tp_price)
        await db.update_trade_field(trade_id, 'updated_at', 'CURRENT_TIMESTAMP')
        
        await state.clear()
        await message.answer(
            f"✅ <b>Take-Profit обновлен: {tp_price}</b>",
            reply_markup=InlineKeyboardBuilder()
                .button(text="📊 Позиция", callback_data=f"position:details:{trade_id}")
                .as_markup(),
            parse_mode="HTML"
        )
        logger.info(f"[FSM] TP updated for trade {trade_id}: {tp_price}")
    except ValueError:
        await message.answer("❌ <b>Введи число</b> (например: 75000):")
    except Exception as e:
        logger.error(f"[FSM] Error in process_tp_price: {e}")
        await state.clear()

@states_router.message(F.text.startswith("/cancel"))
async def cancel_any_state(message: Message, state: FSMContext):
    """Cancel any state"""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await message.answer(
            "✅ <b>Отменено</b>",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup(),
            parse_mode="HTML"
        )