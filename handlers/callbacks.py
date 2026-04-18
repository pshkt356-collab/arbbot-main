# -*- coding: utf-8 -*-
"""
Callback handlers for Telegram bot - FINAL FIX
"""
from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError
import logging
import html
import threading

# Используем существующие импорты из оригинальной структуры
from database.models import UserSettings, Database
from services.trading_engine import trading_engine

# ИСПРАВЛЕНО: Используем SetupStates вместо BotStates
from handlers.states import SetupStates

logger = logging.getLogger(__name__)

callbacks_router = Router()

# Глобальная переменная для бота (устанавливается из main.py) - BUG 20 FIX
_bot = None
_bot_lock = threading.Lock()
_bot_initialized = False
_blocked_users_cache = set()  # Кэш ID пользователей, заблокировавших бота

def set_bot(bot_instance):
    """Установка бота для отправки сообщений"""
    global _bot, _bot_initialized
    with _bot_lock:
        if _bot_initialized and _bot is not None:
            return
        _bot = bot_instance
        _bot_initialized = True

def get_bot():
    """Получение бота с проверкой инициализации"""
    with _bot_lock:
        return _bot if _bot_initialized else None

def escape_html(text: str) -> str:
    """Escape HTML special characters"""
    return html.escape(str(text)) if text else ""

# Доступные биржи (определяем здесь для избежания проблем с импортами)
AVAILABLE_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc', 'whitebit']

def validate_exchange(exchange_id: str) -> bool:
    """Проверка валидности биржи"""
    return exchange_id in AVAILABLE_EXCHANGES

# ==================== MENU CALLBACKS ====================

@callbacks_router.callback_query(F.data == "menu:main")
async def show_main_menu(callback: CallbackQuery, user: UserSettings):
    """Главное меню"""
    await callback.answer()

    builder = InlineKeyboardBuilder()

    # Первый ряд
    builder.row(
        InlineKeyboardButton(text="🚀 Авто-торговля", callback_data="auto_trade:menu"),
        InlineKeyboardButton(text="📊 Спреды", callback_data="spreads:menu")
    )

    # Второй ряд
    builder.row(
        InlineKeyboardButton(text="📈 Мониторинг", callback_data="monitoring:menu"),
        InlineKeyboardButton(text="📉 Позиции", callback_data="positions:menu")
    )

    # Третий ряд
    builder.row(
        InlineKeyboardButton(text="⚙️ Профиль", callback_data="profile:menu"),
        InlineKeyboardButton(text="🔧 Настройки", callback_data="settings:menu")
    )

    text = (
        f"👋 **Привет, {escape_html(callback.from_user.first_name)}!**\n\n"
        f"🤖 **Arbitrage Bot** — отслеживай и торгуй арбитражными спредами.\n\n"
        f"📊 **Режим:** {'🟢 Активен' if user.auto_trade_mode else '🔴 Выключен'}\n"
        f"💰 **Баланс:** {user.total_balance:.2f} USDT\n"
        f"🔔 **Алерты:** {'🟢 Вкл' if user.alerts_enabled else '🔴 Выкл'}\n\n"
        f"Выбери раздел:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "menu:back")
async def back_to_main(callback: CallbackQuery, user: UserSettings):
    """Назад в главное меню"""
    await show_main_menu(callback, user)

# ==================== SPREADS MENU ====================

@callbacks_router.callback_query(F.data == "spreads:menu")
async def show_spreads_menu(callback: CallbackQuery, user: UserSettings):
    """Меню спредов"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔥 Активные спреды", callback_data="spreads:active"),
        InlineKeyboardButton(text="⚙️ Настройки алертов", callback_data="alerts:settings")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Мои алерты", callback_data="alerts:list"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    settings_text = f"🔔 {'Вкл' if user.alerts_enabled else 'Выкл'} | 🎯 {user.min_spread_threshold:.1f}%"

    text = (
        "**📊 Меню спредов**\n\n"
        f"📍 **Настройки:** {settings_text}\n"
        f"📈 **Биржи:** {', '.join(user.selected_exchanges) if user.selected_exchanges else 'Все'}\n\n"
        f"Выбери действие:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "spreads:active")
async def show_active_spreads(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Показать активные спреды"""
    await callback.answer()

    if not scanner:
        await callback.message.edit_text(
            "⏳ **Сканер инициализируется...**\n\nПопробуй через несколько секунд.",
            reply_markup=InlineKeyboardBuilder().button(text="🔄 Обновить", callback_data="spreads:active").as_markup()
        )
        return

    try:
        spreads = await scanner.get_top_spreads(20)

        if not spreads:
            await callback.message.edit_text(
                "😕 **Нет активных спредов**\n\n"
                "Порог может быть слишком высоким или биржи не отвечают.",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return

        # Подписываем пользователя на алерты с правильным порогом из БД
        await subscribe_user_to_alerts(user.user_id, scanner, db)

        text = f"**🔥 Топ-{len(spreads)} спредов** (порог: {user.min_spread_threshold:.1f}%)\n\n"

        for i, spread in enumerate(spreads[:10], 1):
            symbol = spread.get('symbol', 'N/A')
            spread_val = spread.get('spread', 0)
            buy_ex = spread.get('buy_exchange', 'N/A')
            sell_ex = spread.get('sell_exchange', 'N/A')
            buy_px = spread.get('buy_price', 0)
            sell_px = spread.get('sell_price', 0)

            text += (
                f"{i}. **{escape_html(symbol)}**: {spread_val:.2f}%\n"
                f"  📉 {escape_html(buy_ex)}: {buy_px:.6f}\n"
                f"  📈 {escape_html(sell_ex)}: {sell_px:.6f}\n\n"
            )

        # Добавляем кнопки торговли для каждого спреда (макс 5)
        trade_count = 0
        for spread in spreads[:5]:
            sym = spread.get('symbol', 'N/A')
            bex = spread.get('buy_exchange', 'N/A')
            sex = spread.get('sell_exchange', 'N/A')
            builder.row(
                InlineKeyboardButton(text=f"🚀 {sym[:12]}", callback_data=f"trade:open:{sym}:{bex}:{sex}"),
                InlineKeyboardButton(text="📈 Лонг", callback_data=f"trade:open_long:{sym}:{bex}"),
                InlineKeyboardButton(text="📉 Шорт", callback_data=f"trade:open_short:{sym}:{sex}"),
                InlineKeyboardButton(text="🔍", callback_data=f"trade:details:{sym}:{bex}:{sex}")
            )
            trade_count += 1

        builder.row(
            InlineKeyboardButton(text="🔄 Обновить", callback_data="spreads:active"),
            InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
        )

        await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())

    except Exception as e:
        logger.error(f"Error showing spreads: {e}")
        await callback.message.edit_text(
            f"❌ **Ошибка:** {escape_html(str(e))[:100]}",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

# ==================== ALERTS ====================

@callbacks_router.callback_query(F.data == "alerts:settings")
async def show_alert_settings(callback: CallbackQuery, user: UserSettings):
    """Настройки алертов"""
    await callback.answer()

    builder = InlineKeyboardBuilder()

    # Вкл/Выкл
    builder.row(
        InlineKeyboardButton(
            text=f"🔔 {'🟢 Вкл' if user.alerts_enabled else '🔴 Выкл'}",
            callback_data="alerts:toggle"
        )
    )

    # Пороги спредов
    thresholds = [0.1, 0.2, 0.5, 1.0, 2.0, 5.0]
    row = []
    for th in thresholds:
        mark = "✅" if abs(user.min_spread_threshold - th) < 0.05 else ""
        row.append(InlineKeyboardButton(
            text=f"{mark} {th:.1f}%",
            callback_data=f"alerts:threshold:{th}"
        ))
        if len(row) == 3:
            builder.row(*row)
            row = []
    if row:
        builder.row(*row)

    # Типы арбитража
    builder.row(
        InlineKeyboardButton(
            text=f"🔄 Межбиржевой: {'🟢' if user.inter_exchange_enabled else '🔴'}",
            callback_data="alerts:toggle_inter"
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=f"📊 Базис: {'🟢' if user.basis_arbitrage_enabled else '🔴'}",
            callback_data="alerts:toggle_basis"
        )
    )

    builder.row(
        InlineKeyboardButton(text="💾 Сохранить", callback_data="alerts:save"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**⚙️ Настройки алертов**\n\n"
        f"🔔 **Статус:** {'🟢 Вкл' if user.alerts_enabled else '🔴 Выкл'}\n"
        f"🎯 **Порог:** {user.min_spread_threshold:.1f}%\n"
        f"🔄 **Межбиржевой:** {'🟢 Вкл' if user.inter_exchange_enabled else '🔴 Выкл'}\n"
        f"📊 **Базис:** {'🟢 Вкл' if user.basis_arbitrage_enabled else '🔴 Выкл'}\n\n"
        f"Выбери настройки:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "alerts:toggle")
async def toggle_alerts(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Вкл/выкл алерты"""
    user.alerts_enabled = not user.alerts_enabled
    if db:
        await db.update_user(user)
    await show_alert_settings(callback, user)

@callbacks_router.callback_query(F.data.startswith("alerts:threshold:"))
async def set_alert_threshold(callback: CallbackQuery, user: UserSettings, state: FSMContext, db: Database = None):
    """Установить порог алертов"""
    await callback.answer()
    try:
        threshold = float(callback.data.split(":")[2])
        user.min_spread_threshold = threshold
        if db:
            await db.update_user(user)
        await show_alert_settings(callback, user)
    except Exception as e:
        logger.error(f"Error setting threshold: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

@callbacks_router.callback_query(F.data == "alerts:toggle_inter")
async def toggle_inter_exchange(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Переключить межбиржевой арбитраж"""
    user.inter_exchange_enabled = not user.inter_exchange_enabled
    if db:
        await db.update_user(user)
    await show_alert_settings(callback, user)

@callbacks_router.callback_query(F.data == "alerts:toggle_basis")
async def toggle_basis_arbitrage(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Переключить базисный арбитраж"""
    user.basis_arbitrage_enabled = not user.basis_arbitrage_enabled
    if db:
        await db.update_user(user)
    await show_alert_settings(callback, user)

@callbacks_router.callback_query(F.data == "alerts:save")
async def save_alert_settings(callback: CallbackQuery, user: UserSettings, db: Database = None, scanner=None):
    """Сохранить настройки алертов"""
    await callback.answer("✅ Сохранено!", show_alert=True)

    # Обновляем порог в сканере если есть
    if scanner and user.user_id:
        scanner.set_user_threshold(user.user_id, user.min_spread_threshold)
        logger.info(f"Updated threshold for user {user.user_id}: {user.min_spread_threshold}%")

    await show_spreads_menu(callback, user)

async def subscribe_user_to_alerts(user_id: int, scanner, db: Database = None):
    """Подписка пользователя на алерты с загрузкой порога из БД"""
    if not scanner:
        return

    # Проверяем, не заблокировал ли пользователь бота
    if db:
        try:
            user = await db.get_user(user_id)
            if user and user.bot_blocked:
                logger.info(f"User {user_id} blocked the bot, skipping subscription")
                return
        except Exception:
            pass

    existing = [s for s in scanner.subscribers if isinstance(s, tuple) and s[1] == user_id]
    if existing:
        return

    # Загружаем порог пользователя из БД при подписке - BUG 5 FIX
    if db:
        try:
            user = await db.get_user(user_id)
            if user:
                # Use min_spread_threshold field directly, not alert_settings dict
                min_spread = user.min_spread_threshold
                scanner.set_user_threshold(user_id, min_spread)
                logger.info(f"User {user_id} threshold loaded from DB: {min_spread}%")
        except Exception as e:
            logger.error(f"Failed to load user threshold: {e}")

    scanner.subscribe(send_spread_alert, user_id)
    logger.info(f"User {user_id} subscribed to spread alerts")

# ИСПРАВЛЕНО: Убран неработающий from bot import bot, используем _bot

async def _mark_user_blocked(user_id: int):
    """Пометить пользователя как заблокировавшего бота и отписать от алертов"""
    global _blocked_users_cache
    _blocked_users_cache.add(user_id)

    try:
        from services.spread_scanner import spread_scanner
        if spread_scanner:
            spread_scanner.unsubscribe(user_id)
            spread_scanner._blocked_subscribers.add(user_id)
            logger.info(f"User {user_id} unsubscribed from alerts due to bot block")
    except Exception:
        pass

    try:
        db = Database()
        await db.initialize()
        try:
            user = await db.get_user(user_id)
            if user and not user.bot_blocked:
                user.bot_blocked = True
                await db.update_user(user)
                logger.info(f"User {user_id} marked as bot_blocked in DB")
        finally:
            await db.close()
    except Exception as e:
        logger.warning(f"Failed to mark user {user_id} as blocked in DB: {e}")


async def send_spread_alert(spread_info, user_id: int):
    """Отправка алерта пользователю

    Args:
        spread_info: SpreadAlert объект или dict с информацией о спреде
        user_id: ID пользователя для отправки
    """
    global _blocked_users_cache

    # Пропускаем заблокированных пользователей (кэш)
    if user_id in _blocked_users_cache:
        return
    # Check user alert settings from DB before sending
        try:
            db_chk = Database()
            await db_chk.initialize()
            try:
                alert_user = await db_chk.get_user(user_id)
                if not alert_user or not alert_user.alerts_enabled:
                    return
                if hasattr(spread_info, 'symbol'):
                    chk_spread = spread_info.spread_percent
                    chk_type = 'basis' if 'basis' in str(getattr(spread_info, 'arbitrage_type', '')).lower() else 'inter'
                else:
                    chk_spread = spread_info.get('spread', 0)
                    chk_type = spread_info.get('type', 'inter')
                if chk_spread < alert_user.min_spread_threshold:
                    return
                if chk_type == 'basis' and not alert_user.basis_arbitrage_enabled:
                    return
                if chk_type == 'inter' and not alert_user.inter_exchange_enabled:
                    return
            finally:
                await db_chk.close()
        except Exception as e:
            logger.error(f"Alert settings check error: {e}")
            return


    try:
        global _bot
        if _bot is None:
            logger.error("Bot not initialized, cannot send alert")
            return

        # Определяем тип spread_info (dict или объект SpreadAlert)
        if hasattr(spread_info, 'symbol'):
            # Это объект SpreadAlert (NamedTuple)
            symbol = spread_info.symbol
            spread = spread_info.spread_percent
            buy_ex = spread_info.buy_exchange
            sell_ex = spread_info.sell_exchange

            # buy_price и sell_price - это PriceData объекты
            buy_px = spread_info.buy_price.last_price if spread_info.buy_price else 0
            sell_px = spread_info.sell_price.last_price if spread_info.sell_price else 0

            # Определяем тип по arbitrage_type
            spread_type = 'basis' if 'basis' in str(spread_info.arbitrage_type).lower() else 'inter'
        else:
            # Это dict
            symbol = spread_info.get('symbol', 'N/A')
            spread = spread_info.get('spread', 0)
            buy_ex = spread_info.get('buy_exchange', 'N/A')
            sell_ex = spread_info.get('sell_exchange', 'N/A')
            buy_px = spread_info.get('buy_price', 0)
            sell_px = spread_info.get('sell_price', 0)
            spread_type = spread_info.get('type', 'inter')

        if spread_type == 'basis':
            text = (
                f"📊 **БАЗИСНЫЙ АРБИТРАЖ**\n\n"
                f"💎 **{escape_html(symbol)}**\n"
                f"📈 **Спред:** {spread:.2f}%\n\n"
                f"📉 **Покупка:** {escape_html(buy_ex)}\n"
                f"   {buy_px:.6f} USDT\n\n"
                f"📈 **Продажа:** {escape_html(sell_ex)}\n"
                f"   {sell_px:.6f} USDT"
            )
        else:
            text = (
                f"🚨 **АРБИТРАЖНЫЙ СПРЕД!**\n\n"
                f"💎 **{escape_html(symbol)}**\n"
                f"📈 **Спред:** {spread:.2f}%\n\n"
                f"📉 **Покупка на:** {escape_html(buy_ex)}\n"
                f"   {buy_px:.6f} USDT\n\n"
                f"📈 **Продажа на:** {escape_html(sell_ex)}\n"
                f"   {sell_px:.6f} USDT"
            )

        # Создаем клавиатуру для торговли
        trade_keyboard = InlineKeyboardBuilder()
        trade_keyboard.row(
            InlineKeyboardButton(text="⚡ Открыть сделку", callback_data=f"trade:open:{symbol}:{buy_ex}:{sell_ex}")
        )
        trade_keyboard.row(
            InlineKeyboardButton(text=f"📈 Лонг {buy_ex[:10]}", callback_data=f"trade:open_long:{symbol}:{buy_ex}"),
            InlineKeyboardButton(text=f"📉 Шорт {sell_ex[:10]}", callback_data=f"trade:open_short:{symbol}:{sell_ex}")
        )
        trade_keyboard.row(
            InlineKeyboardButton(text="🔍 Детали", callback_data=f"trade:details:{symbol}:{buy_ex}:{sell_ex}"),
            InlineKeyboardButton(text="❌ Пропустить", callback_data="trade:skip")
        )

        # Явно конвертируем user_id в int
        await _bot.send_message(
            chat_id=int(user_id),
            text=text,
            reply_markup=trade_keyboard.as_markup(),
            parse_mode=ParseMode.HTML
        )

    except TelegramForbiddenError as e:
        # Пользователь заблокировал бота — помечаем и отписываем
        error_msg = str(e).lower()
        if "bot was blocked" in error_msg or "blocked" in error_msg:
            logger.info(f"User {user_id} blocked the bot, marking and unsubscribing")
            await _mark_user_blocked(user_id)
        else:
            logger.warning(f"Telegram forbidden for user {user_id}: {e}")
    except Exception as e:
        # Другие ошибки — проверяем на блокировку
        error_msg = str(e).lower()
        if "bot was blocked" in error_msg or "blocked by the user" in error_msg:
            logger.info(f"User {user_id} blocked the bot, marking and unsubscribing")
            await _mark_user_blocked(user_id)
        else:
            logger.error(f"Error sending alert to user {user_id}: {e}")

@callbacks_router.callback_query(F.data == "alerts:list")
async def show_user_alerts(callback: CallbackQuery, user: UserSettings):
    """Показать список алертов пользователя"""
    await callback.answer()

    text = (
        "**📋 Мои алерты**\n\n"
        f"🔔 **Статус:** {'🟢 Вкл' if user.alerts_enabled else '🔴 Выкл'}\n"
        f"🎯 **Порог:** {user.min_spread_threshold:.1f}%\n"
        f"🔄 **Межбиржевой:** {'🟢' if user.inter_exchange_enabled else '🔴'}\n"
        f"📊 **Базис:** {'🟢' if user.basis_arbitrage_enabled else '🔴'}\n\n"
        "Алерты приходят в реальном времени при появлении спредов."
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="alerts:settings"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== PROFILE MENU ====================

@callbacks_router.callback_query(F.data == "profile:menu")
async def show_profile_menu(callback: CallbackQuery, user: UserSettings):
    """Меню профиля"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💼 Мои биржи", callback_data="profile:exchanges"),
        InlineKeyboardButton(text="💰 Баланс", callback_data="profile:balance")
    )
    builder.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="profile:stats"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    exchanges = ', '.join(user.selected_exchanges) if user.selected_exchanges else 'Не выбраны'
    api_count = len(user.api_keys) if user.api_keys else 0

    text = (
        f"**⚙️ Профиль**\n\n"
        f"👤 **ID:** `{user.user_id}`\n"
        f"💰 **Баланс:** {user.total_balance:.2f} USDT\n"
        f"🏦 **Биржи:** {escape_html(exchanges)}\n"
        f"🔑 **API ключей:** {api_count}\n\n"
        f"Выбери действие:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "profile:exchanges")
async def show_exchanges(callback: CallbackQuery, user: UserSettings, state: FSMContext):
    """Управление биржами"""
    await callback.answer()

    builder = InlineKeyboardBuilder()

    for ex in AVAILABLE_EXCHANGES:
        is_connected = ex in (user.selected_exchanges or [])
        status = "🟢" if is_connected else "⚪"
        builder.button(text=f"{status} {ex.upper()}", callback_data=f"exchanges:toggle:{ex}")

    builder.adjust(2)

    builder.row(
        InlineKeyboardButton(text="🔑 Добавить API", callback_data="exchanges:add_api"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    exchanges = ', '.join(user.selected_exchanges) if user.selected_exchanges else 'Не выбраны'

    text = (
        f"**💼 Мои биржи**\n\n"
        f"🟢 **Активные:** {escape_html(exchanges)}\n\n"
        f"Нажми на биржу чтобы подключить/отключить:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ИСПРАВЛЕНО: Добавлен параметр state: FSMContext
@callbacks_router.callback_query(F.data.startswith("exchanges:toggle:"))
async def toggle_exchange(callback: CallbackQuery, user: UserSettings, state: FSMContext, db: Database = None):
    """Подключить/отключить биржу"""
    exchange_id = callback.data.split(":")[2]

    if user.selected_exchanges is None:
        user.selected_exchanges = []

    if exchange_id in user.selected_exchanges:
        user.selected_exchanges.remove(exchange_id)
    else:
        user.selected_exchanges.append(exchange_id)

    if db:
        await db.update_user(user)

    # ИСПРАВЛЕНО: Передаем state в show_exchanges
    await show_exchanges(callback, user, state)

@callbacks_router.callback_query(F.data == "exchanges:add_api")
async def add_exchange_api(callback: CallbackQuery, state: FSMContext):
    """Начать добавление API ключа"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    for ex in AVAILABLE_EXCHANGES:
        builder.button(text=ex.upper(), callback_data=f"api:add:{ex}")
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="📱 Меню", callback_data="menu:main"))

    await callback.message.edit_text(
        "**🔑 Выбери биржу:**",
        reply_markup=builder.as_markup()
    )

@callbacks_router.callback_query(F.data.startswith("api:add:"))
async def start_api_input(callback: CallbackQuery, state: FSMContext):
    """Начать ввод API ключа"""
    exchange_id = callback.data.split(":")[2]
    user_id = callback.from_user.id

    logger.info(f"[FSM] start_api_input: user={user_id}, exchange={exchange_id}")

    if not validate_exchange(exchange_id):
        await callback.answer("❌ Неверная биржа", show_alert=True)
        return

    # Clear any previous state first to avoid stale data
    await state.clear()
    logger.info(f"[FSM] State cleared for user={user_id}")

    # Set the state FIRST, then update data
    await state.set_state(SetupStates.waiting_for_api_key)
    logger.info(f"[FSM] State set to waiting_for_api_key for user={user_id}")

    await state.update_data(current_exchange=exchange_id, step='api_key')
    logger.info(f"[FSM] Data updated: current_exchange={exchange_id}, step=api_key for user={user_id}")

    # Verify data was saved
    verify_data = await state.get_data()
    logger.info(f"[FSM] Verification - stored data: {verify_data} for user={user_id}")

    await callback.message.edit_text(
        f"**🔑 Добавление API для {escape_html(exchange_id.upper())}**\n\n"
        f"Введи **API Key**:\n\n"
        f"_(Ключ будет сохранен безопасно.)_",
        reply_markup=InlineKeyboardBuilder().button(text="❌ Отмена", callback_data="menu:main").as_markup()
    )

# ==================== ИСПРАВЛЕННАЯ ФУНКЦИЯ БАЛАНСА ====================
# ИСПРАВЛЕНО: Теперь баланс обновляется с реальных бирж через API
@callbacks_router.callback_query(F.data == "profile:balance")
async def show_balance(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Показать баланс - с обновлением с бирж"""
    await callback.answer("⏳ Обновляю баланс...")

    # Пробуем получить реальные балансы с подключенных бирж
    total_balance = 0.0
    available_balance = 0.0
    locked_balance = 0.0
    exchange_balances = []

    if user.api_keys:
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
                    # Кешируем баланс в пользователе
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
    if db:
        try:
            await db.update_user(user)
        except Exception as e:
            logger.warning(f"Failed to save balances to DB: {e}")

    # Формируем текст
    text = (
        f"**💰 Баланс**\n\n"
        f"📊 **Общий:** {total_balance:.2f} USDT\n"
        f"💵 **Доступно:** {available_balance:.2f} USDT\n"
        f"🔒 **В ордерах:** {locked_balance:.2f} USDT\n\n"
    )

    if exchange_balances:
        text += "**По биржам:**\n"
        for ex, bal in exchange_balances:
            if bal is not None:
                text += f"• {ex.upper()}: {bal:.2f} USDT\n"
            else:
                text += f"• {ex.upper()}: ❌ ошибка подключения\n"
    else:
        text += "_Нет подключенных бирж с API ключами._\n"
        text += "Добавьте API ключи в Профиль → Мои биржи."

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="profile:balance"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== END ИСПРАВЛЕННАЯ ФУНКЦИЯ БАЛАНСА ====================

@callbacks_router.callback_query(F.data == "profile:stats")
async def show_stats(callback: CallbackQuery, user: UserSettings):
    """Показать статистику"""
    await callback.answer()

    text = (
        f"**📊 Статистика**\n\n"
        f"🎯 **Сделок:** {user.total_trades}\n"
        f"✅ **Успешных:** {user.successful_trades}\n"
        f"❌ **Неудачных:** {user.failed_trades}\n"
        f"💰 **Прибыль:** {user.total_profit:.2f} USDT\n\n"
        f"_(Статистика с момента регистрации.)_"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Меню", callback_data="menu:main")
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== SETTINGS MENU ====================

@callbacks_router.callback_query(F.data == "settings:menu")
async def show_settings_menu(callback: CallbackQuery, user: UserSettings):
    """Меню настроек"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🌐 Язык", callback_data="settings:lang"),
        InlineKeyboardButton(text="🔔 Уведомления", callback_data="settings:notifications")
    )
    builder.row(
        InlineKeyboardButton(text="⚙️ Расширенные", callback_data="settings:advanced"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**⚙️ Настройки**\n\n"
        f"🌐 **Язык:** Русский\n"
        f"🔔 **Уведомления:** {'🟢 Вкл' if user.notifications_enabled else '🔴 Выкл'}\n\n"
        f"Выбери раздел:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== MONITORING MENU ====================

@callbacks_router.callback_query(F.data == "monitoring:menu")
async def show_monitoring_menu(callback: CallbackQuery, user: UserSettings):
    """Меню мониторинга"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📈 Курсы", callback_data="monitoring:prices"),
        InlineKeyboardButton(text="📊 Объемы", callback_data="monitoring:volumes")
    )
    builder.row(
        InlineKeyboardButton(text="🔥 Спреды", callback_data="monitoring:spreads"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**📈 Мониторинг рынка**\n\n"
        "Отслеживай цены, объемы и спреды в реальном времени.\n\n"
        "Выбери действие:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== AUTO TRADE MENU ====================

@callbacks_router.callback_query(F.data == "auto_trade:menu")
async def show_auto_trade_menu(callback: CallbackQuery, user: UserSettings):
    """Меню авто-торговли"""
    await callback.answer()

    status = "🟢 Активен" if user.auto_trade_mode else "🔴 Выключен"

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=f"{'🔴 Выкл' if user.auto_trade_mode else '🟢 Вкл'} Авто-торговлю",
            callback_data="auto_trade:toggle"
        )
    )
    builder.row(
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="auto_trade:settings"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**🚀 Авто-торговля**\n\n"
        f"📍 **Статус:** {status}\n"
        f"💰 **Объем сделки:** {user.trade_amount} USDT\n"
        f"⚡ **Плечо:** {user.leverage}x\n\n"
        f"Бот автоматически открывает позиции при спредах ≥ {user.min_spread_threshold:.1f}%"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "auto_trade:toggle")
async def toggle_auto_trade(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Вкл/выкл авто-торговлю"""
    user.auto_trade_mode = not user.auto_trade_mode
    if db:
        await db.update_user(user)

    status = "ВКЛЮЧЕНА" if user.auto_trade_mode else "ВЫКЛЮЧЕНА"
    await callback.answer(f"🚀 Авто-торговля {status}!", show_alert=True)
    await show_auto_trade_menu(callback, user)

@callbacks_router.callback_query(F.data == "auto_trade:settings")
async def show_auto_trade_settings(callback: CallbackQuery, user: UserSettings):
    """Настройки авто-торговли"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💰 Объем", callback_data="auto_trade:amount"),
        InlineKeyboardButton(text="⚡ Плечо", callback_data="auto_trade:leverage")
    )
    builder.row(
        InlineKeyboardButton(text="🎯 Порог", callback_data="auto_trade:threshold"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**⚙️ Настройки авто-торговли**\n\n"
        f"💰 **Объем сделки:** {user.trade_amount} USDT\n"
        f"⚡ **Плечо:** {user.leverage}x\n"
        f"🎯 **Порог спреда:** {user.min_spread_threshold:.1f}%\n\n"
        f"Выбери параметр:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "auto_trade:amount")
async def set_trade_amount(callback: CallbackQuery, state: FSMContext):
    """Установить объем сделки"""
    await callback.answer()
    # ИСПРАВЛЕНО: SetupStates вместо BotStates
    await state.set_state(SetupStates.waiting_for_trade_amount)
    await callback.message.edit_text(
        "**💰 Введи объем сделки (USDT):**\n\n"
        "Например: 100",
        reply_markup=InlineKeyboardBuilder().button(text="❌ Отмена", callback_data="menu:main").as_markup()
    )

@callbacks_router.callback_query(F.data == "auto_trade:leverage")
async def set_leverage(callback: CallbackQuery, state: FSMContext):
    """Установить плечо"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    for lev in [1, 2, 3, 5, 10, 20]:
        builder.button(text=f"{lev}x", callback_data=f"auto_trade:leverage:{lev}")
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="📱 Меню", callback_data="menu:main"))

    await callback.message.edit_text(
        "**⚡ Выбери плечо:**",
        reply_markup=builder.as_markup()
    )

@callbacks_router.callback_query(F.data.startswith("auto_trade:leverage:"))
async def process_leverage(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Обработка выбора плеча"""
    try:
        leverage = int(callback.data.split(":")[2])
        user.leverage = leverage
        if db:
            await db.update_user(user)
        await callback.answer(f"⚡ Плечо: {leverage}x", show_alert=True)
        await show_auto_trade_settings(callback, user)
    except Exception as e:
        logger.error(f"Error setting leverage: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

# ==================== POSITIONS MENU ====================

@callbacks_router.callback_query(F.data == "positions:menu")
async def show_positions_menu(callback: CallbackQuery, user: UserSettings):
    """Меню позиций"""
    await callback.answer()

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📊 Открытые", callback_data="positions:open"),
        InlineKeyboardButton(text="📈 История", callback_data="positions:history")
    )
    builder.row(
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**📉 Управление позициями**\n\n"
        "Просматривай открытые позиции и историю сделок.\n\n"
        "Выбери раздел:"
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "positions:open")
@callbacks_router.callback_query(F.data == "positions:open")
async def show_open_positions(callback: CallbackQuery, user: UserSettings, db=None):
    """Show open positions from DB"""
    await callback.answer()
    if not db:
        await callback.message.edit_text(
            "**📊 Open Positions**\n\n_No positions._\n\nOpen a trade via 📈 Monitoring → Spreads.",
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="📈 Monitoring", callback_data="monitoring:menu"),
                InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")
            ).as_markup()
        )
        return
    try:
        open_trades = await db.get_open_trades(user.user_id)
        if not open_trades:
            await callback.message.edit_text(
                "**📊 Open Positions**\n\n_No positions._\n\nOpen a trade via 📈 Monitoring → Spreads or wait for auto-trading.",
                reply_markup=InlineKeyboardBuilder().row(
                    InlineKeyboardButton(text="📈 Monitoring", callback_data="monitoring:menu"),
                    InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")
                ).as_markup()
            )
            return
        text = f"**📊 Open Positions ({len(open_trades)})**\n\n"
        builder = InlineKeyboardBuilder()
        for trade in open_trades[:10]:
            pnl_emoji = "🟢" if (trade.pnl_usd or 0) >= 0 else "🔴"
            pnl_str = f"{pnl_emoji} ${trade.pnl_usd or 0:.2f}"
            test_badge = " [TEST]" if trade.metadata.get('test_mode') else ""
            text += (
                f"**#{trade.id}{test_badge}** {trade.symbol}\n"
                f"  Entry spread: {trade.entry_spread:.2f}%\n"
                f"  Size: ${trade.size_usd:.2f}\n"
                f"  PnL: {pnl_str}\n"
                f"  {trade.long_exchange} ↔ {trade.short_exchange}\n\n"
            )
            builder.row(
                InlineKeyboardButton(
                    text=f"❌ Close #{trade.id} {trade.symbol}",
                    callback_data=f"trade:close:{trade.id}"
                )
            )
        builder.row(
            InlineKeyboardButton(text="🔄 Refresh", callback_data="positions:open"),
            InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")
        )
        await callback.message.edit_text(text[:3500], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Error showing open positions: {e}")
        await callback.message.edit_text(
            f"**❌ Error:** {escape_html(str(e))[:100]}",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Menu", callback_data="menu:main").as_markup()
        )

@callbacks_router.callback_query(F.data == "positions:history")
async def show_positions_history(callback: CallbackQuery, user: UserSettings, db=None):
    """Trade history from DB"""
    await callback.answer()
    if not db:
        text = (
            f"**📈 Trade History**\n\n"
            f"🎯 **Total:** {user.total_trades}\n"
            f"✅ **Wins:** {user.successful_trades}\n"
            f"❌ **Losses:** {user.failed_trades}\n"
            f"💰 **Profit:** {user.total_profit:.2f} USDT"
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="📱 Menu", callback_data="menu:main")
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        return
    try:
        stats = await db.get_trade_stats(user.user_id)
        async with db._conn.execute(
            "SELECT * FROM trades WHERE user_id = ? AND status = 'closed' ORDER BY closed_at DESC LIMIT 5",
            (user.user_id,)
        ) as cursor:
            rows = await cursor.fetchall()
        text = f"**📈 Trade History**\n\n🎯 **Total closed:** {stats['total_trades']}\n💰 **Total PnL:** ${stats['total_pnl']:.2f}\n\n"
        if rows:
            text += "**Recent trades:**\n"
            for row in rows[:5]:
                pnl = row['pnl_usd'] or 0
                pnl_emoji = "🟢" if pnl >= 0 else "🔴"
                test_badge = " [T]" if 'test' in str(row['metadata']).lower() else ""
                text += f"#{row['id']}{test_badge} {row['symbol']} | {pnl_emoji} ${pnl:.2f}\n"
        else:
            text += "_No closed trades yet._"
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="📊 Positions", callback_data="positions:menu"),
            InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")
        )
        await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Error showing position history: {e}")
        await callback.message.edit_text(
            f"**❌ Error:** {escape_html(str(e))[:100]}",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Menu", callback_data="menu:main").as_markup()
        )

# ==================== MONITORING HANDLERS ====================

@callbacks_router.callback_query(F.data == "monitoring:prices")
async def show_monitoring_prices(callback: CallbackQuery, user: UserSettings, scanner=None):
    """Show current futures prices"""
    await callback.answer()
    if not scanner:
        await callback.message.edit_text("**⏳ Scanner initializing...**\n\nTry again in a few seconds.",
            reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
        return
    try:
        prices = await scanner.get_prices_copy()
        if not prices:
            await callback.message.edit_text("**😕 No price data**\n\nScanner is still collecting data.",
                reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
            return
        sorted_symbols = sorted(prices.items(), key=lambda x: sum(m.get('futures', type('o', (), {'volume_24h': 0})()).volume_24h for m in x[1].values() if 'futures' in m), reverse=True)[:15]
        text = "**📈 Current Futures Prices**\n\n"
        for symbol, exchanges in sorted_symbols:
            pl = [f"{ex[:3]}: ${m['futures'].last_price:,.2f}" for ex, m in exchanges.items() if 'futures' in m and m['futures'].last_price > 0]
            if pl:
                text += f"**{symbol}**: {' | '.join(pl)}\n"
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="monitoring:prices"), InlineKeyboardButton(text="🔙 Back", callback_data="monitoring:menu"))
        await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Error showing prices: {e}")
        await callback.message.edit_text(f"**❌ Error:** {escape_html(str(e))[:100]}", reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())

@callbacks_router.callback_query(F.data == "monitoring:volumes")
async def show_monitoring_volumes(callback: CallbackQuery, user: UserSettings, scanner=None):
    """Show trading volumes"""
    await callback.answer()
    if not scanner:
        await callback.message.edit_text("**⏳ Scanner initializing...**\n\nTry again in a few seconds.",
            reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
        return
    try:
        prices = await scanner.get_prices_copy()
        if not prices:
            await callback.message.edit_text("**😕 No volume data**\n\nScanner is still collecting data.",
                reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
            return
        volumes = []
        for symbol, exchanges in prices.items():
            for ex, m in exchanges.items():
                if 'futures' in m and m['futures'].volume_24h > 0:
                    volumes.append((symbol, ex, m['futures'].volume_24h))
        volumes.sort(key=lambda x: x[2], reverse=True)
        text = "**📊 Top 24h Volumes (Futures)**\n\n"
        for i, (s, e, v) in enumerate(volumes[:20], 1):
            text += f"{i}. **{s}** ({e[:3]}): ${v:,.0f}\n"
        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="monitoring:volumes"), InlineKeyboardButton(text="🔙 Back", callback_data="monitoring:menu"))
        await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Error showing volumes: {e}")
        await callback.message.edit_text(f"**❌ Error:** {escape_html(str(e))[:100]}", reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())

@callbacks_router.callback_query(F.data == "monitoring:spreads")
async def show_monitoring_spreads(callback: CallbackQuery, user: UserSettings, scanner=None):
    """Show current spreads with trade open buttons"""
    await callback.answer()
    if not scanner:
        await callback.message.edit_text("**⏳ Scanner initializing...**\n\nTry again in a few seconds.",
            reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
        return
    try:
        spreads = await scanner.get_top_spreads(15)
        if not spreads:
            await callback.message.edit_text("**🔥 No active spreads**\n\nThreshold may be too high.",
                reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())
            return
        text = f"**🔥 Top-{len(spreads)} Spreads**\n\n"
        builder = InlineKeyboardBuilder()
        for i, sp in enumerate(spreads[:10], 1):
            sym = sp.get('symbol', 'N/A')
            spv = sp.get('spread', 0)
            bx = sp.get('buy_exchange', 'N/A')
            sx = sp.get('sell_exchange', 'N/A')
            bp = sp.get('buy_price', 0)
            sp_ = sp.get('sell_price', 0)
            if hasattr(bp, 'last_price'): bp = bp.last_price
            if hasattr(sp_, 'last_price'): sp_ = sp_.last_price
            text += f"{i}. **{escape_html(sym)}**: {spv:.2f}%\n  📉 {escape_html(bx)}: {bp:.6f}\n  📈 {escape_html(sx)}: {sp_:.6f}\n\n"
            builder.row(InlineKeyboardButton(text=f"⚡ Open {sym} ({spv:.2f}%)", callback_data=f"trade:open:{sym}:{bx}:{sx}:{spv:.4f}"))
        builder.row(InlineKeyboardButton(text="🔄 Refresh", callback_data="monitoring:spreads"), InlineKeyboardButton(text="🔙 Back", callback_data="monitoring:menu"))
        await callback.message.edit_text(text[:3500], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Error showing monitoring spreads: {e}")
        await callback.message.edit_text(f"**❌ Error:** {escape_html(str(e))[:100]}", reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:menu").as_markup())

@callbacks_router.callback_query(F.data.startswith("trade:open:"))
async def handle_trade_open(callback: CallbackQuery, user: UserSettings, scanner=None, db=None):
    """Open trade from monitoring spreads"""
    await callback.answer()
    if not scanner:
        await callback.answer("❌ Scanner not ready", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) < 6:
        await callback.answer("❌ Invalid parameters", show_alert=True)
        return
    symbol = parts[2]
    buy_ex = parts[3]
    sell_ex = parts[4]
    try:
        spread_val = float(parts[5])
    except ValueError:
        spread_val = 0
    if not user.api_keys:
        await callback.answer("❌ Add API keys in Profile first", show_alert=True)
        return
    if not user.is_trading_enabled:
        await callback.answer("❌ Trading is disabled. Enable in settings.", show_alert=True)
        return
    await callback.message.edit_text(f"**⏳ Opening trade...**\n\nPair: {escape_html(symbol)}\nSpread: {spread_val:.2f}%\nLong: {escape_html(buy_ex)}\nShort: {escape_html(sell_ex)}")
    try:
        from services.trading_engine import trading_engine
        spread_key = f"{symbol}:{buy_ex}:{sell_ex}"
        result = await trading_engine.validate_and_open(user, spread_key, scanner.prices, test_mode=user.alert_settings.get('test_mode', True))
        if result.success:
            await callback.message.edit_text(f"**✅ Trade #{result.trade_id} opened!**\n\nPair: {escape_html(symbol)}\nEntry spread: {result.entry_spread:.2f}%\nSize: ${result.position_size:,.2f}\n\nBot is monitoring automatically.",
                reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="📊 My Positions", callback_data="positions:open"), InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")).as_markup())
        else:
            await callback.message.edit_text(f"**❌ Open error:**\n{escape_html(result.error)}\n\nTry again later.",
                reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔄 Retry", callback_data=callback.data), InlineKeyboardButton(text="🔙 Back", callback_data="monitoring:spreads")).as_markup())
    except Exception as e:
        logger.error(f"Error opening trade: {e}")
        await callback.message.edit_text(f"**❌ Error:** {escape_html(str(e))[:200]}", reply_markup=InlineKeyboardBuilder().button(text="🔙 Back", callback_data="monitoring:spreads").as_markup())

@callbacks_router.callback_query(F.data.startswith("trade:close:"))
async def handle_trade_close(callback: CallbackQuery, user: UserSettings, db=None):
    """Close position by ID"""
    await callback.answer()
    try:
        trade_id = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Invalid trade ID", show_alert=True)
        return
    await callback.message.edit_text(f"**⏳ Closing trade #{trade_id}...**")
    try:
        from services.trading_engine import trading_engine
        result = await trading_engine.close_trade_manually(trade_id, user)
        if result.success:
            pnl_str = ""
            if result.metadata and 'pnl' in result.metadata:
                pnl = result.metadata['pnl']
                pnl_str = f"\nPnL: {'🟢 +' if pnl >= 0 else '🔴 '}${pnl:.2f}"
            await callback.message.edit_text(f"**✅ Trade #{trade_id} closed!**{pnl_str}\n\nPosition closed successfully.",
                reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="📊 Positions", callback_data="positions:open"), InlineKeyboardButton(text="📱 Menu", callback_data="menu:main")).as_markup())
        else:
            await callback.message.edit_text(f"**❌ Close error #{trade_id}:**\n{escape_html(result.error)}\n\nIf error persists, position may already be closed.",
                reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔄 Retry", callback_data=callback.data), InlineKeyboardButton(text="📊 Positions", callback_data="positions:open")).as_markup())
    except Exception as e:
        logger.error(f"Error closing trade: {e}")
        await callback.message.edit_text(f"**❌ Error:** {escape_html(str(e))[:200]}", reply_markup=InlineKeyboardBuilder().button(text="📱 Menu", callback_data="menu:main").as_markup())

# ==================== ERROR HANDLING ====================

# ИСПРАВЛЕНО: Правильная сигнатура для aiogram 3.x
@callbacks_router.errors()
async def callback_error_handler(update: Update, exception: Exception):
    """Обработка ошибок колбэков"""
    logger.error(f"Callback error: {exception}")
    # Пытаемся отправить уведомление пользователю
    try:
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.answer("❌ Ошибка обработки", show_alert=True)
        elif hasattr(update, 'message') and update.message:
            await update.message.answer("❌ Произошла ошибка. Попробуйте /start")
    except Exception as e:
        logger.error(f"Failed to send error notification: {e}")


# ==================== TRADE EXECUTION HANDLERS ====================

@callbacks_router.callback_query(F.data.startswith("trade:open:"))
async def handle_trade_open(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Открыть сделку на обеих биржах (лонг + шорт)"""
    await callback.answer()
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.message.answer("❌ Ошибка данных сделки")
        return
    symbol = parts[2]
    buy_ex = parts[3]
    sell_ex = parts[4]
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    open_trades = await db.get_open_trades(user.user_id)
    max_pos = user.risk_settings.get('max_open_positions', 5)
    if len(open_trades) >= max_pos:
        await callback.answer(f"❌ Лимит позиций: {max_pos}", show_alert=True)
        return
    size = min(user.trade_amount, user.risk_settings.get('max_position_usd', 10000))
    if size < 10:
        size = 100
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text=f"✅ Подтвердить ${size:.0f}", callback_data=f"trade:confirm:{symbol}:{buy_ex}:{sell_ex}:{size:.0f}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="trade:cancel")
    )
    keyboard.row(
        InlineKeyboardButton(text="$50", callback_data=f"trade:confirm:{symbol}:{buy_ex}:{sell_ex}:50"),
        InlineKeyboardButton(text="$100", callback_data=f"trade:confirm:{symbol}:{buy_ex}:{sell_ex}:100"),
        InlineKeyboardButton(text="$500", callback_data=f"trade:confirm:{symbol}:{buy_ex}:{sell_ex}:500"),
        InlineKeyboardButton(text="$1000", callback_data=f"trade:confirm:{symbol}:{buy_ex}:{sell_ex}:1000")
    )
    await callback.message.answer(
        f"⚡ **Открытие арбитражной сделки**\n\n"
        f"💎 **{escape_html(symbol)}**\n"
        f"📈 Лонг: {escape_html(buy_ex)}\n"
        f"📉 Шорт: {escape_html(sell_ex)}\n"
        f"💰 Размер: ${size:.0f} (плечо {user.leverage}x)\n\n"
        f"Подтвердите открытие:",
        reply_markup=keyboard.as_markup()
    )

@callbacks_router.callback_query(F.data.startswith("trade:open_long:"))
async def handle_trade_open_long(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Открыть только лонг позицию на одной бирже"""
    await callback.answer()
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.message.answer("❌ Ошибка данных")
        return
    symbol = parts[2]
    exchange = parts[3]
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    open_trades = await db.get_open_trades(user.user_id)
    max_pos = user.risk_settings.get('max_open_positions', 5)
    if len(open_trades) >= max_pos:
        await callback.answer(f"❌ Лимит позиций: {max_pos}", show_alert=True)
        return
    size = min(user.trade_amount, user.risk_settings.get('max_position_usd', 10000))
    if size < 10:
        size = 100
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text=f"✅ Лонг ${size:.0f}", callback_data=f"trade:confirm_long:{symbol}:{exchange}:{size:.0f}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="trade:cancel")
    )
    keyboard.row(
        InlineKeyboardButton(text="$50", callback_data=f"trade:confirm_long:{symbol}:{exchange}:50"),
        InlineKeyboardButton(text="$100", callback_data=f"trade:confirm_long:{symbol}:{exchange}:100"),
        InlineKeyboardButton(text="$500", callback_data=f"trade:confirm_long:{symbol}:{exchange}:500")
    )
    await callback.message.answer(
        f"📈 **Открытие ЛОНГ позиции**\n\n"
        f"💎 **{escape_html(symbol)}**\n"
        f"🏦 Биржа: {escape_html(exchange)}\n"
        f"💰 Размер: ${size:.0f} (плечо {user.leverage}x)\n\n"
        f"Подтвердите открытие:",
        reply_markup=keyboard.as_markup()
    )

@callbacks_router.callback_query(F.data.startswith("trade:open_short:"))
async def handle_trade_open_short(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Открыть только шорт позицию на одной бирже"""
    await callback.answer()
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.message.answer("❌ Ошибка данных")
        return
    symbol = parts[2]
    exchange = parts[3]
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    open_trades = await db.get_open_trades(user.user_id)
    max_pos = user.risk_settings.get('max_open_positions', 5)
    if len(open_trades) >= max_pos:
        await callback.answer(f"❌ Лимит позиций: {max_pos}", show_alert=True)
        return
    size = min(user.trade_amount, user.risk_settings.get('max_position_usd', 10000))
    if size < 10:
        size = 100
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text=f"✅ Шорт ${size:.0f}", callback_data=f"trade:confirm_short:{symbol}:{exchange}:{size:.0f}"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="trade:cancel")
    )
    keyboard.row(
        InlineKeyboardButton(text="$50", callback_data=f"trade:confirm_short:{symbol}:{exchange}:50"),
        InlineKeyboardButton(text="$100", callback_data=f"trade:confirm_short:{symbol}:{exchange}:100"),
        InlineKeyboardButton(text="$500", callback_data=f"trade:confirm_short:{symbol}:{exchange}:500")
    )
    await callback.message.answer(
        f"📉 **Открытие ШОРТ позиции**\n\n"
        f"💎 **{escape_html(symbol)}**\n"
        f"🏦 Биржа: {escape_html(exchange)}\n"
        f"💰 Размер: ${size:.0f} (плечо {user.leverage}x)\n\n"
        f"Подтвердите открытие:",
        reply_markup=keyboard.as_markup()
    )

@callbacks_router.callback_query(F.data.startswith("trade:confirm:"))
async def handle_trade_confirm(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Подтверждение открытия сделки"""
    await callback.answer("⏳ Открываю сделку...")
    parts = callback.data.split(":")
    if len(parts) < 6:
        await callback.message.answer("❌ Ошибка подтверждения")
        return
    symbol = parts[2]
    buy_ex = parts[3]
    sell_ex = parts[4]
    try:
        size = float(parts[5])
    except ValueError:
        size = 100
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    try:
        from services.trading_engine import trading_engine
        spread_key = f"{symbol}:{buy_ex}:{sell_ex}"
        scanner_prices = scanner.prices if scanner and hasattr(scanner, 'prices') else {}
        result = await trading_engine.validate_and_open(
            user=user, spread_key=spread_key, scanner_prices=scanner_prices,
            test_mode=user.alert_settings.get('test_mode', True)
        )
        if result.success:
            user.total_trades += 1
            await db.update_user(user)
            await callback.message.answer(
                f"✅ **Сделка #{result.trade_id} открыта!**\n\n"
                f"💎 {escape_html(symbol)}\n"
                f"📈 Лонг: {escape_html(buy_ex)}\n"
                f"📉 Шорт: {escape_html(sell_ex)}\n"
                f"💰 Размер: ${size:.0f}\n\n"
                f"📊 Отслеживание активировано"
            )
        else:
            await callback.message.answer(
                f"❌ **Не удалось открыть сделку**\n\n"
                f"Причина: {escape_html(result.error or 'Неизвестная ошибка')}"
            )
    except Exception as e:
        logger.error(f"Trade confirm error: {e}")
        await callback.message.answer(f"❌ Ошибка: {escape_html(str(e))[:200]}")

@callbacks_router.callback_query(F.data.startswith("trade:confirm_long:"))
async def handle_trade_confirm_long(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Подтверждение лонг сделки"""
    await callback.answer("⏳ Открываю лонг...")
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.message.answer("❌ Ошибка данных")
        return
    symbol = parts[2]
    exchange = parts[3]
    try:
        size = float(parts[4])
    except ValueError:
        size = 100
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    try:
        from services.trading_engine import trading_engine
        result = await trading_engine.open_single_exchange_trade(
            user=user, symbol=symbol, exchange_id=exchange,
            side='long', size_usd=size,
            test_mode=user.alert_settings.get('test_mode', True)
        )
        if result.success:
            user.total_trades += 1
            await db.update_user(user)
            await callback.message.answer(
                f"✅ **Лонг #{result.trade_id} открыт!**\n\n"
                f"💎 {escape_html(symbol)} @ {escape_html(exchange)}\n"
                f"💰 Размер: ${size:.0f}"
            )
        else:
            await callback.message.answer(f"❌ Ошибка: {escape_html(result.error or 'Не удалось открыть')}")
    except Exception as e:
        logger.error(f"Long confirm error: {e}")
        await callback.message.answer(f"❌ Ошибка: {escape_html(str(e))[:200]}")

@callbacks_router.callback_query(F.data.startswith("trade:confirm_short:"))
async def handle_trade_confirm_short(callback: CallbackQuery, user: UserSettings, scanner=None, db: Database = None):
    """Подтверждение шорт сделки"""
    await callback.answer("⏳ Открываю шорт...")
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.message.answer("❌ Ошибка данных")
        return
    symbol = parts[2]
    exchange = parts[3]
    try:
        size = float(parts[4])
    except ValueError:
        size = 100
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    try:
        from services.trading_engine import trading_engine
        result = await trading_engine.open_single_exchange_trade(
            user=user, symbol=symbol, exchange_id=exchange,
            side='short', size_usd=size,
            test_mode=user.alert_settings.get('test_mode', True)
        )
        if result.success:
            user.total_trades += 1
            await db.update_user(user)
            await callback.message.answer(
                f"✅ **Шорт #{result.trade_id} открыт!**\n\n"
                f"💎 {escape_html(symbol)} @ {escape_html(exchange)}\n"
                f"💰 Размер: ${size:.0f}"
            )
        else:
            await callback.message.answer(f"❌ Ошибка: {escape_html(result.error or 'Не удалось открыть')}")
    except Exception as e:
        logger.error(f"Short confirm error: {e}")
        await callback.message.answer(f"❌ Ошибка: {escape_html(str(e))[:200]}")

@callbacks_router.callback_query(F.data == "trade:cancel")
async def handle_trade_cancel(callback: CallbackQuery):
    """Отмена торговой операции"""
    await callback.answer("❌ Отменено")
    try:
        await callback.message.edit_text(f"{callback.message.text}\n\n_❌ Отменено пользователем_")
    except:
        pass

@callbacks_router.callback_query(F.data.startswith("trade:details:"))
async def handle_trade_details(callback: CallbackQuery, scanner=None):
    """Показать детали спреда"""
    await callback.answer()
    parts = callback.data.split(":")
    if len(parts) < 5:
        return
    symbol = parts[2]
    buy_ex = parts[3]
    sell_ex = parts[4]
    keyboard = InlineKeyboardBuilder()
    keyboard.row(
        InlineKeyboardButton(text="⚡ Открыть сделку", callback_data=f"trade:open:{symbol}:{buy_ex}:{sell_ex}"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )
    await callback.message.answer(
        f"📊 **Детали спреда**\n\n"
        f"💎 {escape_html(symbol)}\n"
        f"📉 Покупка: {escape_html(buy_ex)}\n"
        f"📈 Продажа: {escape_html(sell_ex)}\n\n"
        f"Выберите действие:",
        reply_markup=keyboard.as_markup()
    )

@callbacks_router.callback_query(F.data == "trade:skip")
async def handle_trade_skip(callback: CallbackQuery):
    """Пропустить алерт"""
    await callback.answer("⏭ Пропущено")


# ==================== POSITION MANAGEMENT MENU ====================

@callbacks_router.callback_query(F.data == "positions:menu")
async def show_positions_menu(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Меню управления позициями — как в MetaTrader"""
    await callback.answer()
    if not db:
        await callback.message.edit_text(
            "❌ База данных недоступна",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )
        return
    try:
        open_trades = await db.get_open_trades(user.user_id)
        if not open_trades:
            await callback.message.edit_text(
                "📊 **Нет открытых позиций**\n\n"
                "Откройте сделку через меню Спреды или дождитесь алерта.",
                reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
            )
            return
        builder = InlineKeyboardBuilder()
        text = f"📊 **Открытые позиции ({len(open_trades)})**\n\n"
        for i, trade in enumerate(open_trades[:10], 1):
            side_emoji = "📈" if trade.position_size_long > 0 else "📉" if trade.position_size_short > 0 else "⚡"
            pnl_emoji = "🟢" if (trade.pnl_usd or 0) > 0 else "🔴" if (trade.pnl_usd or 0) < 0 else "⚪"
            pnl_str = f"{pnl_emoji} ${trade.pnl_usd:.2f}" if trade.pnl_usd else "⚪ $0.00"
            text += (
                f"{i}. {side_emoji} **{escape_html(trade.symbol)}** #{trade.id}\n"
                f"   💰 ${trade.size_usd:.0f} | {pnl_str}\n"
                f"   SL: {trade.stop_loss_price:.4f} | TP: {trade.take_profit_price:.4f}\n\n"
            )
            builder.row(
                InlineKeyboardButton(text=f"{side_emoji} #{trade.id}", callback_data=f"position:details:{trade.id}"),
                InlineKeyboardButton(text="🔄 Обновить", callback_data="positions:menu")
            )
        builder.row(
            InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
        )
        await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Positions menu error: {e}")
        await callback.message.edit_text(
            f"❌ Ошибка: {escape_html(str(e))[:100]}",
            reply_markup=InlineKeyboardBuilder().button(text="📱 Меню", callback_data="menu:main").as_markup()
        )

@callbacks_router.callback_query(F.data.startswith("position:details:"))
async def show_position_details(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Детальная карточка позиции с управлением (как в MetaTrader)"""
    await callback.answer()
    if not db:
        return
    try:
        trade_id = int(callback.data.split(":")[2])
        trade_data = await db.get_trade_by_id(trade_id)
        if not trade_data or trade_data['user_id'] != user.user_id:
            await callback.answer("❌ Позиция не найдена", show_alert=True)
            return
        from database.models import Trade
        trade = Trade(**trade_data)
        if trade.status != "open":
            await callback.answer("❌ Позиция уже закрыта", show_alert=True)
            return
        side = trade.metadata.get('side', 'both')
        side_emoji = "📈 ЛОНГ" if side == 'long' else "📉 ШОРТ" if side == 'short' else "⚡ АРБИТРАЖ"
        pnl_emoji = "🟢" if (trade.pnl_usd or 0) > 0 else "🔴" if (trade.pnl_usd or 0) < 0 else "⚪"
        try:
            from datetime import datetime, timezone
            opened = datetime.fromisoformat(trade.opened_at.replace('Z', '+00:00'))
            hours_open = (datetime.now(timezone.utc) - opened).total_seconds() / 3600
            time_str = f"{hours_open:.1f}ч"
        except:
            time_str = "N/A"
        text = (
            f"{'='*30}\n"
            f"{side_emoji} **#{trade.id} {escape_html(trade.symbol)}**\n"
            f"{'='*30}\n\n"
            f"💰 **Размер:** ${trade.size_usd:.2f}\n"
            f"{pnl_emoji} **P&L:** ${trade.pnl_usd:.2f} ({trade.pnl_percent:.2f}%)\n"
            f"⏱ **В позиции:** {time_str}\n\n"
            f"📊 **Входные цены:**\n"
        )
        if trade.entry_price_long > 0:
            text += f"   📈 Лонг: {trade.entry_price_long:.6f}\n"
            text += f"   Текущий: {trade.current_price_long:.6f}\n"
        if trade.entry_price_short > 0:
            text += f"   📉 Шорт: {trade.entry_price_short:.6f}\n"
            text += f"   Текущий: {trade.current_price_short:.6f}\n"
        text += (
            f"\n🛡 **Защита:**\n"
            f"   SL: {trade.stop_loss_price:.6f}\n"
            f"   TP: {trade.take_profit_price:.6f}\n"
        )
        if trade.trailing_enabled:
            text += f"   📊 Trailing: {trade.trailing_stop_price:.6f}\n"
        text += f"\n🔧 **Действия:**"
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="❌ Закрыть", callback_data=f"position:close:{trade.id}"),
            InlineKeyboardButton(text="💰 50%", callback_data=f"position:partial:{trade.id}:50")
        )
        builder.row(
            InlineKeyboardButton(text="🛡 SL", callback_data=f"position:mod_sl:{trade.id}"),
            InlineKeyboardButton(text="🎯 TP", callback_data=f"position:mod_tp:{trade.id}")
        )
        builder.row(
            InlineKeyboardButton(text="25%", callback_data=f"position:partial:{trade.id}:25"),
            InlineKeyboardButton(text="50%", callback_data=f"position:partial:{trade.id}:50"),
            InlineKeyboardButton(text="75%", callback_data=f"position:partial:{trade.id}:75")
        )
        builder.row(
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"position:details:{trade.id}"),
            InlineKeyboardButton(text="📊 Все позиции", callback_data="positions:menu"),
            InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
        )
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    except Exception as e:
        logger.error(f"Position details error: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

@callbacks_router.callback_query(F.data.startswith("position:close:"))
async def handle_position_close(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Закрыть позицию полностью"""
    trade_id = int(callback.data.split(":")[2])
    await callback.answer("⏳ Закрываю позицию...")
    try:
        from services.trading_engine import trading_engine
        result = await trading_engine.close_trade_manually(trade_id, user)
        if result.success:
            pnl = result.metadata.get('pnl', 0) if result.metadata else 0
            pnl_emoji = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
            await callback.message.answer(
                f"✅ **Позиция #{trade_id} закрыта!**\n\n"
                f"{pnl_emoji} P&L: ${pnl:.2f}\n\n"
                f"📊 Статистика обновлена."
            )
            if pnl > 0:
                user.successful_trades += 1
                user.total_profit += pnl
            elif pnl < 0:
                user.failed_trades += 1
            if db:
                await db.update_user(user)
        else:
            await callback.message.answer(
                f"❌ **Не удалось закрыть позицию #{trade_id}**\n\n"
                f"{escape_html(result.error or 'Неизвестная ошибка')}"
            )
    except Exception as e:
        logger.error(f"Close position error: {e}")
        await callback.answer("❌ Ошибка закрытия", show_alert=True)

@callbacks_router.callback_query(F.data.startswith("position:partial:"))
async def handle_position_partial(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Частичное закрытие позиции"""
    parts = callback.data.split(":")
    trade_id = int(parts[2])
    percentage = int(parts[3])
    await callback.answer(f"⏳ Закрываю {percentage}%...")
    try:
        from services.trading_engine import trading_engine
        result = await trading_engine.partial_close(trade_id, user, float(percentage))
        if result.success:
            metadata = result.metadata or {}
            await callback.message.answer(
                f"✅ **Закрыто {percentage}% позиции #{trade_id}**\n\n"
                f"💰 P&L: ${metadata.get('partial_pnl', 0):.2f}\n"
                f"📊 Остаток: ${metadata.get('remaining_size', 0):.2f}"
            )
        else:
            await callback.message.answer(
                f"❌ Ошибка: {escape_html(result.error or 'Не удалось закрыть')}"
            )
    except Exception as e:
        logger.error(f"Partial close error: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

@callbacks_router.callback_query(F.data.startswith("position:mod_sl:"))
async def handle_modify_sl(callback: CallbackQuery, state: FSMContext, db: Database = None):
    """Начать изменение стоп-лосса"""
    await callback.answer()
    trade_id = int(callback.data.split(":")[2])
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    try:
        trade_data = await db.get_trade_by_id(trade_id)
        if not trade_data:
            await callback.answer("❌ Позиция не найдена", show_alert=True)
            return
        current_sl = trade_data.get('stop_loss_price', 0)
        await state.set_state(SetupStates.waiting_for_sl_price)
        await state.update_data(modify_trade_id=trade_id, new_sl_price=None)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="❌ Отмена", callback_data=f"position:details:{trade_id}")
        await callback.message.answer(
            f"🛡 **Изменение Stop-Loss для #{trade_id}**\n\n"
            f"Текущий SL: {current_sl:.6f}\n\n"
            f"Введите новую цену SL (или /cancel для отмены):",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logger.error(f"Modify SL error: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)

@callbacks_router.callback_query(F.data.startswith("position:mod_tp:"))
async def handle_modify_tp(callback: CallbackQuery, state: FSMContext, db: Database = None):
    """Начать изменение тейк-профита"""
    await callback.answer()
    trade_id = int(callback.data.split(":")[2])
    if not db:
        await callback.message.answer("❌ База данных недоступна")
        return
    try:
        trade_data = await db.get_trade_by_id(trade_id)
        if not trade_data:
            await callback.answer("❌ Позиция не найдена", show_alert=True)
            return
        current_tp = trade_data.get('take_profit_price', 0)
        await state.set_state(SetupStates.waiting_for_tp_price)
        await state.update_data(modify_trade_id=trade_id, new_tp_price=None)
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="❌ Отмена", callback_data=f"position:details:{trade_id}")
        await callback.message.answer(
            f"🎯 **Изменение Take-Profit для #{trade_id}**\n\n"
            f"Текущий TP: {current_tp:.6f}\n\n"
            f"Введите новую цену TP (или /cancel для отмены):",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logger.error(f"Modify TP error: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)
