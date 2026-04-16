"""
Callback handlers for Telegram bot
"""
from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram.enums import ParseMode
import logging
import html

# Используем существующие импорты из оригинальной структуры
from database.models import UserSettings, Database
from services.trading_engine import trading_engine

# ИСПРАВЛЕНО: Используем SetupStates вместо BotStates
from handlers.states import SetupStates

logger = logging.getLogger(__name__)

callbacks_router = Router()

# Глобальная переменная для бота (устанавливается из main.py)
_bot = None

def set_bot(bot_instance):
    """Установка бота для отправки сообщений"""
    global _bot
    _bot = bot_instance

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

        builder = InlineKeyboardBuilder()
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
        mark = "✅" if abs(user.min_spread_threshold - th) < 0.01 else ""
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

    existing = [s for s in scanner.subscribers if isinstance(s, tuple) and s[1] == user_id]
    if existing:
        return

    # Загружаем порог пользователя из БД при подписке
    if db:
        try:
            user = await db.get_user(user_id)
            if user and user.alert_settings:
                min_spread = user.alert_settings.get('min_spread', 0.2)
                scanner.set_user_threshold(user_id, min_spread)
                logger.info(f"User {user_id} threshold loaded from DB: {min_spread}%")
        except Exception as e:
            logger.error(f"Failed to load user threshold: {e}")

    scanner.subscribe(send_spread_alert, user_id)
    logger.info(f"User {user_id} subscribed to spread alerts")

# ИСПРАВЛЕНО: Убран неработающий from bot import bot, используем _bot
async def send_spread_alert(user_id: int, spread_info):
    """Отправка алерта пользователю"""
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

        # Явно конвертируем user_id в int
        await _bot.send_message(chat_id=int(user_id), text=text, parse_mode=ParseMode.HTML)

    except Exception as e:
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

@callbacks_router.callback_query(F.data.startswith("exchanges:toggle:"))
async def toggle_exchange(callback: CallbackQuery, user: UserSettings, db: Database = None):
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

    await show_exchanges(callback, user)

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

    if not validate_exchange(exchange_id):
        await callback.answer("❌ Неверная биржа", show_alert=True)
        return

    await state.update_data(exchange_id=exchange_id, step='api_key')
    # ИСПРАВЛЕНО: SetupStates вместо BotStates
    await state.set_state(SetupStates.waiting_for_api_key)

    await callback.message.edit_text(
        f"**🔑 Добавление API для {escape_html(exchange_id.upper())}**\n\n"
        f"Введи **API Key**:\n\n"
        f"_(Ключ будет сохранен безопасно.)_",
        reply_markup=InlineKeyboardBuilder().button(text="❌ Отмена", callback_data="menu:main").as_markup()
    )

@callbacks_router.callback_query(F.data == "profile:balance")
async def show_balance(callback: CallbackQuery, user: UserSettings):
    """Показать баланс"""
    await callback.answer()

    text = (
        f"**💰 Баланс**\n\n"
        f"📊 **Общий:** {user.total_balance:.2f} USDT\n"
        f"💵 **Доступно:** {user.available_balance:.2f} USDT\n"
        f"🔒 **В ордерах:** {user.locked_balance:.2f} USDT\n\n"
        f"_(Обновляется автоматически при торговле.)_"
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="profile:balance"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

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
async def show_open_positions(callback: CallbackQuery, user: UserSettings):
    """Показать открытые позиции"""
    await callback.answer()

    # Заглушка - здесь нужно загружать из БД
    text = (
        "**📊 Открытые позиции**\n\n"
        " _Позиций нет._\n\n"
        "Авто-торговля откроет позиции автоматически."
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="positions:open"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@callbacks_router.callback_query(F.data == "positions:history")
async def show_positions_history(callback: CallbackQuery, user: UserSettings):
    """История позиций"""
    await callback.answer()

    text = (
        "**📈 История сделок**\n\n"
        f"🎯 **Всего:** {user.total_trades}\n"
        f"✅ **Успешных:** {user.successful_trades}\n"
        f"❌ **Неудачных:** {user.failed_trades}\n"
        f"💰 **Прибыль:** {user.total_profit:.2f} USDT"
    )

    builder = InlineKeyboardBuilder()
    builder.button(text="📱 Меню", callback_data="menu:main")
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ==================== ERROR HANDLING ====================

@callbacks_router.errors()
async def callback_error_handler(event, exception):
    """Обработка ошибок колбэков"""
    logger.error(f"Callback error: {exception}")
    if hasattr(event, 'callback_query') and event.callback_query:
        try:
            await event.callback_query.answer("❌ Ошибка обработки", show_alert=True)
        except:
            pass
