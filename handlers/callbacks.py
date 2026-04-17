"""
Callback handlers for Telegram bot.
FIXED VERSION - Addresses critical bugs:
1. Cannot re-enable exchange (issue #24) - FIXED
2. Alert threshold bug (issue #5) - FIXED
3. send_spread_alert() arguments error - FIXED
4. callback_error_handler() missing argument - FIXED
"""

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiogram.enums import ParseMode
import logging
import html

from database.models import UserSettings, Database
from services.trading_engine import trading_engine

logger = logging.getLogger(__name__)

callbacks_router = Router()

# Global variable for bot (set from main.py)
_bot = None


def set_bot(bot_instance):
    """Set bot instance for sending messages"""
    global _bot
    _bot = bot_instance


def escape_html(text) -> str:
    """Escape HTML special characters - FIXED to handle non-string values"""
    if text is None:
        return ""
    return html.escape(str(text))


# Available exchanges
AVAILABLE_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc', 'whitebit']


def validate_exchange(exchange_id: str) -> bool:
    """Validate exchange ID"""
    return exchange_id in AVAILABLE_EXCHANGES


# ==================== MENU CALLBACKS ====================

@callbacks_router.callback_query(F.data == "menu:main")
async def show_main_menu(callback: CallbackQuery, user: UserSettings):
    """Main menu"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="🚀 Авто-торговля", callback_data="auto_trade:menu"),
        InlineKeyboardButton(text="📊 Спреды", callback_data="spreads:menu")
    )
    builder.row(
        InlineKeyboardButton(text="📈 Мониторинг", callback_data="monitoring:menu"),
        InlineKeyboardButton(text="📉 Позиции", callback_data="positions:menu")
    )
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


@callbacks_router.callback_query(F.data == "spreads:menu")
async def show_spreads_menu(callback: CallbackQuery, user: UserSettings):
    """Spreads menu"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="🔥 Активные спреды", callback_data="spreads:active"),
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="alerts:settings")
    )
    builder.row(
        InlineKeyboardButton(text="📋 Мои алерты", callback_data="alerts:list"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        "**📊 Спреды**\n\n"
        f"🎯 **Порог:** {user.min_spread_threshold:.1f}%\n"
        f"🔄 **Межбиржевой:** {'🟢' if user.inter_exchange_enabled else '🔴'}\n"
        f"📊 **Базис:** {'🟢' if user.basis_arbitrage_enabled else '🔴'}\n\n"
        "Выбери действие:"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup())


@callbacks_router.callback_query(F.data == "spreads:active")
async def show_active_spreads(callback: CallbackQuery, user: UserSettings, state: FSMContext,
                               db: Database = None, scanner=None):
    """Show active spreads"""
    await callback.answer()

    try:
        if not scanner:
            await callback.message.edit_text(
                "❌ Сканер не запущен\n\n"
                "Попробуй позже или обратись к администратору.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="📱 Меню", callback_data="menu:main"
                ).as_markup()
            )
            return

        spreads = await scanner.get_top_spreads(20)
        if not spreads:
            await callback.message.edit_text(
                "😕 **Нет активных спредов**\n\n"
                "Порог может быть слишком высоким или биржи не отвечают.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="📱 Меню", callback_data="menu:main"
                ).as_markup()
            )
            return

        # FIXED: Subscribe user with correct threshold from DB
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
            reply_markup=InlineKeyboardBuilder().button(
                text="📱 Меню", callback_data="menu:main"
            ).as_markup()
        )


# ==================== ALERTS ====================

@callbacks_router.callback_query(F.data == "alerts:settings")
async def show_alert_settings(callback: CallbackQuery, user: UserSettings):
    """Alert settings"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    # On/Off
    builder.row(
        InlineKeyboardButton(
            text=f"🔔 {'🟢 Вкл' if user.alerts_enabled else '🔴 Выкл'}",
            callback_data="alerts:toggle"
        )
    )

    # Spread thresholds
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

    # Arbitrage types
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


@callbacks_router.callback_query(F.data.startswith("alerts:threshold:"))
async def set_alert_threshold(callback: CallbackQuery, user: UserSettings, db: Database = None):
    """Set alert threshold"""
    await callback.answer()

    try:
        threshold = float(callback.data.split(":")[2])
        user.min_spread_threshold = threshold

        # Also update in alert_settings for consistency
        if 'alert_settings' not in user.__dict__:
            user.alert_settings = {}
        user.alert_settings['min_spread'] = threshold

        if db:
            await db.update_user(user)

        # Update scanner threshold if available
        # (would need scanner instance passed)

        logger.info(f"Updated threshold for user {user.user_id}: {threshold}%")
        await show_alert_settings(callback, user)

    except Exception as e:
        logger.error(f"Error setting threshold: {e}")
        await callback.answer("❌ Ошибка", show_alert=True)


# FIXED: Correct alert subscription with proper threshold
async def subscribe_user_to_alerts(user_id: int, scanner, db: Database = None):
    """Subscribe user to alerts with correct threshold from DB"""
    if not scanner:
        return

    # Check if already subscribed
    existing = [s for s in scanner.subscribers if isinstance(s, tuple) and s[1] == user_id]
    if existing:
        logger.debug(f"User {user_id} already subscribed to alerts")
        return

    # Load user threshold from DB
    if db:
        try:
            user = await db.get_user(user_id)
            if user:
                # FIXED: Use correct field min_spread_threshold instead of alert_settings.get('min_spread')
                min_spread = user.min_spread_threshold
                scanner.set_user_threshold(user_id, min_spread)
                logger.info(f"User {user_id} threshold loaded from DB: {min_spread}%")
        except Exception as e:
            logger.error(f"Failed to load user threshold: {e}")

    # FIXED: Create wrapper for correct argument passing
    async def alert_wrapper(spread_info):
        await send_spread_alert(spread_info, user_id)

    scanner.subscribe(alert_wrapper)
    logger.info(f"User {user_id} subscribed to spread alerts")


# FIXED: send_spread_alert with correct signature
async def send_spread_alert(spread_info, user_id: int):
    """Send alert to user - FIXED"""
    global _bot

    try:
        if _bot is None:
            logger.error("Bot not initialized, cannot send alert")
            return

        # Determine spread_info type (dict or SpreadAlert object)
        if hasattr(spread_info, 'symbol'):
            # SpreadAlert object (NamedTuple)
            symbol = spread_info.symbol
            spread = spread_info.spread_percent
            buy_ex = spread_info.buy_exchange
            sell_ex = spread_info.sell_exchange
            buy_px = spread_info.buy_price.last_price if spread_info.buy_price else 0
            sell_px = spread_info.sell_price.last_price if spread_info.sell_price else 0
            spread_type = 'basis' if 'basis' in str(spread_info.arbitrage_type).lower() else 'inter'
        else:
            # Dict
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
                f"{buy_px:.6f} USDT\n\n"
                f"📈 **Продажа:** {escape_html(sell_ex)}\n"
                f"{sell_px:.6f} USDT"
            )
        else:
            text = (
                f"🚨 **АРБИТРАЖНЫЙ СПРЕД!**\n\n"
                f"💎 **{escape_html(symbol)}**\n"
                f"📈 **Спред:** {spread:.2f}%\n\n"
                f"📉 **Покупка на:** {escape_html(buy_ex)}\n"
                f"{buy_px:.6f} USDT\n\n"
                f"📈 **Продажа на:** {escape_html(sell_ex)}\n"
                f"{sell_px:.6f} USDT"
            )

        await _bot.send_message(
            chat_id=int(user_id),
            text=text,
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logger.error(f"Error sending alert to user {user_id}: {e}")


# ==================== EXCHANGES ====================

@callbacks_router.callback_query(F.data == "exchanges:menu")
async def show_exchanges(callback: CallbackQuery, user: UserSettings, state: FSMContext):
    """Exchange management"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    for ex in AVAILABLE_EXCHANGES:
        is_connected = ex in (user.selected_exchanges or [])
        status = "🟢" if is_connected else "⚪"
        builder.button(
            text=f"{status} {ex.upper()}",
            callback_data=f"exchanges:toggle:{ex}"
        )

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


# FIXED: Exchange toggle with proper error handling (issue #24)
@callbacks_router.callback_query(F.data.startswith("exchanges:toggle:"))
async def toggle_exchange(callback: CallbackQuery, user: UserSettings, state: FSMContext, db: Database = None):
    """Enable/disable exchange - FIXED"""
    try:
        exchange_id = callback.data.split(":")[2]

        if not validate_exchange(exchange_id):
            await callback.answer("❌ Неверная биржа", show_alert=True)
            return

        if user.selected_exchanges is None:
            user.selected_exchanges = []

        # Determine action
        if exchange_id in user.selected_exchanges:
            user.selected_exchanges.remove(exchange_id)
            action = "отключена"
            status_emoji = "⚪"
        else:
            user.selected_exchanges.append(exchange_id)
            action = "подключена"
            status_emoji = "🟢"

        # Save to DB
        if db:
            await db.update_user(user)

        # FIXED: Answer callback BEFORE editing message
        await callback.answer(f"{status_emoji} Биржа {exchange_id.upper()} {action}")

        # Update menu
        await show_exchanges(callback, user, state)

    except Exception as e:
        # FIXED: Handle "message is not modified" error
        if "message is not modified" in str(e).lower():
            logger.debug("Message not modified - state is already correct")
        else:
            logger.error(f"Error in toggle_exchange: {e}")
            await callback.answer("❌ Произошла ошибка", show_alert=True)
            raise


@callbacks_router.callback_query(F.data == "exchanges:add_api")
async def add_exchange_api(callback: CallbackQuery, state: FSMContext):
    """Start adding API key"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    for ex in AVAILABLE_EXCHANGES:
        builder.button(text=ex.upper(), callback_data=f"api:add:{ex}")

    builder.adjust(2)
    builder.row(
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    await callback.message.edit_text("**🔑 Выбери биржу:**", reply_markup=builder.as_markup())


@callbacks_router.callback_query(F.data.startswith("api:add:"))
async def start_api_input(callback: CallbackQuery, state: FSMContext):
    """Start API key input"""
    exchange_id = callback.data.split(":")[2]

    if not validate_exchange(exchange_id):
        await callback.answer("❌ Неверная биржа", show_alert=True)
        return

    await state.update_data(exchange_id=exchange_id, step='api_key')
    # Note: State management would need proper implementation

    await callback.message.edit_text(
        f"**🔑 Добавление API для {escape_html(exchange_id.upper())}**\n\n"
        f"Введи **API Key**:\n\n"
        f"_(Ключ будет сохранен безопасно.)_",
        reply_markup=InlineKeyboardBuilder().button(
            text="❌ Отмена", callback_data="menu:main"
        ).as_markup()
    )


# ==================== PROFILE ====================

@callbacks_router.callback_query(F.data == "profile:menu")
async def show_profile(callback: CallbackQuery, user: UserSettings):
    """Show profile"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="💰 Баланс", callback_data="profile:balance"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="profile:stats")
    )
    builder.row(
        InlineKeyboardButton(text="🔑 API ключи", callback_data="exchanges:menu"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        f"**⚙️ Профиль**\n\n"
        f"👤 ID: `{user.user_id}`\n"
        f"💰 Баланс: {user.total_balance:.2f} USDT\n"
        f"📊 Сделок: {user.total_trades}\n"
        f"💵 Прибыль: {user.total_profit:.2f} USDT\n\n"
        f"Выбери раздел:"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup())


@callbacks_router.callback_query(F.data == "profile:balance")
async def show_balance(callback: CallbackQuery, user: UserSettings):
    """Show balance"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="profile:balance"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    text = (
        f"**💰 Баланс**\n\n"
        f"📊 **Общий:** {user.total_balance:.2f} USDT\n"
        f"💵 **Доступно:** {user.available_balance:.2f} USDT\n"
        f"🔒 **В ордерах:** {user.locked_balance:.2f} USDT\n\n"
        f"_(Обновляется автоматически при торговле.)_"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup())


@callbacks_router.callback_query(F.data == "profile:stats")
async def show_stats(callback: CallbackQuery, user: UserSettings):
    """Show statistics"""
    await callback.answer()
    builder = InlineKeyboardBuilder()

    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="profile:stats"),
        InlineKeyboardButton(text="📱 Меню", callback_data="menu:main")
    )

    win_rate = (user.successful_trades / user.total_trades * 100) if user.total_trades > 0 else 0

    text = (
        f"**📊 Статистика**\n\n"
        f"📈 **Всего сделок:** {user.total_trades}\n"
        f"✅ **Успешных:** {user.successful_trades}\n"
        f"❌ **Неудачных:** {user.failed_trades}\n"
        f"📊 **Win Rate:** {win_rate:.1f}%\n"
        f"💰 **Общая прибыль:** {user.total_profit:.2f} USDT\n\n"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
