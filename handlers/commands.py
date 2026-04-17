"""
Command handlers for Telegram bot.
FIXED VERSION - Addresses critical bugs:
1. 'dict' object can't be awaited (issue #25) - FIXED
"""

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.enums import ParseMode
import logging
import html

from database.models import UserSettings, Database
from services.trading_engine import trading_engine

logger = logging.getLogger(__name__)

commands_router = Router()

# Available exchanges
AVAILABLE_EXCHANGES = ['binance', 'bybit', 'okx', 'mexc', 'whitebit']


def escape_html(text) -> str:
    """Escape HTML special characters"""
    if text is None:
        return ""
    return html.escape(str(text))


def validate_exchange(exchange_id: str) -> bool:
    """Validate exchange ID"""
    return exchange_id.lower() in AVAILABLE_EXCHANGES


@commands_router.message(Command("start"))
async def cmd_start(message: Message, user: UserSettings, db: Database):
    """Start command handler"""
    # Create user if not exists
    if not user:
        user = UserSettings(user_id=message.from_user.id)
        await db.create_user(user)
        logger.info(f"Created new user: {message.from_user.id}")

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🚀 Начать", callback_data="menu:main")
    )

    await message.answer(
        f"👋 **Привет, {escape_html(message.from_user.first_name)}!**\n\n"
        f"🤖 **Arbitrage Bot** — профессиональный инструмент для арбитража криптовалют.\n\n"
        f"📊 **Возможности:**\n"
        f"• Мониторинг спредов в реальном времени\n"
        f"• Автоматическая торговля\n"
        f"• Управление рисками\n"
        f"• Алерты о выгодных возможностях\n\n"
        f"Нажми **Начать** чтобы продолжить!",
        reply_markup=builder.as_markup()
    )


@commands_router.message(Command("help"))
async def cmd_help(message: Message):
    """Help command handler"""
    await message.answer(
        "**📖 Помощь**\n\n"
        "**Команды:**\n"
        "/start — Начать работу\n"
        "/help — Показать помощь\n"
        "/menu — Главное меню\n"
        "/testapi <биржа> — Проверить API ключ\n"
        "/balance — Показать баланс\n"
        "/positions — Открытые позиции\n"
        "/stats — Статистика\n\n"
        "**Поддержка:** @support"
    )


@commands_router.message(Command("menu"))
async def cmd_menu(message: Message):
    """Menu command handler"""
    await message.answer("Используйте кнопки меню ниже:")


# FIXED: API test command with proper async handling (issue #25)
@commands_router.message(Command("testapi"))
async def cmd_testapi(message: Message, user: UserSettings, command: CommandObject):
    """Test API connection - FIXED"""
    if not command.args:
        await message.answer("❌ Укажите биржу: /testapi <binance|bybit|okx|...>")
        return

    exchange_id = command.args.lower().strip()

    if not validate_exchange(exchange_id):
        await message.answer(f"❌ Неверная биржа: {exchange_id}")
        return

    # Get user API keys
    api_keys = user.api_keys.get(exchange_id, {})
    api_key = api_keys.get('api_key')
    api_secret = api_keys.get('api_secret')

    if not api_key or not api_secret:
        await message.answer(f"❌ API ключи для {exchange_id.upper()} не настроены")
        return

    await message.answer(f"🔄 Тестирование подключения к {exchange_id.upper()}...")

    try:
        import ccxt.async_support as ccxt

        # Create exchange instance
        exchange_class = getattr(ccxt, exchange_id, None)
        if not exchange_class:
            await message.answer(f"❌ Биржа {exchange_id} не поддерживается")
            return

        exchange = exchange_class({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'sandbox': True,  # Test mode
            'timeout': 30000
        })

        # FIXED: Properly await async methods
        try:
            # Test 1: Load markets
            markets = await exchange.load_markets()
            market_count = len(markets)

            # Test 2: Fetch balance
            balance = await exchange.fetch_balance()
            total_balance = balance.get('total', {}).get('USDT', 0)

            # Test 3: Fetch ticker
            ticker = await exchange.fetch_ticker('BTC/USDT')
            last_price = ticker.get('last', 0)

            await message.answer(
                f"✅ **Подключение к {exchange_id.upper()} успешно!**\n\n"
                f"📊 **Пар:** {market_count}\n"
                f"💰 **Баланс USDT:** {total_balance:.2f}\n"
                f"📈 **BTC/USDT:** {last_price:.2f}\n\n"
                f"API ключи работают корректно!"
            )

        finally:
            # Always close exchange connection
            await exchange.close()

    except ccxt.AuthenticationError:
        logger.error(f"API authentication failed for {exchange_id}")
        await message.answer(
            f"❌ **Ошибка аутентификации**\n\n"
            f"Проверьте правильность API ключей для {exchange_id.upper()}.\n"
            f"Убедитесь что:\n"
            f"• Ключи скопированы без пробелов\n"
            f"• У ключей есть нужные разрешения\n"
            f"• Ключи не истекли"
        )
    except ccxt.NetworkError as e:
        logger.error(f"Network error testing {exchange_id}: {e}")
        await message.answer(
            f"❌ **Ошибка сети**\n\n"
            f"Не удалось подключиться к {exchange_id.upper()}.\n"
            f"Попробуйте позже."
        )
    except Exception as e:
        logger.error(f"API test failed for {exchange_id}: {e}")
        await message.answer(
            f"❌ **Ошибка подключения:** {escape_html(str(e))[:200]}"
        )


@commands_router.message(Command("balance"))
async def cmd_balance(message: Message, user: UserSettings):
    """Show balance"""
    await message.answer(
        f"**💰 Баланс**\n\n"
        f"📊 **Общий:** {user.total_balance:.2f} USDT\n"
        f"💵 **Доступно:** {user.available_balance:.2f} USDT\n"
        f"🔒 **В ордерах:** {user.locked_balance:.2f} USDT\n\n"
        f"_(Обновляется автоматически при торговле.)_"
    )


@commands_router.message(Command("positions"))
async def cmd_positions(message: Message, user: UserSettings, db: Database):
    """Show open positions"""
    try:
        open_trades = await db.get_open_trades(
            user.user_id,
            test_mode=user.alert_settings.get('test_mode', True)
        )

        if not open_trades:
            await message.answer(
                "**📉 Позиции**\n\n"
                "Нет открытых позиций.\n\n"
                "Включите авто-торговлю чтобы начать!"
            )
            return

        text = f"**📉 Открытые позиции ({len(open_trades)})**\n\n"

        for i, trade in enumerate(open_trades, 1):
            pnl_emoji = "🟢" if trade.pnl_usd and trade.pnl_usd > 0 else "🔴"
            text += (
                f"{i}. **{trade.symbol}**\n"
                f"   {trade.long_exchange} → {trade.short_exchange}\n"
                f"   Спред: {trade.entry_spread:.2f}%\n"
                f"   {pnl_emoji} PnL: {trade.pnl_usd:.2f} USDT ({trade.pnl_percent:.2f}%)\n\n"
            )

        await message.answer(text[:4000])

    except Exception as e:
        logger.error(f"Error showing positions: {e}")
        await message.answer(f"❌ Ошибка: {escape_html(str(e))[:200]}")


@commands_router.message(Command("stats"))
async def cmd_stats(message: Message, user: UserSettings):
    """Show statistics"""
    win_rate = (user.successful_trades / user.total_trades * 100) if user.total_trades > 0 else 0

    await message.answer(
        f"**📊 Статистика**\n\n"
        f"📈 **Всего сделок:** {user.total_trades}\n"
        f"✅ **Успешных:** {user.successful_trades}\n"
        f"❌ **Неудачных:** {user.failed_trades}\n"
        f"📊 **Win Rate:** {win_rate:.1f}%\n"
        f"💰 **Общая прибыль:** {user.total_profit:.2f} USDT\n\n"
        f"_(Статистика обновляется в реальном времени.)_"
    )


@commands_router.message(Command("settings"))
async def cmd_settings(message: Message, user: UserSettings):
    """Show settings"""
    await message.answer(
        f"**⚙️ Настройки**\n\n"
        f"🎯 **Порог спреда:** {user.min_spread_threshold:.1f}%\n"
        f"💰 **Объем сделки:** {user.risk_settings.get('trade_amount', 100)} USDT\n"
        f"⚡ **Плечо:** {user.risk_settings.get('max_leverage', 3)}x\n"
        f"📊 **Макс позиций:** {user.risk_settings.get('max_open_positions', 5)}\n"
        f"🔄 **Межбиржевой:** {'🟢' if user.inter_exchange_enabled else '🔴'}\n"
        f"📊 **Базис:** {'🟢' if user.basis_arbitrage_enabled else '🔴'}\n\n"
        f"Используйте меню для изменения настроек."
    )


# Import for inline keyboard
from aiogram.types import InlineKeyboardButton
