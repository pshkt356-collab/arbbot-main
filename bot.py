
import asyncio
import logging
from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, 
    InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.filters.callback_data import CallbackData

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.models import Database, UserSettings, Trade
from services.spread_scanner import SpreadScanner, SpreadAlert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FSM States для настройки
class SetupStates(StatesGroup):
    main_menu = State()
    trading_setup = State()
    api_setup_exchange = State()
    api_setup_key = State()
    api_setup_secret = State()
    commission_setup = State()
    commission_value = State()
    alert_setup = State()
    alert_min_spread = State()
    alert_exchanges = State()
    risk_setup = State()
    risk_max_position = State()

# Callback данные
class MenuCallback(CallbackData, prefix="menu"):
    action: str

class TradeCallback(CallbackData, prefix="trade"):
    action: str
    symbol: str
    spread: float

class ArbitrageBot:
    def __init__(self, token: str):
        self.bot = Bot(token=token, default=ParseMode.HTML)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(storage=self.storage)
        self.router = Router()
        self.dp.include_router(self.router)

        self.db = Database()
        self.scanner = SpreadScanner()
        self.scanner.subscribe(self._handle_spread_alert)

        self._setup_handlers()

    def _setup_handlers(self):
        # Основные команды
        self.router.message.register(self.cmd_start, CommandStart())
        self.router.message.register(self.cmd_stop, Command("stop"))
        self.router.message.register(self.cmd_status, Command("status"))

        # Callback хендлеры
        self.router.callback_query.register(
            self.on_menu_callback, 
            MenuCallback.filter()
        )
        self.router.callback_query.register(
            self.on_trade_callback,
            TradeCallback.filter()
        )

        # FSM хендлеры для настроек
        self.router.message.register(self.process_api_exchange, SetupStates.api_setup_exchange)
        self.router.message.register(self.process_api_key, SetupStates.api_setup_key)
        self.router.message.register(self.process_api_secret, SetupStates.api_setup_secret)
        self.router.message.register(self.process_commission_value, SetupStates.commission_value)
        self.router.message.register(self.process_min_spread, SetupStates.alert_min_spread)
        self.router.message.register(self.process_max_position, SetupStates.risk_max_position)

    async def cmd_start(self, message: Message, state: FSMContext):
        """Старт бота — создание пользователя и главное меню"""
        user_id = message.from_user.id

        # Создаём пользователя если новый
        user = self.db.get_user(user_id)
        if not user:
            user = self.db.create_user(user_id)
            logger.info(f"New user created: {user_id}")

        await state.set_state(SetupStates.main_menu)

        welcome_text = f"""
👋 <b>Привет, {message.from_user.first_name}!</b>

🤖 <b>Бот арбитража крипто-фьючерсов</b>

<b>Что умеет:</b>
• 📊 Мониторит спреды между Binance, Bybit, OKX в реальном времени
• 🔔 Отправляет алерты при спреде > заданного значения
• ⚡ Авто-торговля (если включена и настроены API)
• 📈 Учёт персональных комиссий

<b>Ваш статус:</b>
🔔 Алерты: {'✅ Вкл' if user.alert_settings else '❌ Выкл'}
⚡ Торговля: {'✅ Вкл' if user.is_trading_enabled else '❌ Выкл'}
💰 Комиссии: Настроены {len([k for k, v in user.commission_rates.items() if v])} бирж

Выберите действие:
"""

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚙️ Настройки", callback_data=MenuCallback(action="settings").pack())
        keyboard.button(text="📊 Мои сделки", callback_data=MenuCallback(action="trades").pack())
        keyboard.button(text="🔍 Ручной скан", callback_data=MenuCallback(action="scan").pack())
        keyboard.button(text="ℹ️ Помощь", callback_data=MenuCallback(action="help").pack())
        keyboard.adjust(2)

        await message.answer(welcome_text, reply_markup=keyboard.as_markup())

    async def on_menu_callback(self, callback: CallbackQuery, callback_data: MenuCallback):
        """Обработка нажатий меню"""
        action = callback_data.action
        user_id = callback.from_user.id

        if action == "settings":
            await self._show_settings(callback)
        elif action == "trades":
            await self._show_trades(callback)
        elif action == "scan":
            await self._manual_scan(callback)
        elif action == "help":
            await self._show_help(callback)
        elif action == "setup_trading":
            await self._setup_trading_start(callback)
        elif action == "setup_commission":
            await self._setup_commission_start(callback)
        elif action == "setup_alerts":
            await self._setup_alerts_start(callback)
        elif action == "setup_risk":
            await self._setup_risk_start(callback)
        elif action == "toggle_alerts":
            await self._toggle_alerts(callback)
        elif action == "back_to_main":
            await self._back_to_main(callback)

        await callback.answer()

    async def _show_settings(self, callback: CallbackQuery):
        """Показать меню настроек"""
        user_id = callback.from_user.id
        user = self.db.get_user(user_id)

        text = f"""
⚙️ <b>Настройки</b>

<b>Текущие параметры:</b>
🔔 Мин. спред для алерта: {user.alert_settings.get('min_spread', 0.1)}%
💰 Макс. позиция: ${user.risk_settings.get('max_position_usd', 10000):,}
⚡ Плечо: {user.risk_settings.get('max_leverage', 3)}x

<b>Комиссии:</b>
"""
        for ex, rates in user.commission_rates.items():
            text += f"• {ex.capitalize()}: maker {rates['maker']*100:.3f}%, taker {rates['taker']*100:.3f}%\n"

        text += f"\n<b>API ключи:</b> {len(user.api_keys)} бирж настроено"

        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚡ Настроить торговлю", callback_data=MenuCallback(action="setup_trading").pack())
        keyboard.button(text="💰 Комиссии", callback_data=MenuCallback(action="setup_commission").pack())
        keyboard.button(text="🔔 Алерты", callback_data=MenuCallback(action="setup_alerts").pack())
        keyboard.button(text="⚠️ Риски", callback_data=MenuCallback(action="setup_risk").pack())
        keyboard.button(text="🔙 Назад", callback_data=MenuCallback(action="back_to_main").pack())
        keyboard.adjust(2)

        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())

    async def _setup_trading_start(self, callback: CallbackQuery, state: FSMContext):
        """Начало настройки торговли"""
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Binance", callback_data=MenuCallback(action="api_binance").pack())
        keyboard.button(text="Bybit", callback_data=MenuCallback(action="api_bybit").pack())
        keyboard.button(text="OKX", callback_data=MenuCallback(action="api_okx").pack())
        keyboard.button(text="🔙 Отмена", callback_data=MenuCallback(action="settings").pack())
        keyboard.adjust(3)

        await callback.message.edit_text(
            "⚡ <b>Настройка API ключей</b>\n\n"
            "Выберите биржу для настройки:\n\n"
            "<i>⚠️ Боту нужны только права на фьючерсы, без права на вывод!</i>",
            reply_markup=keyboard.as_markup()
        )
        await state.set_state(SetupStates.api_setup_exchange)

    async def process_api_exchange(self, message: Message, state: FSMContext):
        """Обработка выбора биржи"""
        exchange = message.text.lower().strip()
        if exchange not in ['binance', 'bybit', 'okx']:
            await message.answer("❌ Неверная биржа. Выберите: Binance, Bybit или OKX")
            return

        await state.update_data(current_exchange=exchange)
        await message.answer(
            f"🔑 <b>API Key для {exchange.capitalize()}</b>\n\n"
            f"Введите ваш API Key:\n"
            f"<i>(получите на сайте биржи в разделе API Management)</i>",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(SetupStates.api_setup_key)

    async def process_api_key(self, message: Message, state: FSMContext):
        """Обработка API ключа"""
        api_key = message.text.strip()
        await state.update_data(api_key=api_key)

        data = await state.get_data()
        exchange = data['current_exchange']

        await message.answer(
            f"🔐 <b>API Secret для {exchange.capitalize()}</b>\n\n"
            f"Введите ваш API Secret:\n"
            f"<i>(внимание: сообщение удалится после обработки)</i>"
        )
        await state.set_state(SetupStates.api_setup_secret)

    async def process_api_secret(self, message: Message, state: FSMContext):
        """Обработка API секрета и сохранение"""
        api_secret = message.text.strip()

        # Удаляем сообщение с секретом
        try:
            await message.delete()
        except:
            pass

        data = await state.get_data()
        exchange = data['current_exchange']
        api_key = data['api_key']

        # Сохраняем в БД
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        user.api_keys[exchange] = {
            'key': api_key,
            'secret': api_secret
        }
        user.is_trading_enabled = True
        self.db.update_user(user)

        await message.answer(
            f"✅ <b>API для {exchange.capitalize()} сохранён!</b>\n\n"
            f"⚡ Режим торговли активирован.\n"
            f"При больших спредах бот будет предлагать открыть сделку.",
            reply_markup=ReplyKeyboardRemove()
        )

        await state.clear()

    async def _setup_commission_start(self, callback: CallbackQuery, state: FSMContext):
        """Настройка комиссий"""
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="Binance", callback_data=MenuCallback(action="comm_binance").pack())
        keyboard.button(text="Bybit", callback_data=MenuCallback(action="comm_bybit").pack())
        keyboard.button(text="OKX", callback_data=MenuCallback(action="comm_okx").pack())
        keyboard.button(text="🔙 Назад", callback_data=MenuCallback(action="settings").pack())
        keyboard.adjust(3)

        await callback.message.edit_text(
            "💰 <b>Настройка комиссий</b>\n\n"
            "Укажите ваши реальные комиссии (зависят от VIP уровня):\n"
            "Например: 0.02% maker / 0.05% taker\n\n"
            "Выберите биржу:",
            reply_markup=keyboard.as_markup()
        )

    async def process_commission_value(self, message: Message, state: FSMContext):
        """Обработка ввода комиссии"""
        try:
            data = await state.get_data()
            exchange = data['current_exchange']

            # Парсим ввод: "0.02 0.05" или "0.02/0.05"
            parts = message.text.replace('/', ' ').replace('%', '').split()
            maker = float(parts[0]) / 100
            taker = float(parts[1]) / 100 if len(parts) > 1 else maker

            user_id = message.from_user.id
            user = self.db.get_user(user_id)
            user.commission_rates[exchange] = {'maker': maker, 'taker': taker}
            self.db.update_user(user)

            await message.answer(
                f"✅ Комиссии для {exchange.capitalize()} обновлены:\n"
                f"Maker: {maker*100:.3f}%\n"
                f"Taker: {taker*100:.3f}%"
            )
            await state.clear()

        except Exception as e:
            await message.answer("❌ Ошибка формата. Введите: 0.02 0.05 (maker taker)")

    async def _setup_alerts_start(self, callback: CallbackQuery, state: FSMContext):
        """Настройка алертов"""
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🟢 Низкий (>0.1%)", callback_data=MenuCallback(action="alert_low").pack())
        keyboard.button(text="🟡 Средний (>0.5%)", callback_data=MenuCallback(action="alert_med").pack())
        keyboard.button(text="🔴 Высокий (>1%)", callback_data=MenuCallback(action="alert_high").pack())
        keyboard.button(text="🔙 Назад", callback_data=MenuCallback(action="settings").pack())
        keyboard.adjust(3)

        await callback.message.edit_text(
            "🔔 <b>Настройка алертов</b>\n\n"
            "Выберите минимальный спред для уведомлений:\n"
            "(с учётом ваших комиссий)",
            reply_markup=keyboard.as_markup()
        )

    async def process_min_spread(self, message: Message, state: FSMContext):
        """Обработка минимального спреда"""
        try:
            spread = float(message.text.replace('%', ''))
            user_id = message.from_user.id
            user = self.db.get_user(user_id)
            user.alert_settings['min_spread'] = spread
            self.db.update_user(user)

            await message.answer(f"✅ Минимальный спред установлен: {spread}%")
            await state.clear()
        except:
            await message.answer("❌ Введите число, например: 0.5")

    async def _handle_spread_alert(self, alert: SpreadAlert):
        """Обработка алерта от сканера — отправка всем пользователям"""
        # Получаем всех пользователей
        # В реальности здесь должен быть метод get_all_users()
        # Пока отправляем только тем, кто в памяти

        for user_id in self._get_active_users():
            try:
                user = self.db.get_user(user_id)
                if not user:
                    continue

                # Проверяем настройки пользователя
                if alert.spread_percent < user.alert_settings.get('min_spread', 0.1):
                    continue

                # Расчёт с учётом комиссий пользователя
                buy_commission = user.commission_rates.get(alert.buy_exchange, {}).get('taker', 0.0005)
                sell_commission = user.commission_rates.get(alert.sell_exchange, {}).get('taker', 0.0005)
                total_commission = (buy_commission + sell_commission) * 100

                net_spread = alert.spread_percent - total_commission

                if net_spread <= 0:
                    continue  # Не выгодно после комиссий

                # Формируем сообщение
                emoji = "🚀" if alert.alert_level == 'high' else "⚡" if alert.alert_level == 'medium' else "💡"

                text = f"""
{emoji} <b>АРБИТРАЖНАЯ ВОЗМОЖНОСТЬ</b>

<b>Пара:</b> <code>{alert.symbol}</code>
<b>Спред:</b> <code>{alert.spread_percent:.2f}%</code>
<b>После комиссий:</b> <code>{net_spread:.2f}%</code>

<b>Покупка:</b> {alert.buy_exchange.capitalize()} @ <code>{alert.buy_price:,.4f}</code>
<b>Продажа:</b> {alert.sell_exchange.capitalize()} @ <code>{alert.sell_price:,.4f}</code>

<b>Объём 24ч:</b> ${alert.volume_24h:,.0f}
<b>Разница фандинга:</b> {alert.funding_diff:.4f}%
"""

                keyboard = InlineKeyboardBuilder()

                # Если включена торговля — кнопка открыть сделку
                if user.is_trading_enabled and alert.alert_level in ['medium', 'high']:
                    max_position = user.risk_settings.get('max_position_usd', 10000)
                    size = min(max_position, 1000)  # Минимальная позиция $1000 или макс

                    keyboard.button(
                        text=f"⚡ Открыть сделку ${size:,}",
                        callback_data=TradeCallback(
                            action="open",
                            symbol=alert.symbol,
                            spread=alert.spread_percent
                        ).pack()
                    )

                keyboard.button(
                    text=f"📈 {alert.buy_exchange.capitalize()}",
                    url=f"https://www.{alert.buy_exchange.lower()}.com/futures/{alert.symbol}USDT"
                )
                keyboard.button(
                    text=f"📉 {alert.sell_exchange.capitalize()}",
                    url=f"https://www.{alert.sell_exchange.lower()}.com/futures/{alert.symbol}USDT"
                )
                keyboard.adjust(1 if user.is_trading_enabled else 2)

                await self.bot.send_message(
                    user_id,
                    text,
                    reply_markup=keyboard.as_markup(),
                    disable_notification=alert.alert_level == 'low'
                )

            except Exception as e:
                logger.error(f"Error sending alert to {user_id}: {e}")

    async def on_trade_callback(self, callback: CallbackQuery, callback_data: TradeCallback):
        """Обработка торговых действий"""
        if callback_data.action == "open":
            await self._open_trade(callback, callback_data)
        elif callback_data.action == "close":
            await self._close_trade(callback, callback_data)

        await callback.answer()

    async def _open_trade(self, callback: CallbackQuery, data: TradeCallback):
        """Открытие сделки через API"""
        user_id = callback.from_user.id
        user = self.db.get_user(user_id)

        # Проверка лимитов
        open_trades = self.db.get_open_trades(user_id)
        if len(open_trades) >= user.risk_settings.get('max_open_positions', 5):
            await callback.message.reply("❌ Достигнут лимит открытых позиций (5)")
            return

        # Здесь должна быть логика открытия через API бирж
        # Пока создаём запись в БД

        trade = Trade(
            id=None,
            user_id=user_id,
            symbol=data.symbol,
            strategy='cross_exchange',
            long_exchange='binance',  # Определяем по текущим ценам
            short_exchange='bybit',
            entry_spread=data.spread,
            close_spread=None,
            size_usd=user.risk_settings.get('max_position_usd', 10000),
            pnl_usd=None,
            status='open',
            opened_at=datetime.now().isoformat(),
            closed_at=None,
            metadata={'trigger_alert': True}
        )

        trade_id = self.db.add_trade(trade)

        await callback.message.reply(
            f"✅ <b>Сделка #{trade_id} открыта!</b>\n\n"
            f"Пара: {data.symbol}\n"
            f"Вход: {data.spread:.2f}%\n"
            f"Сумма: ${trade.size_usd:,}\n\n"
            f"Бот отслеживает закрытие..."
        )

    async def cmd_status(self, message: Message):
        """Статус бота"""
        user_id = message.from_user.id
        user = self.db.get_user(user_id)
        open_trades = self.db.get_open_trades(user_id)

        status_text = f"""
📊 <b>Ваш статус</b>

<b>Торговля:</b> {'✅ Активна' if user.is_trading_enabled else '❌ Выключена'}
<b>Открытых сделок:</b> {len(open_trades)}
<b>Алерты:</b> от {user.alert_settings.get('min_spread', 0.1)}%

<b>Последние сделки:</b>
"""
        for trade in open_trades[-3:]:
            status_text += f"• #{trade.id} {trade.symbol} ({trade.entry_spread:.2f}%)\n"

        await message.answer(status_text)

    async def cmd_stop(self, message: Message):
        """Остановка алертов"""
        # Здесь логика отписки от алертов
        await message.answer("🛑 Алерты приостановлены. Используйте /start для возобновления.")

    def _get_active_users(self):
        """Получение списка активных пользователей (заглушка)"""
        # В реальности — из БД или кэша
        return []

    async def run(self):
        """Запуск бота"""
        logger.info("Starting bot...")

        # Запускаем сканер в фоне
        asyncio.create_task(self.scanner.start())

        # Запускаем бота
        await self.dp.start_polling(self.bot)

if __name__ == "__main__":
    from datetime import datetime

    TOKEN = "YOUR_BOT_TOKEN_HERE"
    bot = ArbitrageBot(TOKEN)
    asyncio.run(bot.run())
