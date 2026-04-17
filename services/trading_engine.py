"""
Trading engine for arbitrage bot.
FIXED VERSION - Addresses critical bugs:
1. CCXT connection leak (FIXED)
2. Wrong TP/SL calculation for SHORT positions (FIXED)
3. Memory leak in active_monitors (FIXED)
4. Missing retry for position close (FIXED)
"""

import asyncio
import ccxt.async_support as ccxt
import sqlite3
import time
import uuid
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from decimal import Decimal, ROUND_DOWN

from database.models import Database, Trade, UserSettings

logger = logging.getLogger(__name__)


@dataclass
class TradeResult:
    success: bool
    error: Optional[str] = None
    trade_id: Optional[int] = None
    entry_spread: Optional[float] = None
    entry_price_long: Optional[float] = None
    entry_price_short: Optional[float] = None
    position_size: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    commission_paid: float = 0.0
    correlation_id: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


class TradingEngine:
    def __init__(self):
        self.active_monitors = {}
        self.monitors_lock = asyncio.Lock()
        self.active_exchanges = {}
        self.circuit_breakers = {}
        self.circuit_breaker_lock = asyncio.Lock()

    async def recover_positions(self):
        """Восстановление позиций при перезапуске бота (заглушка)"""
        logger.info("Recovering positions from database...")
        db = Database('/app/data/arbitrage_bot.db')
        await db.initialize()
        try:
            logger.info("Position recovery completed (stub implementation)")
        except Exception as e:
            logger.error(f"Error during position recovery: {e}")
        finally:
            await db.close()

    async def _get_exchange(self, exchange_id: str, api_key: str = None,
                           api_secret: str = None, password: str = None,
                           testnet: bool = True):
        """
        Получение экземпляра биржи с кэшированием и корректной обработкой ошибок.
        FIXED: Proper connection cleanup on error
        """
        cache_key = f"{exchange_id}_{api_key[:8] if api_key else 'public'}_{testnet}"

        if cache_key in self.active_exchanges:
            cached = self.active_exchanges[cache_key]

            # Check if cache is valid Exchange object
            if isinstance(cached, ccxt.Exchange):
                try:
                    # Lightweight health check
                    await cached.load_markets(reload=False)
                    return cached
                except Exception as e:
                    logger.warning(f"Cached exchange {exchange_id} failed health check: {e}")
                    try:
                        await cached.close()
                    except:
                        pass
                    del self.active_exchanges[cache_key]
            else:
                # Invalid cache - remove it
                logger.error(f"Invalid cache type for {exchange_id}: {type(cached)}")
                del self.active_exchanges[cache_key]

        # Create new connection
        exchange = None
        try:
            exchange_class = getattr(ccxt, exchange_id, None)
            if not exchange_class:
                raise ValueError(f"Exchange {exchange_id} not found in CCXT")

            config = {
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
                'timeout': 30000  # 30 seconds timeout
            }

            if api_key and api_secret:
                config['apiKey'] = api_key
                config['secret'] = api_secret
                if password:
                    config['password'] = password

            if testnet:
                config['sandbox'] = True

            exchange = exchange_class(config)

            # Verify connection before caching
            await exchange.load_markets()

            # Save to cache only after successful verification
            self.active_exchanges[cache_key] = exchange
            logger.info(f"Created new exchange instance for {exchange_id}")
            return exchange

        except Exception as e:
            # FIX: Guaranteed connection cleanup on any error
            if exchange:
                try:
                    await exchange.close()
                except Exception as close_error:
                    logger.warning(f"Error closing exchange during cleanup: {close_error}")
            logger.error(f"Failed to create exchange {exchange_id}: {e}")
            raise

    async def close_all_exchanges(self):
        """FIXED: Close all active exchange connections"""
        logger.info(f"Closing {len(self.active_exchanges)} exchange connections")
        for cache_key, exchange in list(self.active_exchanges.items()):
            try:
                if isinstance(exchange, ccxt.Exchange):
                    await exchange.close()
                    logger.debug(f"Closed exchange: {cache_key}")
            except Exception as e:
                logger.warning(f"Error closing exchange {cache_key}: {e}")
        self.active_exchanges.clear()

    def _calculate_position_levels(self, entry_price_long: float, entry_price_short: float,
                                   user: UserSettings) -> Dict[str, float]:
        """
        FIXED: Correct calculation of stop-loss and take-profit levels for both LONG and SHORT positions
        """
        sl_percent = user.risk_settings.get('stop_loss_percent', 2.0) / 100
        tp_percent = user.risk_settings.get('take_profit_percent', 20.0) / 100
        auto_close_spread = user.risk_settings.get('auto_close_spread', 0.05) / 100
        trailing_distance = user.risk_settings.get('trailing_stop_distance', 10.0) / 100

        # For LONG position:
        # - Take Profit should be ABOVE entry price (profit when price goes up)
        # - Stop Loss should be BELOW entry price (loss when price goes down)
        long_take_profit = entry_price_long * (1 + tp_percent)
        long_stop_loss = entry_price_long * (1 - sl_percent)
        long_breakeven = entry_price_long * (1 + auto_close_spread)
        long_trailing_stop = entry_price_long * (1 - trailing_distance)

        # For SHORT position:
        # - Take Profit should be BELOW entry price (profit when price goes down)
        # - Stop Loss should be ABOVE entry price (loss when price goes up)
        short_take_profit = entry_price_short * (1 - tp_percent)
        short_stop_loss = entry_price_short * (1 + sl_percent)
        short_breakeven = entry_price_short * (1 - auto_close_spread)
        short_trailing_stop = entry_price_short * (1 + trailing_distance)

        return {
            'long_stop_loss': long_stop_loss,
            'long_take_profit': long_take_profit,
            'long_breakeven': long_breakeven,
            'long_trailing_stop': long_trailing_stop,
            'short_stop_loss': short_stop_loss,
            'short_take_profit': short_take_profit,
            'short_breakeven': short_breakeven,
            'short_trailing_stop': short_trailing_stop
        }

    async def _cleanup_monitors(self):
        """FIXED: Cleanup completed monitors to prevent memory leak"""
        closed_monitors = [
            key for key, monitor in self.active_monitors.items()
            if hasattr(monitor, 'running') and not monitor.running
            or hasattr(monitor, 'trade') and monitor.trade.status == 'closed'
        ]
        for key in closed_monitors:
            del self.active_monitors[key]
            logger.info(f"Cleaned up monitor {key}")

    async def _start_monitor(self, trade: Trade, user: UserSettings, db: Database):
        """FIXED: Start position monitoring with cleanup"""
        monitor_key = f"{trade.user_id}:{trade.id}"
        async with self.monitors_lock:
            # FIX: Cleanup before creating new monitor
            await self._cleanup_monitors()

            if monitor_key in self.active_monitors:
                logger.info(f"Monitor {monitor_key} already exists, skipping")
                return

            monitor = PositionMonitor(
                trade=trade,
                user=user,
                engine=self,
                db=db
            )
            self.active_monitors[monitor_key] = monitor
            asyncio.create_task(monitor.run())
            logger.info(f"Started monitor for trade {trade.id}")

    async def validate_and_open(self, spread_info: dict, user: UserSettings) -> TradeResult:
        """Валидация и открытие сделки"""
        try:
            # Validate spread info
            if not spread_info.get('symbol'):
                return TradeResult(success=False, error="No symbol provided")

            symbol = spread_info['symbol']
            buy_exchange = spread_info.get('buy_exchange')
            sell_exchange = spread_info.get('sell_exchange')
            spread_percent = spread_info.get('spread', 0)

            # Get exchanges
            exchange_long = await self._get_exchange(
                buy_exchange,
                user.api_keys.get(buy_exchange, {}).get('api_key'),
                user.api_keys.get(buy_exchange, {}).get('api_secret'),
                testnet=user.alert_settings.get('test_mode', True)
            )

            exchange_short = await self._get_exchange(
                sell_exchange,
                user.api_keys.get(sell_exchange, {}).get('api_key'),
                user.api_keys.get(sell_exchange, {}).get('api_secret'),
                testnet=user.alert_settings.get('test_mode', True)
            )

            # Calculate position size
            trade_amount = user.risk_settings.get('trade_amount', 100)
            leverage = user.risk_settings.get('max_leverage', 3)

            # Set leverage with validation
            try:
                markets = await exchange_long.load_markets()
                market = markets.get(symbol)
                if market:
                    max_leverage = market.get('limits', {}).get('leverage', {}).get('max', leverage)
                    actual_leverage = min(leverage, max_leverage)
                    await exchange_long.set_leverage(actual_leverage, symbol)
                    await exchange_short.set_leverage(actual_leverage, symbol)
                    logger.info(f"Set leverage to {actual_leverage} for {symbol}")
            except Exception as e:
                logger.warning(f"Leverage setting error: {e}")
                # Don't ignore - use default leverage

            # Get entry prices
            ticker_long = await exchange_long.fetch_ticker(symbol)
            ticker_short = await exchange_short.fetch_ticker(symbol)

            entry_price_long = ticker_long['last']
            entry_price_short = ticker_short['last']

            # Calculate position levels
            levels = self._calculate_position_levels(entry_price_long, entry_price_short, user)

            # Create trade record
            trade = Trade(
                user_id=user.user_id,
                symbol=symbol,
                strategy='arbitrage',
                long_exchange=buy_exchange,
                short_exchange=sell_exchange,
                entry_spread=spread_percent,
                size_usd=trade_amount,
                entry_price_long=entry_price_long,
                entry_price_short=entry_price_short,
                current_price_long=entry_price_long,
                current_price_short=entry_price_short,
                stop_loss_price=levels['long_stop_loss'],
                take_profit_price=levels['long_take_profit'],
                trailing_stop_price=levels['long_trailing_stop'],
                metadata={'test_mode': user.alert_settings.get('test_mode', True)}
            )

            # Save to database
            db = Database('/app/data/arbitrage_bot.db')
            await db.initialize()
            try:
                trade.id = await db.create_trade(trade)
            finally:
                await db.close()

            # Start monitoring
            await self._start_monitor(trade, user, db)

            return TradeResult(
                success=True,
                trade_id=trade.id,
                entry_spread=spread_percent,
                entry_price_long=entry_price_long,
                entry_price_short=entry_price_short,
                position_size=trade_amount,
                stop_loss=levels['long_stop_loss'],
                take_profit=levels['long_take_profit']
            )

        except Exception as e:
            logger.error(f"Error opening trade: {e}")
            return TradeResult(success=False, error=str(e))

    async def close_trade_manually(self, trade_id: int, user: UserSettings) -> TradeResult:
        """Закрытие сделки вручную с retry логикой"""
        max_retries = 3
        retry_delay = 2.0

        for attempt in range(max_retries):
            try:
                db = Database('/app/data/arbitrage_bot.db')
                await db.initialize()

                try:
                    trade = await db.get_trade(trade_id)
                    if not trade:
                        return TradeResult(success=False, error="Trade not found")

                    if trade.status == 'closed':
                        return TradeResult(success=False, error="Trade already closed")

                    # Get exchanges
                    exchange_long = await self._get_exchange(
                        trade.long_exchange,
                        user.api_keys.get(trade.long_exchange, {}).get('api_key'),
                        user.api_keys.get(trade.long_exchange, {}).get('api_secret'),
                        testnet=user.alert_settings.get('test_mode', True)
                    )

                    exchange_short = await self._get_exchange(
                        trade.short_exchange,
                        user.api_keys.get(trade.short_exchange, {}).get('api_key'),
                        user.api_keys.get(trade.short_exchange, {}).get('api_secret'),
                        testnet=user.alert_settings.get('test_mode', True)
                    )

                    # Close positions
                    await exchange_long.create_market_sell_order(
                        trade.symbol, trade.position_size_long
                    )
                    await exchange_short.create_market_buy_order(
                        trade.symbol, trade.position_size_short
                    )

                    # Update trade status
                    trade.status = 'closed'
                    trade.closed_at = datetime.now().isoformat()
                    await db.update_trade(trade)

                    return TradeResult(success=True, trade_id=trade_id)

                finally:
                    await db.close()

            except ccxt.NetworkError as e:
                logger.warning(f"Network error closing trade (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (2 ** attempt))
                else:
                    return TradeResult(success=False, error=f"Failed after {max_retries} attempts: {e}")
            except Exception as e:
                logger.error(f"Error closing trade: {e}")
                return TradeResult(success=False, error=str(e))


class PositionMonitor:
    def __init__(self, trade: Trade, user: UserSettings, engine: TradingEngine, db: Database):
        self.trade = trade
        self.user = user
        self.engine = engine
        self.db = db
        self.running = True
        self.closing_in_progress = False

    async def run(self):
        """FIXED: Handle DB closure gracefully"""
        logger.info(f"Starting position monitor for trade {self.trade.id}")

        while self.running:
            try:
                await self._update_prices()
                await self._check_exit_conditions()
            except sqlite3.ProgrammingError:
                logger.warning("DB closed during price update, stopping monitor")
                break
            except Exception as e:
                logger.error(f"Error in monitor: {e}")

            await asyncio.sleep(1)

        logger.info(f"Position monitor stopped for trade {self.trade.id}")

    async def _update_prices(self):
        """Обновление цен позиции"""
        try:
            exchange_long = await self.engine._get_exchange(self.trade.long_exchange)
            exchange_short = await self.engine._get_exchange(self.trade.short_exchange)

            ticker_long = await exchange_long.fetch_ticker(self.trade.symbol)
            ticker_short = await exchange_short.fetch_ticker(self.trade.symbol)

            self.trade.current_price_long = ticker_long['last']
            self.trade.current_price_short = ticker_short['last']

            # Calculate PnL
            self._calculate_pnl()

        except Exception as e:
            logger.error(f"Error updating prices: {e}")

    def _calculate_pnl(self):
        """FIXED: Correct PnL calculation for arbitrage"""
        # Entry spread (in percent)
        entry_spread = (
            (self.trade.entry_price_short - self.trade.entry_price_long)
            / self.trade.entry_price_long * 100
        )

        # Current spread (in percent)
        current_spread = (
            (self.trade.current_price_short - self.trade.current_price_long)
            / self.trade.entry_price_long * 100
        )

        # PnL from spread change
        spread_change_pnl = entry_spread - current_spread

        position_size_usd = self.trade.size_usd
        leverage = self.user.risk_settings.get('max_leverage', 3)

        # Absolute PnL in USD
        pnl_usd = spread_change_pnl / 100 * position_size_usd * leverage

        # Percentage PnL relative to margin
        margin = position_size_usd / leverage
        pnl_percent = (pnl_usd / margin) * 100 if margin > 0 else 0

        self.trade.pnl_usd = pnl_usd
        self.trade.pnl_percent = pnl_percent

    async def _check_exit_conditions(self):
        """Проверка условий выхода"""
        if self.closing_in_progress:
            return

        # Check stop loss
        if self.trade.pnl_percent <= -self.user.risk_settings.get('stop_loss_percent', 2):
            await self._close_position("stop_loss")
            return

        # Check take profit
        if self.trade.pnl_percent >= self.user.risk_settings.get('take_profit_percent', 20):
            await self._close_position("take_profit")
            return

        # Check max position hours
        opened_at = datetime.fromisoformat(self.trade.opened_at.replace('Z', '+00:00'))
        hours_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600

        if hours_open >= self.user.risk_settings.get('max_position_hours', 24):
            await self._close_position("max_time")
            return

    async def _close_position(self, reason: str):
        """Закрытие позиции"""
        if self.closing_in_progress:
            return

        self.closing_in_progress = True
        logger.info(f"Closing position {self.trade.id}, reason: {reason}")

        result = await self.engine.close_trade_manually(self.trade.id, self.user)

        if result.success:
            self.running = False
            self.trade.status = 'closed'
            logger.info(f"Position {self.trade.id} closed successfully")
        else:
            logger.error(f"Failed to close position {self.trade.id}: {result.error}")
            self.closing_in_progress = False


# Global trading engine instance
trading_engine = TradingEngine()
