import asyncio
import ccxt.async_support as ccxt
import ccxt
import time
import uuid
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from decimal import Decimal, ROUND_DOWN
from config import settings
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

    # ===== МЕТОД ДОБАВЛЕН СЮДА (в класс TradingEngine) =====
    async def recover_positions(self):
        """Восстановление позиций при перезапуске бота (заглушка)"""
        logger.info("Recovering positions from database...")
        db = Database(settings.db_file)
        await db.initialize()
        try:
            # TODO: Получить все открытые сделки и восстановить мониторы
            # Для каждой открытой сделки запустить PositionMonitor
            logger.info("Position recovery completed (stub implementation)")
        except Exception as e:
            logger.error(f"Error during position recovery: {e}")
        finally:
            await db.close()
    # =====================================================

    async def _get_exchange(self, exchange_id: str, api_key: str = None, api_secret: str = None,
                          password: str = None, testnet: bool = True):
        cache_key = f"{exchange_id}_{api_key[:8] if api_key else 'public'}_{testnet}"

        if cache_key in self.active_exchanges:
            cached = self.active_exchanges[cache_key]
            # Проверяем что кэш - это не dict (битый кэш)
            if isinstance(cached, dict):
                logger.warning(f"Removing corrupted cache for {exchange_id}: dict instead of exchange")
                del self.active_exchanges[cache_key]
            else:
                return cached

        exchange_class = getattr(ccxt, exchange_id, None)
        if not exchange_class:
            raise ValueError(f"Exchange {exchange_id} not found")

        config = {
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'test': testnet,
                'adjustForTimeDifference': True,
            }
        }

        if api_key and api_secret:
            config.update({
                'apiKey': api_key,
                'secret': api_secret,
                'password': password,
            })

        if settings.use_proxy and settings.proxy_url:
            config['proxies'] = {
                'http': settings.proxy_url,
                'https': settings.proxy_url,
            }

        exchange = exchange_class(config)
        await exchange.load_markets()

        if settings.use_proxy and settings.proxy_url:
            logger.info(f"Using proxy for {exchange_id}")

        self.active_exchanges[cache_key] = exchange
        return exchange

    async def _check_circuit_breaker(self, exchange_id: str) -> bool:
        async with self.circuit_breaker_lock:
            now = time.time()
            if exchange_id in self.circuit_breakers:
                last_fail, count = self.circuit_breakers[exchange_id]
                if now - last_fail < 60 and count >= 3:
                    logger.warning(f"Circuit breaker active for {exchange_id}")
                    return False
                elif now - last_fail > 60:
                    del self.circuit_breakers[exchange_id]
            return True

    async def _record_failure(self, exchange_id: str):
        async with self.circuit_breaker_lock:
            now = time.time()
            if exchange_id not in self.circuit_breakers:
                self.circuit_breakers[exchange_id] = (now, 1)
            else:
                _, count = self.circuit_breakers[exchange_id]
                self.circuit_breakers[exchange_id] = (now, count + 1)

    async def validate_and_open(self, user: UserSettings, spread_key: str, scanner_prices: Dict,
                               auto: bool = False, test_mode: bool = None, available_exchanges: Dict = None) -> TradeResult:
        correlation_id = str(uuid.uuid4())[:8]

        if test_mode is None:
            test_mode = user.alert_settings.get('test_mode', True)

        parts = spread_key.split(':')
        if len(parts) != 3:
            return TradeResult(success=False, error="Invalid spread key format", trade_id=None, correlation_id=correlation_id)

        symbol, buy_exchange, sell_exchange = parts

        db = Database(settings.db_file)
        await db.initialize()

        try:
            if not test_mode:
                open_trades = await db.get_open_trades(user.user_id, test_mode=False)
                if len(open_trades) >= user.risk_settings.get('max_open_positions', 5):
                    return TradeResult(success=False, error="Max positions limit reached", correlation_id=correlation_id)

                if not user.is_trading_enabled and not test_mode:
                    return TradeResult(success=False, error="Trading not enabled", correlation_id=correlation_id)

            if symbol not in scanner_prices:
                return TradeResult(success=False, error="Symbol not in scanner prices", correlation_id=correlation_id)

            price_data = scanner_prices[symbol]

            if buy_exchange not in price_data or 'futures' not in price_data[buy_exchange]:
                return TradeResult(success=False, error=f"No futures data for {buy_exchange}", correlation_id=correlation_id)
            if sell_exchange not in price_data or 'futures' not in price_data[sell_exchange]:
                return TradeResult(success=False, error=f"No futures data for {sell_exchange}", correlation_id=correlation_id)

            buy_pd = price_data[buy_exchange]['futures']
            sell_pd = price_data[sell_exchange]['futures']

            now = time.time()
            if now - buy_pd.timestamp > 30 or now - sell_pd.timestamp > 30:
                return TradeResult(success=False, error="Price data is stale (>30s)", correlation_id=correlation_id)

            strategy = "futures_arbitrage"
            long_ex = buy_exchange
            short_ex = sell_exchange

            entry_spread = abs((sell_pd.effective_price - buy_pd.effective_price) / buy_pd.effective_price * 100) if buy_pd.effective_price > 0 else 0

            if test_mode:
                return await self._open_test_trade(user, symbol, long_ex, short_ex, buy_pd, sell_pd, entry_spread, db, strategy, correlation_id)
            else:
                if long_ex not in user.api_keys or not user.api_keys[long_ex].get('api_key'):
                    return TradeResult(success=False, error=f"No API keys for {long_ex}", correlation_id=correlation_id)
                if short_ex not in user.api_keys or not user.api_keys[short_ex].get('api_key'):
                    return TradeResult(success=False, error=f"No API keys for {short_ex}", correlation_id=correlation_id)

                return await self._open_real_trade(user, symbol, long_ex, short_ex, buy_pd, sell_pd, entry_spread, db, strategy, correlation_id)
        finally:
            await db.close()

    async def _open_test_trade(self, user: UserSettings, symbol: str, long_ex: str, short_ex: str,
                              buy_pd, sell_pd, entry_spread: float, db: Database, strategy: str, correlation_id: str) -> TradeResult:
        import random

        size_usd = min(user.risk_settings.get('max_position_usd', 10000), 1000)

        slippage_long = random.uniform(0.0001, 0.001)
        slippage_short = random.uniform(0.0001, 0.001)

        actual_price_long = buy_pd.effective_price * (1 + slippage_long)
        actual_price_short = sell_pd.effective_price * (1 - slippage_short)

        commission_rate = 0.00055
        commission_long = size_usd * commission_rate
        commission_short = size_usd * commission_rate
        total_commission = commission_long + commission_short

        position_size_long = size_usd / actual_price_long if actual_price_long > 0 else 0
        position_size_short = size_usd / actual_price_short if actual_price_short > 0 else 0

        atr = actual_price_long * 0.02
        stop_distance = atr * user.risk_settings.get('atr_multiplier', 2.0)
        stop_loss_price = actual_price_long - stop_distance
        min_stop_pct = user.risk_settings.get('min_stop_loss_percent', 2.0)
        stop_pct = (actual_price_long - stop_loss_price) / actual_price_long * 100
        if stop_pct < min_stop_pct:
            stop_loss_price = actual_price_long * (1 - min_stop_pct / 100)

        take_profit_pct = user.risk_settings.get('take_profit_percent', 20.0)
        take_profit_price = actual_price_short * (1 + take_profit_pct / 100)
        emergency_stop = user.risk_settings.get('emergency_stop_percent', 50.0)
        emergency_price = actual_price_long * (1 - emergency_stop / 100)

        trade = Trade(
            user_id=user.user_id,
            symbol=symbol,
            strategy=strategy,
            long_exchange=long_ex or "",
            short_exchange=short_ex or "",
            entry_spread=entry_spread,
            size_usd=size_usd,
            position_size_long=position_size_long,
            position_size_short=position_size_short,
            entry_price_long=actual_price_long,
            entry_price_short=actual_price_short,
            current_price_long=actual_price_long,
            current_price_short=actual_price_short,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            emergency_stop_price=emergency_price,
            trailing_enabled=user.risk_settings.get('trailing_stop_enabled', True),
            trailing_stop_price=stop_loss_price,
            status="open",
            metadata={
                'test_mode': True,
                'opened_by': 'manual',
                'correlation_id': correlation_id,
                'available_exchanges': {'buy': long_ex is not None, 'sell': short_ex is not None},
                'slippage': {'long': slippage_long, 'short': slippage_short},
                'commission': total_commission,
                'entry_time': datetime.now(timezone.utc).isoformat(),
                'emulation': True
            }
        )

        trade_id = await db.add_trade(trade)
        trade.id = trade_id

        if long_ex or short_ex:
            await self._start_monitor(trade, user, db)

        logger.info(f"[{correlation_id}] Test trade opened: {symbol} #{trade_id} (emulated)")

        return TradeResult(
            success=True,
            trade_id=trade_id,
            error=None,
            entry_spread=entry_spread,
            entry_price_long=actual_price_long,
            entry_price_short=actual_price_short,
            position_size=size_usd,
            stop_loss=stop_loss_price,
            take_profit=take_profit_price,
            commission_paid=total_commission,
            correlation_id=correlation_id,
            metadata={'test_mode': True, 'strategy': strategy, 'emulated': True, 'slippage': f"{(slippage_long+slippage_short)*100:.3f}%"}
        )

    async def _open_real_trade(self, user: UserSettings, symbol: str, long_ex: str, short_ex: str,
                              buy_pd, sell_pd, entry_spread: float, db: Database, strategy: str, correlation_id: str, auto: bool = False) -> TradeResult:
        try:
            if not await self._check_circuit_breaker(long_ex) or not await self._check_circuit_breaker(short_ex):
                return TradeResult(success=False, error="Circuit breaker active", correlation_id=correlation_id)

            size_usd = min(user.risk_settings.get('max_position_usd', 10000), 1000)

            exchange_long = None
            exchange_short = None

            try:
                exchange_long = await self._get_exchange(long_ex,
                    user.api_keys[long_ex]['api_key'],
                    user.api_keys[long_ex]['api_secret'],
                    user.api_keys[long_ex].get('password'),
                    user.api_keys[long_ex].get('testnet', True))
                exchange_short = await self._get_exchange(short_ex,
                    user.api_keys[short_ex]['api_key'],
                    user.api_keys[short_ex]['api_secret'],
                    user.api_keys[short_ex].get('password'),
                    user.api_keys[short_ex].get('testnet', True))
            except Exception as e:
                if exchange_long:
                    await exchange_long.close()
                if exchange_short:
                    await exchange_short.close()
                return TradeResult(success=False, error=f"Failed to connect exchanges: {str(e)}", correlation_id=correlation_id)

            try:
                balance_long = await exchange_long.fetch_balance()
                balance_short = await exchange_short.fetch_balance()

                usdt_long = self._get_usdt_balance(balance_long)
                usdt_short = self._get_usdt_balance(balance_short)

                required = size_usd * 1.2

                if usdt_long < required or usdt_short < required:
                    await exchange_long.close()
                    await exchange_short.close()
                    return TradeResult(success=False, error=f"Insufficient balance. Required: ${required:.2f} per exchange", correlation_id=correlation_id)
            except Exception as e:
                await exchange_long.close()
                await exchange_short.close()
                return TradeResult(success=False, error=f"Balance check failed: {str(e)}", correlation_id=correlation_id)

            position_size_long = size_usd / buy_pd.effective_price if buy_pd.effective_price > 0 else 0
            position_size_short = size_usd / sell_pd.effective_price if sell_pd.effective_price > 0 else 0

            try:
                market_long = exchange_long.market(f"{symbol.replace('/', '')}:USDT")
                market_short = exchange_short.market(f"{symbol.replace('/', '')}:USDT")

                min_amount_long = market_long['limits']['amount']['min'] if market_long['limits']['amount']['min'] else 0
                min_amount_short = market_short['limits']['amount']['min'] if market_short['limits']['amount']['min'] else 0

                if position_size_long < min_amount_long:
                    await exchange_long.close()
                    await exchange_short.close()
                    return TradeResult(success=False, error=f"Long amount {position_size_long} < min {min_amount_long}", correlation_id=correlation_id)
                if position_size_short < min_amount_short:
                    await exchange_long.close()
                    await exchange_short.close()
                    return TradeResult(success=False, error=f"Short amount {position_size_short} < min {min_amount_short}", correlation_id=correlation_id)
            except Exception as e:
                logger.warning(f"Could not check min lot size: {e}")

            try:
                amount_precision_long = market_long['precision']['amount']
                amount_precision_short = market_short['precision']['amount']

                position_size_long = float(Decimal(str(position_size_long)).quantize(Decimal(str(amount_precision_long)), rounding=ROUND_DOWN))
                position_size_short = float(Decimal(str(position_size_short)).quantize(Decimal(str(amount_precision_short)), rounding=ROUND_DOWN))
            except Exception as e:
                logger.warning(f"Precision rounding error: {e}")

            leverage = user.risk_settings.get('max_leverage', 3)
            try:
                await exchange_long.set_leverage(leverage, f"{symbol.replace('/', '')}:USDT")
                await exchange_short.set_leverage(leverage, f"{symbol.replace('/', '')}:USDT")
            except Exception as e:
                logger.warning(f"Leverage setting error: {e}")

            long_order = None
            short_order = None

            try:
                long_order = await exchange_long.create_market_buy_order(
                    f"{symbol.replace('/', '')}:USDT",
                    position_size_long
                )

                short_order = await exchange_short.create_market_sell_order(
                    f"{symbol.replace('/', '')}:USDT",
                    position_size_short
                )

                actual_price_long = long_order['average'] if long_order['average'] else long_order['price']
                actual_price_short = short_order['average'] if short_order['average'] else short_order['price']

                commission_long = long_order['fee']['cost'] if long_order.get('fee') else (size_usd * 0.00055)
                commission_short = short_order['fee']['cost'] if short_order.get('fee') else (size_usd * 0.00055)
                total_commission = commission_long + commission_short

            except Exception as e:
                if long_order and not short_order:
                    try:
                        await exchange_long.create_market_sell_order(f"{symbol.replace('/', '')}:USDT", position_size_long)
                    except:
                        pass
                await exchange_long.close()
                await exchange_short.close()
                await self._record_failure(long_ex if not long_order else short_ex)
                return TradeResult(success=False, error=f"Order execution failed: {str(e)}", correlation_id=correlation_id)

            atr = actual_price_long * 0.02
            stop_distance = atr * user.risk_settings.get('atr_multiplier', 2.0)
            stop_loss_price = actual_price_long - stop_distance
            min_stop_pct = user.risk_settings.get('min_stop_loss_percent', 2.0)
            stop_pct = (actual_price_long - stop_loss_price) / actual_price_long * 100
            if stop_pct < min_stop_pct:
                stop_loss_price = actual_price_long * (1 - min_stop_pct / 100)

            take_profit_price = actual_price_short * (1 + user.risk_settings.get('take_profit_percent', 20.0) / 100)
            emergency_price = actual_price_long * (1 - user.risk_settings.get('emergency_stop_percent', 50.0) / 100)

            trade = Trade(
                user_id=user.user_id,
                symbol=symbol,
                strategy=strategy,
                long_exchange=long_ex,
                short_exchange=short_ex,
                entry_spread=entry_spread,
                size_usd=size_usd,
                position_size_long=position_size_long,
                position_size_short=position_size_short,
                entry_price_long=actual_price_long,
                entry_price_short=actual_price_short,
                current_price_long=actual_price_long,
                current_price_short=actual_price_short,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                emergency_stop_price=emergency_price,
                trailing_enabled=user.risk_settings.get('trailing_stop_enabled', True),
                trailing_stop_price=stop_loss_price,
                status="open",
                metadata={
                    'test_mode': False,
                    'opened_by': 'auto' if auto else 'manual',
                    'correlation_id': correlation_id,
                    'long_order_id': long_order['id'],
                    'short_order_id': short_order['id'],
                    'commission': total_commission,
                    'entry_time': datetime.now(timezone.utc).isoformat(),
                    'leverage': leverage,
                    'margin_mode': user.risk_settings.get('margin_mode', 'isolated')
                }
            )

            trade_id = await db.add_trade(trade)
            trade.id = trade_id

            await exchange_long.close()
            await exchange_short.close()

            await self._start_monitor(trade, user, db)

            logger.info(f"[{correlation_id}] Real trade opened: {symbol} #{trade_id}")

            return TradeResult(
                success=True,
                trade_id=trade_id,
                error=None,
                entry_spread=entry_spread,
                entry_price_long=actual_price_long,
                entry_price_short=actual_price_short,
                position_size=size_usd,
                stop_loss=stop_loss_price,
                take_profit=take_profit_price,
                commission_paid=total_commission,
                correlation_id=correlation_id,
                metadata={'test_mode': False, 'strategy': strategy, 'leverage': leverage}
            )

        except Exception as e:
            logger.error(f"Unexpected error in _open_real_trade: {e}")
            return TradeResult(success=False, error=f"Unexpected error: {str(e)}", correlation_id=correlation_id)

    def _get_usdt_balance(self, balance: Dict) -> float:
        try:
            if 'USDT' in balance:
                return balance['USDT']['free'] if isinstance(balance['USDT'], dict) else 0
            if 'free' in balance and 'USDT' in balance['free']:
                return balance['free']['USDT']
            return 0
        except Exception as e:
            logger.error(f"Error parsing balance: {e}")
            return 0

    async def _start_monitor(self, trade: Trade, user: UserSettings, db: Database):
        monitor_key = f"{trade.user_id}:{trade.id}"

        async with self.monitors_lock:
            if monitor_key in self.active_monitors:
                logger.warning(f"Monitor already exists for {monitor_key}")
                return

            monitor = PositionMonitor(trade, user, db, self)
            self.active_monitors[monitor_key] = monitor
            asyncio.create_task(monitor.run())
            logger.info(f"Started monitor for trade {trade.id}")

    async def stop_all_monitors(self):
        async with self.monitors_lock:
            for key, monitor in self.active_monitors.items():
                await monitor.stop()
            self.active_monitors.clear()
            logger.info("All monitors stopped")

    async def _cleanup_cache(self):
        while True:
            try:
                await asyncio.sleep(300)
                async with self.circuit_breaker_lock:
                    now = time.time()
                    to_remove = []
                    for exchange_id, (last_fail, count) in self.circuit_breakers.items():
                        if now - last_fail > 3600:
                            to_remove.append(exchange_id)
                    for ex in to_remove:
                        del self.circuit_breakers[ex]
                        logger.info(f"Cleaned up circuit breaker for {ex}")

                logger.debug("Cache cleanup completed")
            except asyncio.CancelledError:
                logger.info("Cache cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")
                await asyncio.sleep(60)

    async def close_trade_manually(self, trade_id: int, user: UserSettings) -> TradeResult:
        correlation_id = str(uuid.uuid4())[:8]

        db = Database(settings.db_file)
        await db.initialize()

        try:
            trade_data = await db.get_trade_by_id(trade_id)
            if not trade_data or trade_data['user_id'] != user.user_id:
                return TradeResult(success=False, error="Trade not found", correlation_id=correlation_id)

            trade = Trade(**trade_data)

            if trade.status != "open":
                return TradeResult(success=False, error="Trade already closed", correlation_id=correlation_id)

            monitor_key = f"{user.user_id}:{trade_id}"
            async with self.monitors_lock:
                if monitor_key in self.active_monitors:
                    await self.active_monitors[monitor_key].stop()
                    del self.active_monitors[monitor_key]

            if trade.metadata.get('test_mode'):
                close_spread = 0
                pnl = trade.pnl_percent if trade.pnl_percent else 0
                pnl_usd = trade.pnl_usd if trade.pnl_usd else 0

                trade.close_spread = close_spread
                trade.pnl_usd = pnl_usd
                trade.status = "closed"
                trade.closed_at = datetime.now(timezone.utc).isoformat()
                await db.update_trade(trade)
                await db.close_trade(trade_id, close_spread, pnl_usd)

                return TradeResult(
                    success=True,
                    trade_id=trade_id,
                    error=None,
                    correlation_id=correlation_id,
                    metadata={'pnl': pnl, 'test_mode': True}
                )
            else:
                return await self._close_real_position(trade, user, db, correlation_id)

        finally:
            await db.close()

    async def _close_real_position(self, trade: Trade, user: UserSettings, db: Database, correlation_id: str) -> TradeResult:
        try:
            exchange_long = await self._get_exchange(trade.long_exchange,
                user.api_keys[trade.long_exchange]['api_key'],
                user.api_keys[trade.long_exchange]['api_secret'],
                user.api_keys[trade.long_exchange].get('password'),
                user.api_keys[trade.long_exchange].get('testnet', True))
            exchange_short = await self._get_exchange(trade.short_exchange,
                user.api_keys[trade.short_exchange]['api_key'],
                user.api_keys[trade.short_exchange]['api_secret'],
                user.api_keys[trade.short_exchange].get('password'),
                user.api_keys[trade.short_exchange].get('testnet', True))

            symbol = trade.symbol.replace('/', '')

            await exchange_long.create_market_sell_order(f"{symbol}:USDT", trade.position_size_long)
            await exchange_short.create_market_buy_order(f"{symbol}:USDT", trade.position_size_short)

            await exchange_long.close()
            await exchange_short.close()

            close_spread = 0
            pnl_usd = trade.pnl_usd if trade.pnl_usd else 0

            trade.close_spread = close_spread
            trade.status = "closed"
            trade.closed_at = datetime.now(timezone.utc).isoformat()
            await db.update_trade(trade)
            await db.close_trade(trade.id, close_spread, pnl_usd)

            return TradeResult(
                success=True,
                trade_id=trade.id,
                error=None,
                correlation_id=correlation_id,
                metadata={'pnl': trade.pnl_percent, 'test_mode': False}
            )

        except Exception as e:
            logger.error(f"Error closing real position: {e}")
            return TradeResult(success=False, error=f"Close failed: {str(e)}", correlation_id=correlation_id)

    async def test_api_connection(self, exchange_id: str, api_key: str, api_secret: str, testnet: bool = True) -> dict:
        """Тестирование подключения к API биржи"""
        try:
            exchange = await self._get_exchange(exchange_id, api_key, api_secret, None, testnet)
            
            # Проверяем баланс
            balance = await exchange.fetch_balance()
            usdt_balance = self._get_usdt_balance(balance)
            
            # Проверяем доступность торговли
            markets = await exchange.load_markets()
            
            await exchange.close()
            
            return {
                'success': True,
                'balance_usdt': usdt_balance,
                'markets_count': len(markets),
                'message': f'Подключение успешно. Баланс: {usdt_balance:.2f} USDT'
            }
        except Exception as e:
            logger.error(f"API connection test failed for {exchange_id}: {e}")
            return {
                'success': False,
                'balance_usdt': 0,
                'markets_count': 0,
                'message': f'Ошибка подключения: {str(e)}'
            }

    async def partial_close(self, trade_id: int, user: UserSettings, percentage: float) -> TradeResult:
        """Частичное закрытие позиции"""
        correlation_id = str(uuid.uuid4())[:8]
        
        try:
            # Находим сделку
            trade = None
            for monitor in self.active_monitors.values():
                if monitor.trade.id == trade_id:
                    trade = monitor.trade
                    break
            
            if not trade:
                return TradeResult(success=False, error="Trade not found", correlation_id=correlation_id)
            
            if percentage <= 0 or percentage > 100:
                return TradeResult(success=False, error="Percentage must be between 1 and 100", correlation_id=correlation_id)
            
            # TODO: Реализовать частичное закрытие через API бирж
            logger.info(f"[{correlation_id}] Partial close {percentage}% for trade #{trade_id}")
            
            return TradeResult(
                success=True,
                trade_id=trade_id,
                correlation_id=correlation_id,
                message=f"Partial close {percentage}% initiated"
            )
            
        except Exception as e:
            logger.error(f"[{correlation_id}] Partial close error: {e}")
            return TradeResult(success=False, error=str(e), correlation_id=correlation_id)

    async def _check_zombie_positions(self, user: UserSettings, db: Database):
        """Проверка и закрытие 'зомби'-позиций (зависших сделок)"""
        try:
            max_hours = user.risk_settings.get('max_position_hours', 24)
            now = datetime.now(timezone.utc)
            
            for trade_id, monitor in list(self.active_monitors.items()):
                trade = monitor.trade
                
                # Парсим дату открытия
                try:
                    if trade.opened_at.endswith('Z'):
                        opened_at = datetime.fromisoformat(trade.opened_at.replace('Z', '+00:00'))
                    else:
                        opened_at = datetime.fromisoformat(trade.opened_at)
                        opened_at = opened_at.replace(tzinfo=timezone.utc)
                except:
                    continue
                
                hours_open = (now - opened_at).total_seconds() / 3600
                
                if hours_open > max_hours:
                    logger.warning(f"Zombie position detected: trade #{trade_id}, open for {hours_open:.1f} hours")
                    
                    # Закрываем позицию
                    result = await self.close_trade_manually(trade_id, user)
                    
                    if result.success:
                        logger.info(f"Zombie position #{trade_id} closed automatically")
                        # Отправляем уведомление
                        if hasattr(self, '_bot') and self._bot:
                            try:
                                await self._bot.send_message(
                                    chat_id=user.user_id,
                                    text=f"⚠️ Зомби-позиция #{trade_id} закрыта автоматически после {hours_open:.1f} часов"
                                )
                            except:
                                pass
                    else:
                        logger.error(f"Failed to close zombie position #{trade_id}: {result.error}")
                        
        except Exception as e:
            logger.error(f"Zombie check error: {e}")

# ===== КЛАСС PositionMonitor (отдельно, без recover_positions) =====
class PositionMonitor:
    def __init__(self, trade: Trade, user: UserSettings, db: Database, engine: TradingEngine):
        self.trade = trade
        self.user = user
        self.db = db
        self.engine = engine
        self.running = True
        self.last_update = time.time()
        self.update_interval = 5
        self.closing_in_progress = False

    async def run(self):
        logger.info(f"Monitor started for trade {self.trade.id}")

        while self.running:
            try:
                await self._update_prices()
                await self._check_conditions()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"Monitor error for trade {self.trade.id}: {e}")
                await asyncio.sleep(10)

        logger.info(f"Monitor stopped for trade {self.trade.id}")

    async def stop(self):
        self.running = False

    async def _update_prices(self):
        try:
            if not self.trade.metadata.get('test_mode'):
                pass

            if self.trade.current_price_long > 0 and self.trade.current_price_short > 0:
                long_pnl = (self.trade.current_price_long - self.trade.entry_price_long) / self.trade.entry_price_long * 100
                short_pnl = (self.trade.entry_price_short - self.trade.current_price_short) / self.trade.entry_price_short * 100
                self.trade.pnl_percent = (long_pnl + short_pnl) / 2
                self.trade.pnl_usd = self.trade.size_usd * self.trade.pnl_percent / 100

            await self.db.update_trade(self.trade)

        except Exception as e:
            logger.error(f"Price update error: {e}")

    async def _check_conditions(self):
        if self.closing_in_progress:
            return

        current_pnl = self.trade.pnl_percent

        if current_pnl >= self.user.risk_settings.get('take_profit_percent', 20):
            await self._close_position("take_profit")
            return

        if self.trade.trailing_enabled and self.trade.trailing_stop_price > 0:
            long_drawdown = (self.trade.current_price_long - self.trade.entry_price_long) / self.trade.entry_price_long * 100
            if long_drawdown <= -self.user.risk_settings.get('trailing_stop_distance', 10):
                await self._close_position("trailing_stop")
                return

        if self.trade.current_price_long <= self.trade.emergency_stop_price:
            await self._close_position("emergency_stop")
            return

        if not self.trade.breakeven_triggered and current_pnl >= self.user.risk_settings.get('stop_loss_breakeven_trigger', 10):
            self.trade.breakeven_triggered = True
            self.trade.stop_loss_price = self.trade.entry_price_long
            await self.db.update_trade(self.trade)
            logger.info(f"Breakeven triggered for trade {self.trade.id}")

        max_hours = self.user.risk_settings.get('max_position_hours', 24)
        if max_hours > 0:
            opened_at = datetime.fromisoformat(self.trade.opened_at)
            hours_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600
            if hours_open >= max_hours:
                await self._close_position("time_limit")
                return

    async def _close_position(self, reason: str):
        if self.closing_in_progress:
            return

        self.closing_in_progress = True
        logger.info(f"Closing trade {self.trade.id}, reason: {reason}")

        try:
            result = await self.engine.close_trade_manually(self.trade.id, self.user)
            if result.success:
                logger.info(f"Trade {self.trade.id} closed successfully")
            else:
                logger.error(f"Failed to close trade {self.trade.id}: {result.error}")
        except Exception as e:
            logger.error(f"Error closing trade {self.trade.id}: {e}")
        finally:
            self.running = False

# Глобальный экземпляр
trading_engine = TradingEngine()
