import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone
from aiohttp import web

from aiogram import Bot, Dispatcher
from functools import partial

# SQLite-based FSM storage for reliable state persistence
from utils.fsm_storage import SQLiteStorage

from config import settings
from database.models import Database
from database.backup import DatabaseBackup
from database.archive import DatabaseArchiver
from services.spread_scanner import SpreadScanner
from services.trading_engine import trading_engine
from services.circuit_breaker import circuit_breaker
from services.notification import init_alert_manager, alert_manager
from services.exchange_status import status_checker
from services.mexc_flip_trader import flip_trader
from handlers.commands import commands_router
from handlers.callbacks import callbacks_router, send_spread_alert, subscribe_user_to_alerts, set_bot
from handlers.states import states_router
from middleware.user_context import UserContextMiddleware, ScannerMiddleware
from middleware.rate_limiter import UserRateLimiter, DoubleSubmitProtection

import os
from pathlib import Path

# Create debug logs directory
DEBUG_LOG_DIR = Path("/app/data/debug_logs")
DEBUG_LOG_DIR.mkdir(parents=True, exist_ok=True)

# Main logging setup
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add debug file handler for FSM operations
debug_handler = logging.FileHandler(DEBUG_LOG_DIR / "fsm_debug.log", encoding='utf-8')
debug_handler.setLevel(logging.DEBUG)
debug_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))

# Add handler to root logger for FSM debug
fsm_logger = logging.getLogger("utils.fsm_storage")
fsm_logger.setLevel(logging.DEBUG)
fsm_logger.addHandler(debug_handler)

states_logger = logging.getLogger("handlers.states")
states_logger.setLevel(logging.DEBUG)
states_logger.addHandler(debug_handler)

callbacks_logger = logging.getLogger("handlers.callbacks")
callbacks_logger.setLevel(logging.DEBUG)
callbacks_logger.addHandler(debug_handler)

logger = logging.getLogger(__name__)

scanner = None
db = None
bot = None
backup_manager = None
archiver = None
app = None
runner = None
site = None

async def health_handler(request):
    """HTTP health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {}
    }

    try:
        if db and db._initialized:
            health_status["services"]["database"] = "connected"
        else:
            health_status["services"]["database"] = "disconnected"
            health_status["status"] = "unhealthy"
    except Exception as e:
        health_status["services"]["database"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"

    try:
        if scanner and scanner.running:
            active_streams = sum(1 for v in scanner.stats['connections'].values() if v)
            health_status["services"]["scanner"] = {
                "status": "running",
                "mode": "degraded" if getattr(scanner, '_degraded_mode', False) else "normal",
                "active_streams": f"{active_streams}/10"
            }
            if getattr(scanner, '_degraded_mode', False):
                health_status["status"] = "degraded"
        else:
            health_status["services"]["scanner"] = "stopped"
            health_status["status"] = "unhealthy"
    except Exception as e:
        health_status["services"]["scanner"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"

    try:
        if trading_engine:
            active_monitors = len(trading_engine.active_monitors)
            health_status["services"]["trading_engine"] = {
                "status": "running",
                "active_monitors": active_monitors
            }
    except Exception as e:
        health_status["services"]["trading_engine"] = f"error: {str(e)}"

    try:
        cb_status = circuit_breaker.get_status_summary()
        health_status["services"]["circuit_breaker"] = cb_status
    except:
        pass

    try:
        if flip_trader and flip_trader.running:
            active_sessions = len(flip_trader.active_sessions)
            health_status["services"]["flip_trader"] = {
                "status": "running",
                "active_sessions": active_sessions
            }
        else:
            health_status["services"]["flip_trader"] = "stopped"
    except Exception as e:
        health_status["services"]["flip_trader"] = f"error: {str(e)}"

    status_code = 200 if health_status["status"] == "healthy" else 503
    return web.json_response(health_status, status=status_code)

async def metrics_handler(request):
    """Prometheus-style metrics"""
    metrics = []

    if scanner:
        metrics.append(f'scanner_spreads_total {scanner.stats["spreads_found"]}')
        metrics.append(f'scanner_basis_total {scanner.stats["basis_found"]}')
        active_streams = sum(1 for v in scanner.stats['connections'].values() if v)
        metrics.append(f'scanner_active_streams {active_streams}')

    if trading_engine:
        metrics.append(f'trading_active_monitors {len(trading_engine.active_monitors)}')

    return web.Response(text="\n".join(metrics), content_type="text/plain")

async def start_health_server(host='0.0.0.0', port=8080):
    global app, runner, site

    app = web.Application()
    app.router.add_get('/health', health_handler)
    app.router.add_get('/metrics', metrics_handler)
    app.router.add_get('/ready', health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)

    await site.start()
    logger.info(f"Health check server started on http://{host}:{port}/health")
    return site

async def stop_health_server():
    global runner, site

    if site:
        await site.stop()
    if runner:
        await runner.cleanup()
    logger.info("Health check server stopped")
    
async def _stop_with_timeout(stop_func, name, timeout=2.0):
    """Остановка компонента с таймаутом"""
    try:
        await asyncio.wait_for(stop_func(), timeout=timeout)
        logger.info(f"{name} stopped")
    except asyncio.TimeoutError:
        logger.warning(f"{name} stop timeout ({timeout}s), forcing...")
    except Exception as e:
        logger.error(f"{name} stop error: {e}")

async def shutdown(signal_name=None):
    """Graceful shutdown with timeout for Railway (10s limit)"""
    logger.info(f"Received exit signal {signal_name}...")
    
    shutdown_start = asyncio.get_event_loop().time()
    max_shutdown_time = 8.0
    
    async def _do_shutdown():
        await _stop_with_timeout(stop_health_server, "Health server", timeout=1.0)
        
        if backup_manager:
            await _stop_with_timeout(backup_manager.stop, "Backup manager", timeout=1.0)
        
        if archiver:
            await _stop_with_timeout(archiver.stop, "Archiver", timeout=1.0)
        
        if scanner:
            await _stop_with_timeout(scanner.stop, "Scanner", timeout=2.0)
        
        if status_checker:
            await _stop_with_timeout(status_checker.stop, "Status checker", timeout=1.0)
        
        if trading_engine:
            try:
                trading_engine.stop()
                logger.info("Trading engine stopped")
            except Exception as e:
                logger.error(f"Trading engine stop error: {e}")

        try:
            await _stop_with_timeout(flip_trader.stop, "Flip trader", timeout=2.0)
        except Exception as e:
            logger.error(f"Flip trader stop error: {e}")
        
        try:
            circuit_breaker.stop()
            logger.info("Circuit breaker stopped")
        except Exception as e:
            logger.error(f"Circuit breaker stop error: {e}")
        
        if db:
            await _stop_with_timeout(db.close, "Database", timeout=1.0)
        
        if bot:
            try:
                await asyncio.wait_for(bot.session.close(), timeout=1.0)
                logger.info("Bot session closed")
            except asyncio.TimeoutError:
                logger.warning("Bot session close timeout, forcing...")
            except Exception as e:
                logger.error(f"Bot session close error: {e}")
    
    try:
        await asyncio.wait_for(_do_shutdown(), timeout=max_shutdown_time)
        elapsed = asyncio.get_event_loop().time() - shutdown_start
        logger.info(f"Shutdown complete in {elapsed:.2f}s")
    except asyncio.TimeoutError:
        logger.warning(f"Shutdown timeout ({max_shutdown_time}s), forcing exit...")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")
    finally:
        sys.exit(0)

def handle_signal(sig):
    asyncio.create_task(shutdown(signal_name=sig.name))

async def subscribe_existing_users():
    """Подписка существующих пользователей на алерты при старте бота"""
    try:
        global db, scanner
        if not db or not scanner:
            logger.warning("Cannot subscribe users: db or scanner not initialized")
            return

        users = await db.get_all_users()
        subscribed_count = 0

        for user in users:
            # Пропускаем если алерты выключены или бот заблокирован
            if not user.alerts_enabled or user.bot_blocked:
                continue
            
            if user.min_spread_threshold > 0:
                # Проверяем, не подписан ли уже
                already_subscribed = False
                for sub in scanner.subscribers:
                    # Проверяем если subscriber - это tuple с user_id
                    if isinstance(sub, tuple) and len(sub) >= 2 and sub[1] == user.user_id:
                        already_subscribed = True
                        break
                    # Проверяем если это partial функция (сложнее проверить)
                
                if not already_subscribed:
                    scanner.subscribe(send_spread_alert, user.user_id)
                    scanner.set_user_threshold(
                        user.user_id,
                        user.min_spread_threshold,
                        alerts_enabled=user.alerts_enabled
                    )
                    subscribed_count += 1
                    logger.info(f"Auto-subscribed user {user.user_id} to spread alerts (threshold: {user.min_spread_threshold}%)")

        logger.info(f"Total users subscribed to alerts: {subscribed_count}/{len(users)}")
    except Exception as e:
        logger.error(f"Error subscribing existing users: {e}")
        
async def main():
    global scanner, db, bot, backup_manager, archiver

    logger.info("🚀 Starting Arbitrage Bot...")

    # Инициализация БД первым делом
    db = Database(settings.db_file)
    await db.initialize()
    logger.info(f"Database initialized: {settings.db_file}")

    backup_manager = DatabaseBackup(settings.db_file)
    await backup_manager.start(interval_hours=24)

    archiver = DatabaseArchiver(db)
    await archiver.start(archive_interval_hours=24)

    await status_checker.start()

    scanner = SpreadScanner(
        min_spread=0.2,
        check_interval=settings.scan_interval
    )

    bot = Bot(token=settings.telegram_bot_token)
    
    # Устанавливаем бота для callbacks
    set_bot(bot)
    
    init_alert_manager(bot, settings.telegram_admin_id)

    # Use SQLiteStorage for reliable FSM state persistence
    storage = SQLiteStorage(settings.fsm_storage_path)
    dp = Dispatcher(storage=storage)

    # Middleware порядок важен!
    dp.message.middleware(UserRateLimiter(max_requests=20, window=60))
    dp.callback_query.middleware(UserRateLimiter(max_requests=30, window=60))
    dp.callback_query.middleware(DoubleSubmitProtection(cooldown=2.0))

    # Scanner middleware передает scanner в хендлеры
    dp.message.middleware(ScannerMiddleware(scanner))
    dp.callback_query.middleware(ScannerMiddleware(scanner))

    # UserContextMiddleware передает user, db и scanner
    user_middleware = UserContextMiddleware(scanner, db)
    dp.message.middleware(user_middleware)
    dp.callback_query.middleware(user_middleware)

    dp.include_router(commands_router)
    dp.include_router(callbacks_router)
    dp.include_router(states_router)

    scanner_task = asyncio.create_task(scanner.start(), name="scanner")
    cleanup_task = asyncio.create_task(trading_engine._cleanup_cache(), name="cache_cleanup")
    health_task = asyncio.create_task(start_health_server(), name="health")
    flip_task = asyncio.create_task(flip_trader.start(), name="flip_trader")

    await subscribe_existing_users()

    async def zombie_check_loop():
        while getattr(trading_engine, 'running', True):
            try:
                await asyncio.sleep(300)
                if settings.telegram_admin_id:
                    admin_user = await db.get_user(settings.telegram_admin_id)
                    if admin_user:
                        await trading_engine._check_zombie_positions(admin_user, db)
            except Exception as e:
                logger.error(f"Zombie check loop error: {e}")

    zombie_task = asyncio.create_task(zombie_check_loop(), name="zombie_checker")

    logger.info("Recovering positions...")
    try:
        # ИСПРАВЛЕНО: recover_positions не принимает аргументов
        await trading_engine.recover_positions()

        if alert_manager:
            await alert_manager.info("Bot started successfully", source="system")
    except Exception as e:
        logger.error(f"Error recovering positions: {e}")
        if alert_manager:
            await alert_manager.critical(f"Position recovery failed: {str(e)}", source="system")

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    logger.info("✅ All services started. Health check: http://localhost:8080/health")

    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Bot polling error: {e}")
        if alert_manager:
            await alert_manager.critical(f"Bot polling error: {str(e)}", source="system")
    finally:
        await shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
