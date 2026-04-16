import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
import aiosqlite

logger = logging.getLogger(__name__)

class DatabaseArchiver:
    def __init__(self, db: 'Database'):
        self.db = db
        self._running = False
        self._task = None
        
    async def start(self, archive_interval_hours: int = 24):
        self._running = True
        self._task = asyncio.create_task(self._archive_loop(archive_interval_hours))
        logger.info("Database archiver started")
    
    async def stop(self):
    self._running = False
    if self._task and not self._task.done():
        self._task.cancel()
        try:
            await asyncio.wait_for(self._task, timeout=1.0)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            pass
    
    async def _archive_loop(self, interval_hours: int):
        await asyncio.sleep(3600)
        
        while self._running:
            try:
                await self.archive_old_trades(days=30)
                await self.cleanup_spread_history(days=7)
                await self.vacuum_database()
                
                await asyncio.sleep(interval_hours * 3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Archive loop error: {e}")
                await asyncio.sleep(3600)
    
    async def archive_old_trades(self, days: int = 30):
        try:
            async with self.db._query_lock:
                await self.db._conn.execute("""
                    CREATE TABLE IF NOT EXISTS trades_archive (
                        LIKE trades INCLUDING ALL
                    )
                """)
                
                cursor = await self.db._conn.execute("""
                    INSERT INTO trades_archive 
                    SELECT * FROM trades 
                    WHERE status = 'closed' 
                    AND closed_at < datetime('now', ?)
                """, (f'-{days} days',))
                
                archived_count = cursor.rowcount
                
                if archived_count > 0:
                    await self.db._conn.execute("""
                        DELETE FROM trades 
                        WHERE status = 'closed' 
                        AND closed_at < datetime('now', ?)
                    """, (f'-{days} days',))
                    
                    await self.db._conn.commit()
                    logger.info(f"Archived {archived_count} trades older than {days} days")
        except Exception as e:
            logger.error(f"Archive trades error: {e}")
    
    async def cleanup_spread_history(self, days: int = 7):
        try:
            async with self.db._query_lock:
                cursor = await self.db._conn.execute("""
                    DELETE FROM spread_history 
                    WHERE timestamp < datetime('now', ?)
                """, (f'-{days} days',))
                
                deleted = cursor.rowcount
                await self.db._conn.commit()
                
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} spread history records")
        except Exception as e:
            logger.error(f"Cleanup spread history error: {e}")
    
    async def vacuum_database(self):
        try:
            async with self.db._query_lock:
                cursor = await self.db._conn.execute("PRAGMA freelist_count")
                freelist = await cursor.fetchone()
                
                if freelist and freelist[0] > 1000:
                    logger.info("Running VACUUM to optimize database...")
                    await self.db._conn.execute("VACUUM")
                    logger.info("Database VACUUM completed")
        except Exception as e:
            logger.error(f"Vacuum error: {e}")
    
    async def get_archive_stats(self) -> dict:
        try:
            async with self.db._query_lock:
                cursor = await self.db._conn.execute(
                    "SELECT COUNT(*) FROM trades_archive"
                )
                archive_count = (await cursor.fetchone())[0]
                
                cursor = await self.db._conn.execute(
                    "SELECT COUNT(*) FROM trades WHERE status = 'open'"
                )
                active_count = (await cursor.fetchone())[0]
                
                cursor = await self.db._conn.execute(
                    "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()"
                )
                db_size = (await cursor.fetchone())[0]
                
                return {
                    'archived_trades': archive_count,
                    'active_trades': active_count,
                    'db_size_bytes': db_size,
                    'db_size_mb': round(db_size / (1024 * 1024), 2)
                }
        except Exception as e:
            logger.error(f"Archive stats error: {e}")
            return {}
