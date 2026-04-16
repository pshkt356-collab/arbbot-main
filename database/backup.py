import asyncio
import shutil
import os
import gzip
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
import aiosqlite

logger = logging.getLogger(__name__)

class DatabaseBackup:
    def __init__(self, db_path: str, backup_dir: str = "backups", max_backups: int = 7):
        self.db_path = db_path
        self.backup_dir = Path(backup_dir)
        self.max_backups = max_backups
        self._running = False
        self._task = None
        
        self.backup_dir.mkdir(parents=True, exist_ok=True)
    
    async def start(self, interval_hours: int = 24):
        self._running = True
        self._task = asyncio.create_task(self._backup_loop(interval_hours))
        logger.info(f"Auto-backup started: every {interval_hours}h, max {self.max_backups} backups")
    
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
    
    async def _backup_loop(self, interval_hours: int):
        while self._running:
            try:
                await self.create_backup()
                await self._cleanup_old_backups()
                await asyncio.sleep(interval_hours * 3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Backup loop error: {e}")
                await asyncio.sleep(3600)
    
    async def create_backup(self) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_name = f"arbitrage_bot_{timestamp}.db.gz"
        backup_path = self.backup_dir / backup_name
        
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                
                if result[0] != "ok":
                    raise Exception(f"Database integrity check failed: {result[0]}")
                
                logger.info("Database integrity check passed")
            
            with open(self.db_path, 'rb') as f_in:
                with gzip.open(backup_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            if backup_path.stat().st_size < 1000:
                raise Exception("Backup file too small, possibly corrupted")
            
            logger.info(f"✅ Backup created: {backup_path} ({backup_path.stat().st_size / 1024:.1f} KB)")
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            if backup_path.exists():
                backup_path.unlink()
            raise
    
    async def _cleanup_old_backups(self):
        try:
            backups = sorted(self.backup_dir.glob("arbitrage_bot_*.db.gz"), 
                           key=lambda x: x.stat().st_mtime, reverse=True)
            
            if len(backups) > self.max_backups:
                to_delete = backups[self.max_backups:]
                for backup in to_delete:
                    backup.unlink()
                    logger.info(f"🗑️ Removed old backup: {backup.name}")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    async def restore_backup(self, backup_path: str, target_path: str = None) -> bool:
        if target_path is None:
            target_path = self.db_path
        
        try:
            if os.path.exists(target_path):
                temp_backup = f"{target_path}.bak"
                shutil.copy2(target_path, temp_backup)
            
            with gzip.open(backup_path, 'rb') as f_in:
                with open(target_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            async with aiosqlite.connect(target_path) as conn:
                cursor = await conn.execute("PRAGMA integrity_check")
                result = await cursor.fetchone()
                
                if result[0] != "ok":
                    if os.path.exists(temp_backup):
                        shutil.copy2(temp_backup, target_path)
                    raise Exception(f"Restored database integrity check failed")
            
            logger.info(f"✅ Database restored from: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Restore failed: {e}")
            return False
        finally:
            temp_backup = f"{target_path}.bak"
            if os.path.exists(temp_backup):
                os.remove(temp_backup)
    
    def list_backups(self) -> List[dict]:
        backups = []
        for backup in sorted(self.backup_dir.glob("arbitrage_bot_*.db.gz"), reverse=True):
            stat = backup.stat()
            backups.append({
                'filename': backup.name,
                'created': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'size_kb': round(stat.st_size / 1024, 1),
                'path': str(backup)
            })
        return backups
