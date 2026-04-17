# -*- coding: utf-8 -*-
"""
SQLite-based FSM Storage for aiogram 3.x
Persists FSM states to SQLite database (replaces MemoryStorage)
More reliable than JSON files for concurrent access.
"""
import json
import aiosqlite
from typing import Any, Dict, Optional
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.state import State


class SQLiteStorage(BaseStorage):
    """
    SQLite-based FSM storage that persists states to SQLite database.
    Compatible with aiogram 3.x BaseStorage interface.
    """

    def __init__(self, db_path: str = "/app/data/fsm_storage.db"):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def _get_db(self) -> aiosqlite.Connection:
        """Get or create database connection"""
        if self._db is None:
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.execute("""
                CREATE TABLE IF NOT EXISTS fsm_states (
                    key TEXT PRIMARY KEY,
                    state TEXT,
                    data TEXT DEFAULT '{}'
                )
            """)
            await self._db.commit()
        return self._db

    def _make_key(self, key: StorageKey) -> str:
        """Generate unique key for storage entry"""
        return f"{key.bot_id}:{key.chat_id}:{key.user_id}"

    async def set_state(self, key: StorageKey, state: Optional[State] = None) -> None:
        """Set state for a key"""
        db = await self._get_db()
        str_key = self._make_key(key)
        state_str = state.state if state else None

        await db.execute(
            """INSERT INTO fsm_states (key, state, data) VALUES (?, ?, '{}')
               ON CONFLICT(key) DO UPDATE SET state=excluded.state""",
            (str_key, state_str)
        )
        await db.commit()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        """Get state for a key"""
        db = await self._get_db()
        str_key = self._make_key(key)

        async with db.execute(
            "SELECT state FROM fsm_states WHERE key = ?", (str_key,)
        ) as cursor:
            row = await cursor.fetchone()
            return row['state'] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """Set data for a key"""
        db = await self._get_db()
        str_key = self._make_key(key)
        data_json = json.dumps(data)

        await db.execute(
            """INSERT INTO fsm_states (key, state, data) VALUES (?, NULL, ?)
               ON CONFLICT(key) DO UPDATE SET data=excluded.data""",
            (str_key, data_json)
        )
        await db.commit()

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        """Get data for a key"""
        db = await self._get_db()
        str_key = self._make_key(key)

        async with db.execute(
            "SELECT data FROM fsm_states WHERE key = ?", (str_key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row and row['data']:
                return json.loads(row['data'])
            return {}

    async def update_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """Update data for a key (merge with existing)"""
        db = await self._get_db()
        str_key = self._make_key(key)

        # Get existing data
        async with db.execute(
            "SELECT data FROM fsm_states WHERE key = ?", (str_key,)
        ) as cursor:
            row = await cursor.fetchone()
            existing = json.loads(row['data']) if row and row['data'] else {}

        # Merge and save
        existing.update(data)
        data_json = json.dumps(existing)

        await db.execute(
            """INSERT INTO fsm_states (key, state, data) VALUES (?, NULL, ?)
               ON CONFLICT(key) DO UPDATE SET data=excluded.data""",
            (str_key, data_json)
        )
        await db.commit()

    async def close(self) -> None:
        """Close database connection"""
        if self._db:
            await self._db.close()
            self._db = None

    async def cleanup(self, chat_id: Optional[int] = None, user_id: Optional[int] = None) -> None:
        """Cleanup old/expired states"""
        db = await self._get_db()

        if chat_id is not None and user_id is not None:
            # Delete specific user
            key = f"%:{chat_id}:{user_id}"
            await db.execute("DELETE FROM fsm_states WHERE key LIKE ?", (key,))
        elif chat_id is not None:
            key = f"%:{chat_id}:%"
            await db.execute("DELETE FROM fsm_states WHERE key LIKE ?", (key,))
        elif user_id is not None:
            key = f"%:%:{user_id}"
            await db.execute("DELETE FROM fsm_states WHERE key LIKE ?", (key,))

        await db.commit()
