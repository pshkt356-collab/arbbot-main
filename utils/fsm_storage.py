"""
Custom JSON-based FSM Storage for aiogram 3.x
Persists FSM states to filesystem (replaces MemoryStorage)
"""
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.state import State


class JSONFileStorage(BaseStorage):
    """
    JSON-based FSM storage that persists states to filesystem.
    Compatible with aiogram 3.x BaseStorage interface.
    """
    
    def __init__(self, storage_path: str = "/app/data/fsm_storage"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._states: Dict[str, Dict[str, Any]] = {}
        self._data: Dict[str, Dict[str, Any]] = {}
        self._loaded = False
    
    def _get_key(self, key: StorageKey) -> str:
        """Generate unique key for storage entry"""
        return f"{key.bot_id}:{key.chat_id}:{key.user_id}"
    
    def _get_state_file(self, key: str) -> Path:
        """Get path to state file for a key"""
        # Use hashed key for filename to avoid special characters
        safe_key = key.replace(":", "_")
        return self.storage_path / f"{safe_key}.json"
    
    def _load_all(self):
        """Load all states from disk (lazy loading)"""
        if self._loaded:
            return
        
        try:
            for file_path in self.storage_path.glob("*.json"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        key = file_path.stem.replace("_", ":")
                        # state can be a string or None
                        if 'state' in data and data['state'] is not None:
                            self._states[key] = data['state']
                        if 'data' in data:
                            self._data[key] = data['data']
                except (json.JSONDecodeError, IOError):
                    continue
            self._loaded = True
        except Exception:
            pass
    
    def _save(self, key: str):
        """Save state and data for a key to disk"""
        try:
            file_path = self._get_state_file(key)
            data = {
                'state': self._states.get(key),
                'data': self._data.get(key, {})
            }
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except (IOError, OSError):
            pass
    
    def _delete(self, key: str):
        """Delete state file for a key"""
        try:
            file_path = self._get_state_file(key)
            if file_path.exists():
                file_path.unlink()
        except (IOError, OSError):
            pass
    
    async def set_state(self, key: StorageKey, state: Optional[State] = None) -> None:
        """Set state for a key"""
        self._load_all()
        str_key = self._get_key(key)
        
        if state is None:
            self._states.pop(str_key, None)
        else:
            # Store only the state string, not the State object (which contains non-serializable group)
            self._states[str_key] = state.state
        
        self._save(str_key)
    
    async def get_state(self, key: StorageKey) -> Optional[str]:
        """Get state for a key"""
        self._load_all()
        str_key = self._get_key(key)
        return self._states.get(str_key)
    
    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """Set data for a key"""
        self._load_all()
        str_key = self._get_key(key)
        self._data[str_key] = data
        self._save(str_key)
    
    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        """Get data for a key"""
        self._load_all()
        str_key = self._get_key(key)
        return self._data.get(str_key, {}).copy()
    
    async def update_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        """Update data for a key"""
        self._load_all()
        str_key = self._get_key(key)
        
        if str_key not in self._data:
            self._data[str_key] = {}
        
        self._data[str_key].update(data)
        self._save(str_key)
    
    async def close(self) -> None:
        """Close storage - sync all to disk"""
        # All data is already saved, just clear memory
        self._states.clear()
        self._data.clear()
    
    async def cleanup(self, chat_id: Optional[int] = None, user_id: Optional[int] = None) -> None:
        """Cleanup old/expired states"""
        self._load_all()
        
        keys_to_delete = []
        for key in list(self._states.keys()):
            parts = key.split(":")
            if len(parts) == 3:
                key_chat_id = int(parts[1])
                key_user_id = int(parts[2])
                
                if (chat_id is None or key_chat_id == chat_id) and \
                   (user_id is None or key_user_id == user_id):
                    keys_to_delete.append(key)
        
        for key in keys_to_delete:
            self._states.pop(key, None)
            self._data.pop(key, None)
            self._delete(key)
