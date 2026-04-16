import asyncio
import time
import logging
from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

@dataclass
class ExchangeStatus:
    exchange_id: str
    failures: int = 0
    last_failure: Optional[float] = None
    last_success: Optional[float] = None
    is_available: bool = True
    status_message: str = "OK"
    circuit_open_since: Optional[float] = None
    consecutive_successes: int = 0

class ExchangeCircuitBreaker:
    """Управление доступностью бирж"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 600,
                 half_open_max_calls: int = 3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.exchanges: Dict[str, ExchangeStatus] = {}
        self._lock = asyncio.Lock()
        self._recovery_task: Optional[asyncio.Task] = None
        
    def _get_status(self, exchange_id: str) -> ExchangeStatus:
        if exchange_id not in self.exchanges:
            self.exchanges[exchange_id] = ExchangeStatus(exchange_id=exchange_id)
        return self.exchanges[exchange_id]
    
    async def record_failure(self, exchange_id: str, error: str):
        async with self._lock:
            status = self._get_status(exchange_id)
            status.failures += 1
            status.last_failure = time.time()
            status.consecutive_successes = 0
            
            if status.failures >= self.failure_threshold and status.is_available:
                status.is_available = False
                status.circuit_open_since = time.time()
                status.status_message = f"Circuit open: {error[:50]}"
                logger.warning(f"🔴 Circuit breaker OPEN for {exchange_id}: {error}")
                
                if not self._recovery_task or self._recovery_task.done():
                    self._recovery_task = asyncio.create_task(self._recovery_loop())
    
    async def record_success(self, exchange_id: str):
        async with self._lock:
            status = self._get_status(exchange_id)
            status.last_success = time.time()
            status.consecutive_successes += 1
            
            if not status.is_available:
                if status.consecutive_successes >= self.half_open_max_calls:
                    status.is_available = True
                    status.failures = 0
                    status.circuit_open_since = None
                    status.status_message = "Recovered"
                    logger.info(f"🟢 Circuit breaker CLOSED for {exchange_id}")
    
    async def can_execute(self, exchange_id: str) -> bool:
        async with self._lock:
            status = self._get_status(exchange_id)
            
            if status.is_available:
                return True
            
            if status.circuit_open_since:
                elapsed = time.time() - status.circuit_open_since
                if elapsed >= self.recovery_timeout:
                    logger.info(f"🟡 Circuit breaker HALF-OPEN for {exchange_id}")
                    return True
            
            return False
    
    async def _recovery_loop(self):
        while True:
            try:
                await asyncio.sleep(60)
                
                async with self._lock:
                    for status in self.exchanges.values():
                        if not status.is_available and status.circuit_open_since:
                            elapsed = time.time() - status.circuit_open_since
                            if elapsed >= self.recovery_timeout:
                                logger.info(f"🔄 Attempting recovery for {status.exchange_id}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Recovery loop error: {e}")
    
    def get_status_summary(self) -> Dict[str, dict]:
        result = {}
        for ex_id, status in self.exchanges.items():
            result[ex_id] = {
                'available': status.is_available,
                'failures': status.failures,
                'status': status.status_message,
                'last_failure': datetime.fromtimestamp(status.last_failure).isoformat() if status.last_failure else None,
                'circuit_open': status.circuit_open_since is not None
            }
        return result
    
    def stop(self):
        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()

circuit_breaker = ExchangeCircuitBreaker()
