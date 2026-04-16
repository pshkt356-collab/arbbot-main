import asyncio
import aiohttp
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ExchangeHealth:
    exchange_id: str
    is_operational: bool
    is_maintenance: bool
    status_message: str
    last_check: datetime
    next_check: datetime

class ExchangeStatusChecker:
    def __init__(self, check_interval: int = 60):
        self.check_interval = check_interval
        self._statuses: Dict[str, ExchangeHealth] = {}
        self._running = False
        self._task = None
        
    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("Exchange status checker started")
    
    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
    
    async def _check_loop(self):
        while self._running:
            try:
                await self._check_all_exchanges()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Status check loop error: {e}")
                await asyncio.sleep(10)
    
    async def _check_all_exchanges(self):
        tasks = [
            self._check_binance(),
            self._check_bybit(),
            self._check_okx(),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_binance(self):
        try:
            url = "https://fapi.binance.com/fapi/v1/ping"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    is_up = resp.status == 200
                    
            self._statuses['binance'] = ExchangeHealth(
                exchange_id='binance',
                is_operational=is_up,
                is_maintenance=False,
                status_message='OK' if is_up else 'No response',
                last_check=datetime.now(),
                next_check=datetime.now() + timedelta(seconds=self.check_interval)
            )
        except Exception as e:
            self._statuses['binance'] = ExchangeHealth(
                exchange_id='binance',
                is_operational=False,
                is_maintenance=False,
                status_message=str(e)[:50],
                last_check=datetime.now(),
                next_check=datetime.now() + timedelta(seconds=self.check_interval)
            )
    
    async def _check_bybit(self):
        try:
            url = "https://api.bybit.com/v5/market/time"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    data = await resp.json()
                    is_up = resp.status == 200 and data.get('retCode') == 0
            
            self._statuses['bybit'] = ExchangeHealth(
                exchange_id='bybit',
                is_operational=is_up,
                is_maintenance=False,
                status_message='OK' if is_up else 'API error',
                last_check=datetime.now(),
                next_check=datetime.now() + timedelta(seconds=self.check_interval)
            )
        except Exception as e:
            self._statuses['bybit'] = ExchangeHealth(
                exchange_id='bybit',
                is_operational=False,
                is_maintenance=False,
                status_message=str(e)[:50],
                last_check=datetime.now(),
                next_check=datetime.now() + timedelta(seconds=self.check_interval)
            )
    
    async def _check_okx(self):
        try:
            url = "https://www.okx.com/api/v5/system/status"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        maintenance = False
                        if data.get('data'):
                            for item in data['data']:
                                if item.get('state') != '0':
                                    maintenance = True
                        
                        self._statuses['okx'] = ExchangeHealth(
                            exchange_id='okx',
                            is_operational=not maintenance,
                            is_maintenance=maintenance,
                            status_message='Maintenance' if maintenance else 'OK',
                            last_check=datetime.now(),
                            next_check=datetime.now() + timedelta(seconds=self.check_interval)
                        )
                    else:
                        raise Exception(f"HTTP {resp.status}")
        except Exception as e:
            self._statuses['okx'] = ExchangeHealth(
                exchange_id='okx',
                is_operational=False,
                is_maintenance=False,
                status_message=str(e)[:50],
                last_check=datetime.now(),
                next_check=datetime.now() + timedelta(seconds=self.check_interval)
            )
    
    def is_exchange_available(self, exchange_id: str) -> bool:
        status = self._statuses.get(exchange_id.lower())
        if not status:
            return True
        return status.is_operational and not status.is_maintenance
    
    def get_status(self, exchange_id: str) -> Optional[ExchangeHealth]:
        return self._statuses.get(exchange_id.lower())
    
    def get_all_statuses(self) -> Dict[str, ExchangeHealth]:
        return dict(self._statuses)
    
    def get_available_exchanges(self) -> list:
        return [
            ex_id for ex_id, status in self._statuses.items()
            if status.is_operational and not status.is_maintenance
        ]

status_checker = ExchangeStatusChecker()
