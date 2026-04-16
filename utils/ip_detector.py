import aiohttp
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

class IPDetector:
    IP_SERVICES = [
        "https://api.ipify.org?format=json",
        "https://ip.seeip.org/json",
        "https://api.my-ip.io/ip.json",
        "https://api.ip.sb/geoip",
    ]
    
    def __init__(self):
        self._cached_ip: Optional[str] = None
        self._cache_time: float = 0
        self._cache_ttl = 3600
    
    async def get_public_ip(self, force_refresh: bool = False) -> Optional[str]:
        import time
        
        if not force_refresh and self._cached_ip:
            if time.time() - self._cache_time < self._cache_ttl:
                return self._cached_ip
        
        for service in self.IP_SERVICES:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.get(service) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            ip = data.get('ip') or data.get('origin')
                            if ip:
                                self._cached_ip = ip
                                self._cache_time = time.time()
                                logger.info(f"Detected public IP: {ip}")
                                return ip
            except Exception as e:
                logger.debug(f"IP service {service} failed: {e}")
                continue
        
        logger.error("Failed to detect public IP from all services")
        return None
    
    def get_ip_message(self) -> str:
        if self._cached_ip:
            return f"""
⚠️ <b>Важно: Настройка IP Whitelist</b>

Ваш текущий IP: <code>{self._cached_ip}</code>

Для работы API ключей добавьте этот IP в whitelist на бирже:
1. Зайдите в настройки API на бирже
2. Включите IP ограничение (IP Whitelist)
3. Добавьте: <code>{self._cached_ip}</code>

<i>Если у вас динамический IP, может потребоваться обновление настроек.</i>
            """.strip()
        return "⚠️ Не удалось определить IP. Проверьте настройки вручную."
    
    def validate_ip(self, ip: str) -> bool:
        import re
        pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        return bool(re.match(pattern, ip))
    
    def is_private_ip(self, ip: str) -> bool:
        parts = ip.split('.')
        if len(parts) != 4:
            return True
        
        try:
            first, second = int(parts[0]), int(parts[1])
            if first == 10:
                return True
            if first == 172 and 16 <= second <= 31:
                return True
            if first == 192 and second == 168:
                return True
            if first == 127:
                return True
            return False
        except:
            return True

ip_detector = IPDetector()
