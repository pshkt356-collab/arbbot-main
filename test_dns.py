import os
import sys
import asyncio
import socket
import ssl
import aiohttp
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

print("=" * 50)
print("🔍 ДИАГНОСТИКА DNS И ПОДКЛЮЧЕНИЯ")
print("=" * 50)
print(f"Токен: {TOKEN[:20]}..." if TOKEN else "❌ Токен не найден!")
print(f"Python: {sys.version}")
print()

async def test_with_resolver():
    """Тест с кастомным DNS resolver"""
    print("Тест 1: Google DNS (8.8.8.8)...")
    try:
        resolver = aiohttp.AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
        connector = aiohttp.TCPConnector(resolver=resolver, ssl=True)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=10) as resp:
                data = await resp.json()
                print(f"✅ УСПЕХ: {data['result']['username']}")
                return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

async def test_without_ssl():
    """Тест без SSL проверки"""
    print("\nТест 2: Без SSL проверки...")
    try:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=10) as resp:
                data = await resp.json()
                print(f"✅ УСПЕХ: {data['result']['username']}")
                return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

async def test_ip_direct():
    """Тест по прямому IP"""
    print("\nТест 3: Прямое подключение по IP...")
    try:
        # Получаем IP api.telegram.org
        ip = socket.getaddrinfo("api.telegram.org", None)[0][4][0]
        print(f"IP адрес: {ip}")
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        connector = aiohttp.TCPConnector(ssl=ssl_context, force_close=True)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            url = f"https://{ip}/bot{TOKEN}/getMe"
            headers = {"Host": "api.telegram.org"}
            async with session.get(url, headers=headers, timeout=10) as resp:
                data = await resp.json()
                print(f"✅ УСПЕХ: {data['result']['username']}")
                return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

async def main():
    if not TOKEN:
        print("❌ Сначала создайте .env файл с TELEGRAM_BOT_TOKEN!")
        return
    
    results = []
    
    results.append(await test_with_resolver())
    results.append(await test_without_ssl())
    results.append(await test_ip_direct())
    
    print("\n" + "=" * 50)
    if any(results):
        print("✅ ХОТЯ БЫ ОДИН ТЕСТ ПРОШЁЛ!")
        print("Используйте рабочий вариант в main.py")
    else:
        print("❌ ВСЕ ТЕСТЫ ПРОВАЛЕНЫ")
        print("Проблема в системе/сети, не в коде")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(main())