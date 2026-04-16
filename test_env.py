from dotenv import load_dotenv
import os

load_dotenv()
token = os.getenv('TELEGRAM_BOT_TOKEN')

print(f"Токен найден: {'ДА' if token else 'НЕТ'}")
print(f"Длина токена: {len(token) if token else 0}")
print(f"Первые 20 символов: {token[:20] if token else 'НЕТ'}...")