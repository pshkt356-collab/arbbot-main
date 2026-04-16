@echo off
chcp 65001 >nul
title Arbitrage Bot - Full Launch
echo.
echo  🚀 Запуск ПОЛНОГО комплекта Arbitrage Bot
echo  =========================================
echo.
cd /d "%~dp0"

echo  [1/3] Запуск Telegram бота...
start "Telegram Bot" cmd /k "python main.py"

echo  [2/3] Запуск Дашборда...
timeout /t 2 >nul
start "Dashboard" cmd /k "streamlit run web/dashboard.py --server.port=8501"

echo  [3/3] Запуск Панели управления...
timeout /t 2 >nul
start "Control Panel" cmd /k "streamlit run web/bot_control.py --server.port=8502"

echo.
echo  ✅ ВСЕ компоненты запущены!
echo.
echo  📱 Telegram бот: работает в фоне
echo  📊 Дашборд:      http://localhost:8501
echo  🤖 Управление:   http://localhost:8502
echo.
pause