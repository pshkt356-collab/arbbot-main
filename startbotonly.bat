@echo off
chcp 65001 >nul
title Arbitrage Bot - Telegram
echo.
echo  🤖 Запуск Telegram бота...
echo.
cd /d "%~dp0"
python main.py
pause