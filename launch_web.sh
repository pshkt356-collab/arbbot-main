
#!/bin/bash

echo "🚀 Запуск Arbitrage Bot Web Interface..."

# Проверка наличия Streamlit
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit не установлен. Установка..."
    pip install streamlit plotly pandas
fi

# Запуск дашборда
echo "📊 Запуск дашборда на http://localhost:8501"
streamlit run web/dashboard.py --server.port=8501 --server.address=0.0.0.0 &

# Запуск панели управления
echo "🤖 Запуск панели управления на http://localhost:8502"
streamlit run web/bot_control.py --server.port=8502 --server.address=0.0.0.0 &

echo ""
echo "✅ Оба интерфейса запущены!"
echo "📊 Дашборд: http://localhost:8501"
echo "🤖 Управление: http://localhost:8502"
echo ""
echo "Нажмите Ctrl+C для остановки"

wait
