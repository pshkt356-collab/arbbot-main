
import streamlit as st
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import Database, UserSettings

st.set_page_config(
    page_title="Bot Control Panel",
    page_icon="🤖",
    layout="wide"
)

def render_user_management():
    """Управление пользователями"""
    st.header("👥 Управление пользователями")

    db = Database()

    # В реальности здесь должен быть метод get_all_users()
    # Пока показываем заглушку
    users_data = {
        'ID': [123456, 789012],
        'Username': ['@trader1', '@trader2'],
        'Торговля': ['✅', '❌'],
        'API ключей': [2, 0],
        'Комиссия': ['0.02%/0.05%', '0.04%/0.08%'],
        'Дата регистрации': ['2024-01-15', '2024-01-20']
    }

    df = pd.DataFrame(users_data)
    st.dataframe(df, use_container_width=True)

def render_trade_monitoring():
    """Мониторинг сделок"""
    st.header("💼 Мониторинг сделок")

    # Фильтры
    col1, col2, col3 = st.columns(3)
    with col1:
        status = st.selectbox("Статус", ["Все", "Открытые", "Закрытые"])
    with col2:
        symbol = st.text_input("Пара", "")
    with col3:
        date_range = st.date_input("Период", [datetime.now(), datetime.now()])

    # Таблица сделок
    trades_data = {
        'ID': [1, 2, 3],
        'Пользователь': ['@trader1', '@trader1', '@trader2'],
        'Пара': ['BTC', 'ETH', 'SOL'],
        'Стратегия': ['Cross-Exchange', 'Funding', 'Cross-Exchange'],
        'Вход': ['0.45%', '-0.02%', '0.82%'],
        'Текущий': ['0.32%', '-0.01%', '0.75%'],
        'PnL': ['+$45', '+$12', '+$120'],
        'Статус': ['Открыта', 'Закрыта', 'Открыта'],
        'Время': ['10:30', '11:15', '12:00']
    }

    df = pd.DataFrame(trades_data)

    # Цветовая индикация
    def highlight_status(val):
        if val == 'Открыта':
            return 'background-color: #ccffcc'
        return 'background-color: #ccccff'

    st.dataframe(df.style.applymap(highlight_status, subset=['Статус']), 
                 use_container_width=True)

def render_api_status():
    """Статус API бирж"""
    st.header("🔌 Статус API")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Binance")
        st.markdown("""
        - Статус: 🟢 Онлайн
        - Задержка: 45 мс
        - WebSocket: 🟢 Активен
        - Последний ping: 2 сек назад
        """)

        # График задержки
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=[40, 42, 45, 43, 44, 45],
            mode='lines',
            name='Задержка (мс)'
        ))
        fig.update_layout(height=200, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Bybit")
        st.markdown("""
        - Статус: 🟢 Онлайн
        - Задержка: 62 мс
        - WebSocket: 🟢 Активен
        - Последний ping: 1 сек назад
        """)

    with col3:
        st.subheader("OKX")
        st.markdown("""
        - Статус: 🟡 Задержка
        - Задержка: 120 мс
        - WebSocket: 🟢 Активен
        - Последний ping: 5 сек назад
        """)

def render_settings():
    """Настройки бота"""
    st.header("⚙️ Настройки бота")

    with st.form("bot_settings"):
        st.subheader("Параметры сканирования")

        col1, col2 = st.columns(2)
        with col1:
            scan_interval = st.slider("Интервал сканирования (сек)", 1, 30, 5)
            min_spread = st.number_input("Мин. спред по умолчанию (%)", value=0.1, step=0.05)

        with col2:
            max_positions = st.number_input("Макс. позиций на пользователя", value=5, step=1)
            auto_close = st.checkbox("Авто-закрытие позиций", value=True)

        st.subheader("Уведомления")
        telegram_channel = st.text_input("Telegram канал для алертов", "@arbitrage_alerts")

        submitted = st.form_submit_button("💾 Сохранить настройки")

        if submitted:
            st.success("Настройки сохранены!")

def render_logs():
    """Логи системы"""
    st.header("📋 Логи")

    log_level = st.selectbox("Уровень логов", ["INFO", "WARNING", "ERROR", "DEBUG"])

    # Имитация логов
    logs = """
    2024-01-20 14:30:15 INFO Scanner started
    2024-01-20 14:30:16 INFO Connected to Binance WebSocket
    2024-01-20 14:30:17 INFO Connected to Bybit WebSocket
    2024-01-20 14:30:18 INFO Connected to OKX WebSocket
    2024-01-20 14:31:22 ALERT Spread detected: BTC 0.45% (Binance->Bybit)
    2024-01-20 14:31:45 INFO User @trader1 opened position BTC
    2024-01-20 14:32:10 WARNING OKX latency high: 120ms
    """

    st.text_area("Системные логи", logs, height=300)

    if st.button("🧹 Очистить логи"):
        st.success("Логи очищены")

def main():
    st.title("🤖 Панель управления ботом")

    # Меню
    page = st.sidebar.radio(
        "Раздел",
        ["Пользователи", "Сделки", "API Статус", "Настройки", "Логи"]
    )

    if page == "Пользователи":
        render_user_management()
    elif page == "Сделки":
        render_trade_monitoring()
    elif page == "API Статус":
        render_api_status()
    elif page == "Настройки":
        render_settings()
    elif page == "Логи":
        render_logs()

if __name__ == "__main__":
    main()
