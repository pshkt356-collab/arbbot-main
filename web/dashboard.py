import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings

@st.cache_resource
def get_database():
    """Singleton Database instance"""
    from database.models import Database
    db = Database(settings.db_file)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(db.initialize())
    return db

@st.cache_resource
def get_scanner():
    """Singleton Scanner instance"""
    from services.spread_scanner import SpreadScanner
    scanner = SpreadScanner(
        min_spread=0.2,
        check_interval=settings.scan_interval
    )
    return scanner

if 'db' not in st.session_state:
    st.session_state.db = get_database()

if 'scanner' not in st.session_state:
    st.session_state.scanner = get_scanner()

db = st.session_state.db
scanner = st.session_state.scanner

st.set_page_config(
    page_title="Arbitrage Bot Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Arbitrage Bot Dashboard")

with st.sidebar:
    st.header("⚡ Статус подключений")
    
    for conn_name, status in scanner.stats['connections'].items():
        status_icon = "🟢" if status else "🔴"
        st.write(f"{status_icon} {conn_name.replace('_', ' ').title()}")
    
    if getattr(scanner, '_degraded_mode', False):
        st.error("⚠️ DEGRADED MODE")
    
    st.divider()
    st.write(f"🕒 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
    st.write(f"📊 Спредов: {scanner.stats['spreads_found']}")
    st.write(f"📈 Базисов: {scanner.stats['basis_found']}")

tab1, tab2, tab3 = st.tabs(["📈 Активные спреды", "📋 История сделок", "⚙️ Статус"])

with tab1:
    st.header("Текущие возможности арбитража")
    
    spreads = scanner.get_active_spreads(min_spread=0.1)
    
    if not spreads:
        st.info("🔄 В данный момент спреды не обнаружены")
    else:
        sorted_spreads = sorted(spreads.items(), key=lambda x: x[1].spread_percent, reverse=True)
        
        top_spreads = []
        for key, spread in sorted_spreads[:10]:
            top_spreads.append({
                'Symbol': spread.symbol,
                'Spread %': f"{spread.spread_percent:.2f}%",
                'Buy': f"{spread.buy_exchange} @ ${spread.buy_price:.2f}",
                'Sell': f"{spread.sell_exchange} @ ${spread.sell_price:.2f}",
                'Volume 24h': f"${spread.volume_24h:,.0f}",
                'Type': 'Basis' if 'basis' in key else 'Inter-exchange'
            })
        
        df = pd.DataFrame(top_spreads)
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        if len(sorted_spreads) > 0:
            fig = go.Figure(data=[go.Bar(
                x=[s.symbol for _, s in sorted_spreads[:15]],
                y=[s.spread_percent for _, s in sorted_spreads[:15]],
                marker_color=['red' if s.spread_percent > 1.0 else 'orange' if s.spread_percent > 0.5 else 'green' 
                             for _, s in sorted_spreads[:15]]
            )])
            fig.update_layout(title="Топ спредов", xaxis_title="Символ", yaxis_title="Спред %")
            st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.header("История торговли")
    st.info("Данные загружаются из БД...")

with tab3:
    st.header("Системный статус")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Активных потоков", f"{sum(1 for v in scanner.stats['connections'].values() if v)}/10")
    
    with col2:
        st.metric("Найдено спредов", scanner.stats['spreads_found'])
    
    with col3:
        st.metric("Режим", "DEGRADED" if getattr(scanner, '_degraded_mode', False) else "NORMAL")
    
    st.divider()
    st.subheader("Circuit Breaker статус")
    from services.circuit_breaker import circuit_breaker
    cb_status = circuit_breaker.get_status_summary()
    for ex, status in cb_status.items():
        if status['available']:
            st.success(f"🟢 {ex.upper()}: {status['status']}")
        else:
            st.error(f"🔴 {ex.upper()}: {status['status']}")

st.markdown("---")
st.caption("Dashboard обновляется автоматически при перезагрузке страницы")
