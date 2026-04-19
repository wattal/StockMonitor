import streamlit as st
import datetime
import pandas as pd
import yfinance as yf
import numpy as np
import time
import json
import os
import engine as eng
from tickers import MASTER_MAP

# 1. PAGE CONFIG
st.set_page_config(page_title="Market Monitor v1.1.0", layout="wide")

# 2. UI STYLE
st.markdown("""
<style>
    .block-container { max-width: 100% !important; padding: 0.3rem 0.5rem !important; }
    .stDataFrame thead th { padding: 4px 6px !important; font-size: 0.7rem !important; }
    .stDataFrame tbody td { padding: 2px 6px !important; }
    section[data-testid="stSidebar"] button { 
        background-color: #ffffff !important; 
        color: #1e40af !important;
        border: 1px solid #3b82f6 !important;
        border-radius: 4px !important;
        font-weight: 500 !important;
    }
    section[data-testid="stSidebar"] button:hover { 
        background-color: #dbeafe !important;
    }
</style>
""", unsafe_allow_html=True)

# 3. STATE INITIALIZATION
if "market_df" not in st.session_state: st.session_state.market_df = pd.DataFrame()
if "baselines_cache" not in st.session_state: st.session_state.baselines_cache = {}
if "raw_history_cache" not in st.session_state: st.session_state.raw_history_cache = None
if "watchlist" not in st.session_state: st.session_state.watchlist = eng.load_watchlist()
if "persist_ref_date" not in st.session_state: st.session_state.persist_ref_date = datetime.date(2023, 12, 31)
if "persist_search" not in st.session_state: st.session_state.persist_search = ""
if "persist_trend" not in st.session_state: st.session_state.persist_trend = "All"
if "persist_favs" not in st.session_state: st.session_state.persist_favs = False
if "persist_rsi" not in st.session_state: st.session_state.persist_rsi = "All"
if "trend_view" not in st.session_state: st.session_state.trend_view = False

def load_portfolio():
    """Load portfolio from file."""
    if not os.path.exists("portfolio.txt"):
        return {}
    try:
        with open("portfolio.txt", "r") as f:
            return json.loads(f.read())
    except: return {}

def save_portfolio():
    """Save portfolio to file."""
    with open("portfolio.txt", "w") as f:
        f.write(json.dumps(st.session_state.portfolio))

if "portfolio" not in st.session_state: st.session_state.portfolio = load_portfolio()

def handle_star_update():
    ticker = st.session_state.star_selector
    if ticker:
        is_adding = ticker not in st.session_state.watchlist
        eng.save_to_watchlist(ticker, add=is_adding)
        st.session_state.watchlist = eng.load_watchlist()

def handle_portfolio_add():
    ticker = st.session_state.port_ticker
    try:
        count = int(st.session_state.port_count) if st.session_state.port_count else 0
        avg_cost = float(st.session_state.port_avg_cost) if st.session_state.port_avg_cost else 0
    except: count, avg_cost = 0, 0
    if ticker and count > 0 and avg_cost > 0:
        st.session_state.portfolio[ticker] = {"count": count, "avg_cost": avg_cost}
        save_portfolio()

def handle_portfolio_remove(ticker):
    if ticker in st.session_state.portfolio:
        del st.session_state.portfolio[ticker]
        save_portfolio()

TOTAL_MASTER_COUNT = len(MASTER_MAP)
MASTER_TICKERS = list(MASTER_MAP.keys())

def is_market_open():
    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    if now.weekday() >= 5: return False
    return datetime.time(9, 15) <= now.time() <= datetime.time(15, 30)

def get_index_data(ticker, name):
    try:
        df = yf.download(ticker, period="2d", progress=False)
        if df is not None and not df.empty:
            c = float(df["Close"].iloc[-1])
            p = float(df["Close"].iloc[-2])
            chg = ((c-p)/p)*100
            return name, f"{c:,.0f}", f"{chg:+.2f}%"
    except: pass
    return name, "---", "N/A"

def do_full_refresh():
    container = st.empty()
    start_time = time.time()
    with container.container():
        clock, status = st.empty(), st.empty()
        try:
            status.markdown('<p class="phase-active">⏳ Phase 1: History...</p>', unsafe_allow_html=True)
            raw = st.cache_data(eng.download_bulk_history)(MASTER_TICKERS)
            t1 = time.time() - start_time
            status.markdown(f'<p class="phase-done">✅ Phase 1 ({t1:.1f}s)</p><p class="phase-active">⏳ Phase 2: Analysis...</p>', unsafe_allow_html=True)
            clock.markdown(f'<div class="load-timer">{t1:.1f}s</div>', unsafe_allow_html=True)
            
            base = eng.calculate_baselines(MASTER_TICKERS, raw, st.session_state.persist_ref_date)
            st.session_state.baselines_cache = base
            st.session_state.raw_history_cache = raw
            t2 = time.time() - start_time
            status.markdown(f'<p class="phase-done">✅ Ph 1 & 2 ({t2:.1f}s)</p><p class="phase-active">⏳ Phase 3: Prices...</p>', unsafe_allow_html=True)
            clock.markdown(f'<div class="load-timer">{t2:.1f}s</div>', unsafe_allow_html=True)
            
            df, _, _ = eng.get_live_data(MASTER_TICKERS, base, set())
            st.session_state.market_df = df
            st.session_state.load_time = time.time() - start_time
            container.empty()
            st.rerun()
        except Exception as e: st.error(f"Engine Error: {e}")

def do_quick_refresh():
    if not st.session_state.baselines_cache:
        do_full_refresh()
        return
    start_time = time.time()
    try:
        df = eng.quick_refresh_prices(MASTER_TICKERS, st.session_state.baselines_cache)
        st.session_state.market_df = df
        st.session_state.load_time = time.time() - start_time
        st.rerun()
    except Exception as e: st.error(f"Quick Refresh Error: {e}")

def clear_all_caches():
    for f in ["history_cache.pkl", "daily_prices_15d.json"]:
        try: os.remove(f)
        except: pass
    st.session_state.market_df = pd.DataFrame()
    st.session_state.baselines_cache = {}
    st.session_state.raw_history_cache = None
    st.success("Caches cleared! Click Reset to reload.")
    st.rerun()

# 4. SIDEBAR RENDERING (Dense Layout)
def section_header(title):
    st.markdown(f"<p style='color:#1e40af; font-weight:600; margin:0.2rem 0 0.3rem 0;'>{title}</p>", unsafe_allow_html=True)

with st.sidebar:
    # --- MARKET STATUS ---
    section_header("Market Overview")
    
    m_status = "Open" if is_market_open() else "Closed"
    n_name, n_val, n_chg = get_index_data("^NSEI", "NIFTY")
    s_name, s_val, s_chg = get_index_data("^BSESN", "SENSEX")
    active_count = len(st.session_state.market_df) if not st.session_state.market_df.empty else 0
    
    m_col1, m_col2 = st.columns(2)
    m_col1.metric("Nifty 50", n_val, n_chg)
    m_col2.metric("Sensex", s_val, s_chg)
    
    status_color = "#16a34a" if m_status == "Open" else "#dc2626"
    st.markdown(f"""
    <div style="display: flex; justify-content: space-between; padding: 0 0.5rem;">
        <span>Status: <b style="color: {status_color};">{m_status}</b></span>
        <span>Stocks: <b>{active_count}/{TOTAL_MASTER_COUNT}</b></span>
    </div>
    """, unsafe_allow_html=True)
    if st.session_state.get("load_time"):
        st.caption(f"⏱ Loaded in {st.session_state.load_time:.1f}s")
    st.markdown("<div style='margin-top: 1.5rem;'></div><hr style='margin: 0;'>", unsafe_allow_html=True)

    # --- QUICK FILTERS ---
    section_header("Filters")
    st.text_input("Search", placeholder="Type to search...", key="persist_search")
    st.selectbox("Trend", ["All", "Green", "Red"], key="persist_trend")
    st.checkbox("Trend View", key="trend_view")
    
    st.write("**RSI Filter:**")
    rsi_col1, rsi_col2, rsi_col3 = st.columns(3)
    if rsi_col1.button("OB", use_container_width=True):
        st.session_state.persist_rsi = "Overbought"
        st.rerun()
    if rsi_col2.button("OS", use_container_width=True):
        st.session_state.persist_rsi = "Oversold"
        st.rerun()
    if rsi_col3.button("Clear", use_container_width=True):
        st.session_state.persist_rsi = "All"
        st.rerun()
    st.markdown("<div style='margin-top: 1.5rem;'></div><hr style='margin: 0;'>", unsafe_allow_html=True)

    # --- WATCHLIST ---
    section_header("Watchlist")
    st.checkbox("Starred Stocks", value=st.session_state.get("persist_favs", False), key="persist_favs")
    
    st.selectbox("Add to watchlist", options=[""] + sorted(MASTER_TICKERS), key="star_selector", label_visibility="collapsed")
    if st.button("Add to Watchlist", use_container_width=True):
        handle_star_update()
        st.rerun()
    st.markdown("<div style='margin-top: 1.5rem;'></div><hr style='margin: 0;'>", unsafe_allow_html=True)
    
    # --- PORTFOLIO ---
    section_header("Portfolio")
    port_ticker = st.selectbox("Stock", options=[""] + sorted(MASTER_TICKERS), key="port_ticker", label_visibility="collapsed")
    col1, col2 = st.columns(2)
    col1.text_input("Qty", key="port_count", label_visibility="collapsed")
    col2.text_input("Avg Cost", key="port_avg_cost", label_visibility="collapsed")
    if st.button("Add to Portfolio", use_container_width=True):
        handle_portfolio_add()
        st.rerun()
    st.markdown("<div style='margin-top: 1.5rem;'></div><hr style='margin: 0;'>", unsafe_allow_html=True)
    
    # --- CONTROLS ---
    section_header("Refresh")
    c1, c2 = st.columns(2)
    if c1.button("Qik F5", use_container_width=True):
        do_quick_refresh()
    if c2.button("Reset", use_container_width=True):
        do_full_refresh()
    if st.button("Clear Cache", use_container_width=True):
        clear_all_caches()
    
    # --- EXPORT ---
    section_header("Export")
    if not st.session_state.market_df.empty:
        export_df = st.session_state.market_df.copy()
        export_df["Star"] = export_df["TickerID"].apply(lambda x: "⭐" if x in st.session_state.watchlist else "")
        order = ["Star", "Name", "Sector", "LTP", "Change%", "MCap ($)", "PE", "RSI(14)"]
        final_cols = [c for c in order if c in export_df.columns]
        csv = export_df[final_cols].to_csv(index=False).encode('utf-8')
        st.download_button("Download CSV", csv, "market_data.csv", "text/csv", use_container_width=True)
    st.markdown("<div style='margin-top: 1.5rem;'></div><hr style='margin: 0;'>", unsafe_allow_html=True)
    
    # --- TIMESTAMP AT BOTTOM ---
    now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    st.text(f"Last updated: {now_ist.strftime('%H:%M:%S')}")
    
    # --- MY HOLDINGS (Collapsible) ---
    if st.session_state.portfolio:
        with st.expander("My Holdings", expanded=False):
            for t, v in list(st.session_state.portfolio.items()):
                name = MASTER_MAP.get(t, {}).get("Name", t)
                col1, col2 = st.columns([4, 1])
                col1.markdown(f"**{name}**: {v['count']} × ₹{v['avg_cost']:.1f} = ₹{v['count']*v['avg_cost']:.0f}")
                if col2.button("×", key=f"rem_{t}"):
                    handle_portfolio_remove(t)
                    st.rerun()

# 5. DATA FETCH ENGINE
if st.session_state.market_df.empty:
    do_full_refresh()

# 6. GLOBAL FILTERING LOGIC
if not st.session_state.market_df.empty:
    active = st.session_state.market_df.copy()
    active["Star"] = active["TickerID"].apply(lambda x: "⭐" if x in st.session_state.watchlist else "")

    q = st.session_state.persist_search.lower().strip()
    if q:
        active = active[
            active["Name"].str.lower().str.contains(q, na=False) | 
            active["TickerID"].str.lower().str.contains(q, na=False) | 
            active["Sector"].str.lower().str.contains(q, na=False)
        ]

    if st.session_state.persist_favs: active = active[active["Star"] == "⭐"]
    if st.session_state.persist_trend == "Green": active = active[active["Change%"] > 0]
    elif st.session_state.persist_trend == "Red": active = active[active["Change%"] < 0]
    
    if st.session_state.persist_rsi == "Overbought": active = active[active["RSI(14)"] >= 70]
    elif st.session_state.persist_rsi == "Oversold": active = active[active["RSI(14)"] <= 30]

    # Lazy load daily changes only when Trend View is enabled
    if st.session_state.get("trend_view", False):
        daily_prices = eng.get_daily_prices(list(active["TickerID"]), days=15)
        for i, row in active.iterrows():
            ticker = row["TickerID"]
            closes = daily_prices.get(ticker, [])
            if len(closes) >= 11:
                for days_ago in range(2, 11):
                    curr_idx = -days_ago
                    prev_idx = -(days_ago + 1)
                    curr_c = closes[curr_idx]
                    prev_c = closes[prev_idx]
                    if prev_c > 0:
                        active.at[i, f"{days_ago}D Chg"] = ((curr_c - prev_c) / prev_c) * 100

    # Portfolio columns
    def get_port_count(ticker):
        if ticker in st.session_state.portfolio:
            return st.session_state.portfolio[ticker]["count"]
        return 0
    
    def get_port_total(ticker):
        if ticker in st.session_state.portfolio:
            return st.session_state.portfolio[ticker]["count"] * st.session_state.portfolio[ticker]["avg_cost"]
        return 0
    
    active["Port Count"] = active["TickerID"].apply(get_port_count)
    active["Port Total"] = active["TickerID"].apply(get_port_total)
    
    # Link column - Yahoo Finance links
    def get_link(ticker):
        suffix = ".NS" if ".NS" in ticker else ".BO"
        symbol = ticker.replace(".NS", "").replace(".BO", "")
        return f"https://finance.yahoo.com/quote/{symbol}{suffix}"
    active["Link"] = active["TickerID"].apply(get_link)

    # 1Y Status - Color coded (Green=High, Red=Low)
    def get_1y_color(row):
        if pd.isna(row.get("vs 1Y H %")): return ""
        pct_from_high = row["vs 1Y H %"]  # How far from 1Y high (0 = at high, -50 = 50% below)
        pct_from_low = row.get("vs 1Y L %", 50)  # How far from 1Y low (0 = at low, 50 = 50% above)
        
        # Calculate a score: higher is closer to high
        score = 100 + pct_from_high  # 100 means at high, 50 means 50% below high
        
        if score >= 98: return "🟢"      # At/Near High - Green
        if score >= 93: return "🔵"      # Very Close - Blue
        if score >= 80: return "🟡"      # Above Mid - Yellow  
        if score >= 60: return "🟠"      # Below Mid - Orange
        return "🔴"                      # Near Low - Red
    active["1Y"] = active.apply(get_1y_color, axis=1)
    
    active.insert(1, "#", range(1, len(active) + 1))

    active = active.sort_values(by="Name", ignore_index=True)

    # 7. COLUMN DEFINITIONS & STYLING (Compact)
    if st.session_state.get("trend_view", False):
        order = [
            "Star", "#", "1Y", "Link", "Name", "Sector", "LTP", "Change%",
            "2D Chg", "3D Chg", "4D Chg", "5D Chg", "6D Chg",
            "7D Chg", "8D Chg", "9D Chg", "10D Chg",
            "vs 15D H %", "vs 30D H %", "vs 3M H %", "vs 6M H %", "vs 1Y H %"
        ]
    else:
        order = [
            "Star", "#", "1Y", "Link", "Name", "Sector", "LTP", "Change%", "Port Count", "Port Total",
            "vs 7D H %", "vs 15D H %", "vs 30D H %", "vs 3M H %", "vs 1Y H %", "vs 1Y L %",
            "vs 100DMA %", "RSI(14)", "Vol Breakout", "MCap ($)", "PE", "EPS"
        ]
    
    final_cols = [c for c in order if c in active.columns]
    pct_cols = [c for c in final_cols if "%" in c or "Chg" in c]
    tech_fmt_active = [c for c in final_cols if c in ["Vol Breakout", "RSI(14)", "PE", "EPS", "MCap ($)", "Port Count", "Port Total"]]

    def color_pct(val):
        if not isinstance(val, (int, float)) or pd.isna(val): return ""
        return f"color: {'#16a34a' if val > 0 else '#dc2626'};"

    def color_rsi(val):
        if not isinstance(val, (int, float)) or pd.isna(val): return ""
        if val >= 70: return "color: #ef4444; background: #fee2e2;"
        if val <= 30: return "color: #10b981; background: #dcfce7;"
        return "color: #64748b;"
    
    def color_vol(val):
        if not isinstance(val, (int, float)) or pd.isna(val): return ""
        if val >= 2.0: return "background: #fef3c7;"
        if val >= 1.5: return "background: #dbeafe;"
        return ""
    
    def color_1y(val):
        if val == "🟢": return "background: #dcfce7; color: #15803d;"  # Green bg
        if val == "🔵": return "background: #dbeafe; color: #1d4ed8;"  # Blue bg
        if val == "🟡": return "background: #fef9c3; color: #a16207;"  # Yellow bg
        if val == "🟠": return "background: #ffedd5; color: #c2410c;"  # Orange bg
        if val == "🔴": return "background: #fee2e2; color: #dc2626;"  # Red bg
        return ""
    
    def color_port(val):
        if not isinstance(val, (int, float)) or pd.isna(val) or val == 0: return ""
        return "background: #dcfce7; color: #15803d;"
    
    styled_df = (active[final_cols].style
        .map(color_pct, subset=pct_cols)
        .map(color_rsi, subset=["RSI(14)"] if "RSI(14)" in final_cols else [])
        .map(color_vol, subset=["Vol Breakout"] if "Vol Breakout" in final_cols else [])
        .map(color_1y, subset=["1Y"])
        .map(color_port, subset=["Port Count", "Port Total"] if "Port Count" in final_cols else [])
        .format(precision=1, subset=pct_cols + tech_fmt_active))

    # Stock count and export in one row
    total = len(st.session_state.market_df)
    shown = len(active)
    filter_active = ""
    if st.session_state.get("persist_rsi") == "Overbought": filter_active = "RSI >70"
    elif st.session_state.get("persist_rsi") == "Oversold": filter_active = "RSI <30"
    elif st.session_state.get("persist_favs"): filter_active = "Favorites"
    elif st.session_state.get("persist_trend") != "All": filter_active = st.session_state.get("persist_trend")
    
    st.markdown(f"**{shown}** of **{total}** stocks" + (f" · {filter_active}" if filter_active else ""))
    
    st.dataframe(styled_df, width='stretch', hide_index=True, height=2000,
        column_config={
            "Star": st.column_config.TextColumn("⭐", width=30),
            "#": st.column_config.NumberColumn("#", width=30),
            "1Y": st.column_config.TextColumn("1Y", width=35),
            "Link": st.column_config.LinkColumn("🔗", width=40, display_text="🔗"),
            "Name": st.column_config.TextColumn("Name", width=130),
            "Sector": st.column_config.TextColumn("Sector", width=70),
            "LTP": st.column_config.NumberColumn("LTP", format="₹%.0f", width=65),
            "Change%": st.column_config.NumberColumn("Chg%", format="%.1f%%", width=55),
            "Port Count": st.column_config.NumberColumn("Count", format="%d", width=50),
            "Port Total": st.column_config.NumberColumn("Total", format="₹%.0f", width=70),
            "RSI(14)": st.column_config.NumberColumn("RSI", format="%.0f", width=45),
            "Vol Breakout": st.column_config.NumberColumn("Vol", format="%.1f", width=45),
            "MCap ($)": st.column_config.NumberColumn("MCap", format="%.0f", width=70),
            "PE": st.column_config.NumberColumn("PE", format="%.1f", width=50),
            "EPS": st.column_config.NumberColumn("EPS", format="%.1f", width=50),
            **{c: st.column_config.NumberColumn(c.replace("vs ", "").replace(" %", ""), format="%.1f%%", width=50) for c in pct_cols if c not in ["Change%", "RSI(14)", "Vol Breakout"]}
        })

    # 8. BACKGROUND SNAPSHOT SYNC
    if "MCap ($)" in active.columns and active["MCap ($)"].isnull().all():
        with st.status("Syncing Snapshot...", expanded=False):
            usd_rate = eng.get_usd_rate()
            f_map = eng.fetch_fundamentals_map(MASTER_TICKERS, usd_rate)
            st.session_state.fundamentals_time = datetime.datetime.now().strftime("%H:%M")
            for t, v in f_map.items():
                for col, val in v.items(): 
                    st.session_state.market_df.loc[st.session_state.market_df['TickerID'] == t, col] = val
            st.rerun()