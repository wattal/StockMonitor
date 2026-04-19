import yfinance as yf
import pandas as pd
import numpy as np
import time
import streamlit as st
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

HISTORY_CACHE_FILE = "history_cache.pkl"
CACHE_HOURS = 24  # Refresh history once per day

def get_file_age_hours(filepath):
    """Check how old a file is in hours."""
    if not os.path.exists(filepath):
        return 999
    file_time = os.path.getmtime(filepath)
    age = (time.time() - file_time) / 3600
    return age

# --- BLOCK E1: TECHNICAL CALCULATIONS ---
def calculate_rsi(series, period=14):
    """Retained: Calculates Relative Strength Index."""
    if series is None or len(series) < period:
        return 50
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

@st.cache_data(ttl=3600)
def get_usd_rate():
    """Retained: Fetches live USD to INR exchange rate."""
    try:
        df = yf.download("USDINR=X", period="5d", progress=False)
        return float(df["Close"].iloc[-1])
    except:
        return 84.5

def get_daily_prices(tickers, days=15):
    """Get daily closing prices with caching (refreshes once per day)."""
    import json
    cache_file = f"daily_prices_{days}d.json"
    
    if get_file_age_hours(cache_file) < CACHE_HOURS:
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except: pass
    
    data = yf.download(tickers, period=f"{days}d", group_by="ticker", progress=False, threads=True)
    prices = {}
    for t in tickers:
        try:
            df_t = data[t] if len(tickers) > 1 else data
            closes = df_t["Close"].dropna().tolist()
            prices[t] = closes
        except: pass
    
    try:
        with open(cache_file, 'w') as f:
            json.dump(prices, f)
    except: pass
    
    return prices

# --- BLOCK E2: DATA FETCHING ---
def download_bulk_history(tickers):
    """Downloads historical data with daily file caching."""
    if get_file_age_hours(HISTORY_CACHE_FILE) < CACHE_HOURS:
        try:
            import pickle
            with open(HISTORY_CACHE_FILE, 'rb') as f:
                return pickle.load(f)
        except: pass
    
    cleaned = [t.upper().strip() + (".NS" if not (t.endswith(".NS") or t.endswith(".BO")) else "") for t in tickers]
    data = yf.download(list(set(cleaned)), period="2y", group_by="ticker", progress=False, threads=True)
    
    try:
        import pickle
        with open(HISTORY_CACHE_FILE, 'wb') as f:
            pickle.dump(data, f)
    except: pass
    
    return data

def _calc_baseline_single(ticker, raw_data, cut):
    """Helper function to calculate baseline for a single ticker."""
    try:
        df = raw_data[ticker].dropna(subset=["Close"]).copy()
        if df.empty: return None
        
        windows = {
            "2D": df.iloc[-3:-2], "3D": df.iloc[-4:-3], "4D": df.iloc[-5:-4], 
            "5D": df.iloc[-6:-5], "6D": df.iloc[-7:-6], "7D": df.iloc[-5:], 
            "8D": df.iloc[-9:-8], "9D": df.iloc[-10:-9], "10D": df.iloc[-11:-10],
            "15D": df.iloc[-10:], "30D": df.iloc[-21:], 
            "3M": df.iloc[-63:], "6M": df.iloc[-126:], "1Y": df.iloc[-252:]
        }

        rl = np.nan
        if cut:
            since_df = df[df.index.tz_localize(None) >= cut]
            if not since_df.empty: rl = float(since_df["Low"].min())

        res = {
            "MA100": float(df["Close"].iloc[-100:].mean()),
            "RSI": calculate_rsi(df["Close"]).iloc[-1],
            "AvgVol": float(df["Volume"].iloc[-10:].mean()), 
            "RefLow": rl
        }
        
        for label, window_df in windows.items():
            res[f"{label}H"] = float(window_df["High"].max())
            res[f"{label}L"] = float(window_df["Low"].min())

        return ticker, res
    except: return None

def calculate_baselines(tickers, raw_data, ref_date=None):
    """Enhanced: Retains all original metrics while adding 30D, 3M, and 1Y High/Lows."""
    baselines = {}
    cut = pd.to_datetime(ref_date).tz_localize(None) if ref_date else None
    
    # Use parallel processing
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_calc_baseline_single, t, raw_data, cut): t for t in tickers}
        for future in as_completed(futures):
            result = future.result()
            if result:
                ticker, res = result
                baselines[ticker] = res
    
    return baselines

def get_live_data(tickers, baselines, dormant_set, mode="desktop"):
    """Enhanced: Maps calculated baselines to the exact column set you requested."""
    from tickers import MASTER_MAP
    active = [t for t in tickers if t not in dormant_set]
    rows = []
    
    # Get live prices
    data = yf.download(active, period="5d", group_by="ticker", progress=False, threads=True)
    
    for t in active:
        try:
            df_t = data[t] if len(active) > 1 else data
            v = df_t.dropna(subset=["Close"]).copy()
            p = float(v["Close"].iloc[-1])
            prev = float(v["Close"].iloc[-2]) if len(v) > 1 else p
            b = baselines.get(t, {})
            
            pct = lambda val, base: (((val - base) / base) * 100 if base and not pd.isna(base) else np.nan)
            
            rows.append({
                "Name": MASTER_MAP[t]["Name"], "Sector": MASTER_MAP[t]["Sector"], "LTP": p,
                "Change%": ((p - prev) / prev) * 100,
                "vs 15D H %": pct(p, b.get("15DH")), "vs 30D H %": pct(p, b.get("30DH")), 
                "vs 3M H %": pct(p, b.get("3MH")), "vs 6M H %": pct(p, b.get("6MH")),
                "vs 1Y H %": pct(p, b.get("1YH")), "vs 1Y L %": pct(p, b.get("1YL")),
                "H/L since %": pct(p, b.get("RefLow")), 
                "RSI(14)": b.get("RSI", 50),
                "Vol Breakout": float(v["Volume"].iloc[-1]) / b.get("AvgVol", 1) if b.get("AvgVol", 1) > 0 else 1.0,
                "vs 100DMA %": pct(p, b.get("MA100")),
                "TickerID": t, "MCap ($)": np.nan, "PE": np.nan, "PB": np.nan, "EPS": np.nan
            })
        except: continue
    return pd.DataFrame(rows), 0, []

@st.cache_data(ttl=86400)
def fetch_fundamentals_map(tickers, usd_rate):
    """Retained: Fetches MCAP, PE, PB, and EPS."""
    results = {}
    for t in tickers:
        try:
            info = yf.Ticker(t).info
            mcap = info.get("marketCap", np.nan)
            curr = info.get("currency", "INR")
            if not pd.isna(mcap):
                mcap = (mcap / usd_rate / 1_000_000) if curr == "INR" else (mcap / 1_000_000)
            results[t] = {
                "MCap ($)": round(mcap, 2),
                "PE": info.get("trailingPE", np.nan),
                "PB": info.get("priceToBook", np.nan),
                "EPS": info.get("trailingEps", np.nan)
            }
        except: continue
    return results

def quick_refresh_prices(tickers, baselines):
    """Fast refresh - only fetches LTP, uses cached baselines for calculations."""
    from tickers import MASTER_MAP
    rows = []
    
    data = yf.download(tickers, period="2d", group_by="ticker", progress=False, threads=True)
    
    for t in tickers:
        try:
            df_t = data[t] if len(tickers) > 1 else data
            v = df_t.dropna(subset=["Close"]).copy()
            if v.empty: continue
            p = float(v["Close"].iloc[-1])
            prev = float(v["Close"].iloc[-2]) if len(v) > 1 else p
            b = baselines.get(t, {})
            pct = lambda val, base: (((val - base) / base) * 100 if base and not pd.isna(base) else np.nan)
            
            rows.append({
                "Name": MASTER_MAP[t]["Name"], "Sector": MASTER_MAP[t]["Sector"], "LTP": p,
                "Change%": ((p - prev) / prev) * 100,
                "vs 15D H %": pct(p, b.get("15DH")), "vs 30D H %": pct(p, b.get("30DH")), 
                "vs 3M H %": pct(p, b.get("3MH")), "vs 6M H %": pct(p, b.get("6MH")),
                "vs 1Y H %": pct(p, b.get("1YH")), "vs 1Y L %": pct(p, b.get("1YL")),
                "H/L since %": pct(p, b.get("RefLow")), 
                "RSI(14)": b.get("RSI", 50),
                "Vol Breakout": float(v["Volume"].iloc[-1]) / b.get("AvgVol", 1) if b.get("AvgVol", 1) > 0 else 1.0,
                "vs 100DMA %": pct(p, b.get("MA100")),
                "TickerID": t, "MCap ($)": np.nan, "PE": np.nan, "PB": np.nan, "EPS": np.nan
            })
        except: continue
    return pd.DataFrame(rows)

# --- BLOCK E3: WATCHLIST MANAGEMENT ---
def load_watchlist():
    """Retained: Reads saved tickers from local file."""
    if not os.path.exists("watchlist.txt"):
        return []
    with open("watchlist.txt", "r") as f:
        return [line.strip() for line in f.readlines() if line.strip()]

def save_to_watchlist(ticker, add=True):
    """Retained: Saves or removes tickers from local file."""
    current = set(load_watchlist())
    if add: current.add(ticker)
    else: current.discard(ticker)
    with open("watchlist.txt", "w") as f:
        for t in sorted(current):
            f.write(f"{t}\n")