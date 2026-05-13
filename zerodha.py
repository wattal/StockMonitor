import streamlit as st
from kiteconnect import KiteConnect

def generate_login_url(api_key):
    return f"https://kite.zerodha.com/connect/login?api_key={api_key}&v=3"

def get_session(api_key, api_secret, request_token):
    try:
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        return data["access_token"]
    except Exception as e:
        st.error(f"Token exchange failed: {e}")
        return None

def fetch_holdings(api_key, access_token):
    try:
        kite = KiteConnect(api_key=api_key, access_token=access_token)
        return kite.holdings()
    except Exception as e:
        st.error(f"Failed to fetch holdings: {e}")
        return []

scrip_to_ticker = {
    "20MICRONS": "20MICRONS.NS", "AARTIIND": "AARTIIND.NS", "ABB": "ABB.NS",
    "ACC": "ACC.NS", "ADANIENT": "ADANIENT.NS", "ADANIPORTS": "ADANIPORTS.NS",
    "ADANIPOWER": "ADANIPOWER.NS", "ATGL": "ATGL.NS", "ABCAPITAL": "ABCAPITAL.NS",
    "ABFRL": "ABFRL.NS", "AMBUJACEM": "AMBUJACEM.NS", "ANGELONE": "ANGELONE.NS",
    "APOLLOHOSP": "APOLLOHOSP.NS", "APOLLOTYRE": "APOLLOTYRE.NS",
    "ASHOKLEY": "ASHOKLEY.NS", "ASIANPAINT": "ASIANPAINT.NS",
    "AUROPHARMA": "AUROPHARMA.NS", "AXISBANK": "AXISBANK.NS",
    "BAJAJ-AUTO": "BAJAJ-AUTO.NS", "BAJAJFINSV": "BAJAJFINSV.NS",
    "BAJFINANCE": "BAJFINANCE.NS", "BALKRISIND": "BALKRISIND.NS",
    "BANDHANBNK": "BANDHANBNK.NS", "BANKBARODA": "BANKBARODA.NS",
    "BEL": "BEL.NS", "BHARATFORG": "BHARATFORG.NS", "BHARTIARTL": "BHARTIARTL.NS",
    "BHEL": "BHEL.NS", "BIOCON": "BIOCON.NS", "BOSCHLTD": "BOSCHLTD.NS",
    "BPCL": "BPCL.NS", "BRITANNIA": "BRITANNIA.NS", "CIPLA": "CIPLA.NS",
    "COALINDIA": "COALINDIA.NS", "COFORGE": "COFORGE.NS", "COLPAL": "COLPAL.NS",
    "CONCOR": "CONCOR.NS", "CUB": "CUB.NS", "DABUR": "DABUR.NS",
    "DIVISLAB": "DIVISLAB.NS", "DIXON": "DIXON.NS", "DLF": "DLF.NS",
    "EICHERMOT": "EICHERMOT.NS", "ESCORTS": "ESCORTS.NS",
    "FEDERALBNK": "FEDERALBNK.NS", "GAIL": "GAIL.NS", "GLENMARK": "GLENMARK.NS",
    "GMRINFRA": "GMRINFRA.NS", "GODREJCP": "GODREJCP.NS",
    "GODREJPROP": "GODREJPROP.NS", "GRASIM": "GRASIM.NS",
    "HAL": "HAL.NS", "HAVELLS": "HAVELLS.NS", "HCLTECH": "HCLTECH.NS",
    "HDFCAMC": "HDFCAMC.NS", "HDFCBANK": "HDFCBANK.NS", "HDFCLIFE": "HDFCLIFE.NS",
    "HEROMOTOCO": "HEROMOTOCO.NS", "HINDALCO": "HINDALCO.NS",
    "HINDPETRO": "HINDPETRO.NS", "HINDUNILVR": "HINDUNILVR.NS",
    "ICICIBANK": "ICICIBANK.NS", "ICICIGI": "ICICIGI.NS",
    "ICICIPRULI": "ICICIPRULI.NS", "IDEA": "IDEA.NS", "INDIGO": "INDIGO.NS",
    "INDUSINDBK": "INDUSINDBK.NS", "INFY": "INFY.NS", "IOC": "IOC.NS",
    "ITC": "ITC.NS", "JINDALSTEL": "JINDALSTEL.NS", "JSWSTEEL": "JSWSTEEL.NS",
    "JUBLFOOD": "JUBLFOOD.NS", "KOTAKBANK": "KOTAKBANK.NS",
    "LAURUSLABS": "LAURUSLABS.NS", "LICI": "LICI.NS", "LT": "LT.NS",
    "LTIM": "LTIM.NS", "LTTS": "LTTS.NS", "LUPIN": "LUPIN.NS",
    "M&M": "M&M.NS", "M&MFIN": "M&MFIN.NS", "MARICO": "MARICO.NS",
    "MARUTI": "MARUTI.NS", "MCDOWELL-N": "MCDOWELL-N.NS",
    "MCX": "MCX.NS", "MOTHERSON": "MOTHERSON.NS",
    "MPHASIS": "MPHASIS.NS", "MRF": "MRF.NS", "MUTHOOTFIN": "MUTHOOTFIN.NS",
    "NAM-INDIA": "NAM-INDIA.NS", "NATIONALUM": "NATIONALUM.NS",
    "NAUKRI": "NAUKRI.NS", "NAVINFLUOR": "NAVINFLUOR.NS", "NCC": "NCC.NS",
    "NESTLEIND": "NESTLEIND.NS", "NMDC": "NMDC.NS", "NTPC": "NTPC.NS",
    "ONGC": "ONGC.NS", "PAGEIND": "PAGEIND.NS", "PERSISTENT": "PERSISTENT.NS",
    "PETRONET": "PETRONET.NS", "PIDILITIND": "PIDILITIND.NS",
    "PNB": "PNB.NS", "POLYCAB": "POLYCAB.NS", "POWERGRID": "POWERGRID.NS",
    "PIDILITIND": "PIDILITIND.NS", "PVRINOX": "PVRINOX.NS",
    "RBLBANK": "RBLBANK.NS", "RECLTD": "RECLTD.NS", "RELIANCE": "RELIANCE.NS",
    "SBILIFE": "SBILIFE.NS", "SBIN": "SBIN.NS", "SHREECEM": "SHREECEM.NS",
    "SHRIRAMFIN": "SHRIRAMFIN.NS", "SIEMENS": "SIEMENS.NS",
    "SOLARINDS": "SOLARINDS.NS", "SONACOMS": "SONACOMS.NS",
    "SRF": "SRF.NS", "SUNPHARMA": "SUNPHARMA.NS", "SUNTV": "SUNTV.NS",
    "SYNGENE": "SYNGENE.NS", "TATACHEM": "TATACHEM.NS",
    "TATACONSUM": "TATACONSUM.NS", "TATAMOTORS": "TATAMOTORS.NS",
    "TATAPOWER": "TATAPOWER.NS", "TATASTEEL": "TATASTEEL.NS",
    "TCS": "TCS.NS", "TECHM": "TECHM.NS", "TITAN": "TITAN.NS",
    "TORNTPHARM": "TORNTPHARM.NS", "TRENT": "TRENT.NS",
    "TVSMOTOR": "TVSMOTOR.NS", "ULTRACEMCO": "ULTRACEMCO.NS",
    "UPL": "UPL.NS", "VEDL": "VEDL.NS", "VOLTAS": "VOLTAS.NS",
    "WIPRO": "WIPRO.NS", "YESBANK": "YESBANK.NS", "ZOMATO": "ZOMATO.NS",
    "ZEEL": "ZEEL.NS", "ZYDUSLIFE": "ZYDUSLIFE.NS",
    "IPCALAB": "IPCALAB.NS", "AUBANK": "AUBANK.NS",
    "IDFCFIRSTB": "IDFCFIRSTB.NS", "MANAPPURAM": "MANAPPURAM.NS",
    "CHOLAFIN": "CHOLAFIN.NS", "INDUSTOWER": "INDUSTOWER.NS",
    "ADANIGREEN": "ADANIGREEN.NS", "ADANIPORTS": "ADANIPORTS.NS",
    "ABFRL": "ABFRL.NS", "ABCAPITAL": "ABCAPITAL.NS",
    "IRCTC": "IRCTC.NS", "IRFC": "IRFC.NS", "IREDA": "IREDA.NS",
    "JIOFIN": "JIOFIN.NS", "NHPC": "NHPC.NS", "NTPC": "NTPC.NS",
    "PFC": "PFC.NS", "RVNL": "RVNL.NS", "RAILTEL": "RAILTEL.NS",
    "SAIL": "SAIL.NS", "POWERGRID": "POWERGRID.NS",
    "CERA": "CERA.NS",
}

BSE_SCRIP_TO_TICKER = {
    "M&M": "M&M.NS",
}

def map_holding_to_ticker(holdings):
    """Map Kite holdings to our ticker system. Returns list of (ticker, qty, avg_cost)"""
    results = []
    for h in holdings:
        tradingsymbol = h.get("tradingsymbol", "")
        exchange = h.get("exchange", "")
        qty = h.get("quantity", 0)
        avg_price = h.get("average_price", 0)
        
        if qty <= 0: continue
        
        # Try direct lookup
        ticker = scrip_to_ticker.get(tradingsymbol)
        
        if not ticker and exchange == "BSE":
            ticker = f"{tradingsymbol}.BO"
        
        if not ticker and exchange == "NSE":
            ticker = f"{tradingsymbol}.NS"
        
        if ticker:
            results.append((ticker, qty, avg_price))
    
    return results
