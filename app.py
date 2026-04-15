#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Stage 2 Breakout Screener — Nifty Microcap 250           ║
║   FIXED: Bulk yfinance, NSE fallback, safe auto-refresh    ║
╚══════════════════════════════════════════════════════════════╝
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import time
from datetime import datetime
import warnings

# Safe auto-refresh (actually works in Streamlit)
from streamlit_autorefresh import st_autorefresh

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
INDEX_NAME = "NIFTY MICROCAP 250"
REFRESH_SEC = 300          # auto-refresh interval (5 min)
HISTORY_PERIOD = "3mo"     # enough for 50-DMA + RSI warm-up

DMA_FAST = 20
DMA_SLOW = 50
VOL_AVG_PERIOD = 10
VOL_RATIO_THRESHOLD = 1.5  # 150 %
RSI_PERIOD = 14
RSI_LO, RSI_HI = 50, 70
MIN_VOLUME = 100_000       # 1 lakh shares

# ──────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Stage 2 Breakout Screener | Microcap 250",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    .card { padding: 1.25rem 1rem; border-radius: 0.85rem; color: #fff; text-align: center; box-shadow: 0 4px 14px rgba(0,0,0,.12); }
    .card .num { font-size: 2.1rem; font-weight: 800; line-height: 1.1; }
    .card .lbl { font-size: 0.82rem; opacity: 0.9; margin-top: 2px; }
    .card-purple { background: linear-gradient(135deg,#667eea,#764ba2); }
    .card-teal { background: linear-gradient(135deg,#11998e,#38ef7d); }
    .card-rose { background: linear-gradient(135deg,#f093fb,#f5576c); }
    .card-slate { background: linear-gradient(135deg,#475569,#1e293b); }
    .criteria { background: #f8fafc; border-left: 4px solid #667eea; padding: 1rem 1.25rem; border-radius: 0 0.6rem 0.6rem 0; font-size: 0.92rem; line-height: 1.7; }
    .hero-title { font-size: 2rem; font-weight: 800; background: linear-gradient(90deg,#667eea,#f5576c); -webkit-background-clip: text; -webkit-text-fill-color: transparent; text-align: center; }
    .hero-sub { text-align: center; color: #64748b; font-size: 0.95rem; margin-top: -4px; }
    .sb-head { font-weight: 700; font-size: 0.95rem; margin-bottom: 0.4rem; }
</style>
""", unsafe_allow_html=True
)

# ══════════════════════════════════════════════
# DATA LAYER
# ══════════════════════════════════════════════

# FALLBACK: If NSE blocks the request, we use a hardcoded recent snapshot
_FALLBACK_CONSTITUENTS = [
	"ASKAUTOLTD", "AXISCADES", "AARTIDRUGS", "AARTIPHARM", "AVL", "ADVENZYMES", "AEQUS", "AETHER", "AHLUCONT",
	"AKUMS", "APLLTD", "ALIVUS", "ALKYLAMINE", "ALOKINDS", "APOLLO", "ACI", "ARVINDFASN", "ARVIND", "ASHAPURMIN", 
	"ASHOKA", "ASTRAMICRO", "ATLANTAELE", "AURIONPRO", "AVALON", "AVANTIFEED", "CCAVENUE", "AWFIS", "AZAD", 
	"BAJAJELEC", "BALAMINES", "BALUFORGE", "BANCOINDIA", "BIRLACORPN", "BBOX", "BLACKBUCK", "BLUESTONE", "BORORENEW", 
	"CIEINDIA", "CMSINFO", "CORONA", "CSBBANK", "CAMPUS", "CRAMC", "CAPILLARY", "CELLO", "CENTURYPLY", "CERA", "CIGNITITEC", 
	"CRIZAC", "CUPID", "DCBBANK", "DATAMATICS", "DIACABS", "DBL", "AGARWALEYE", "DYNAMATECH", "EPL", "EDELWEISS", "EMIL", 
	"ELECTCAST", "ELLEN", "EMBDL", "ENTERO", "EIEL", "EQUITASBNK", "ETHOSLTD", "EUREKAFORB", "FEDFINA", "FIEMIND", "FINPIPE", 
	"UTLSOLAR", "GHCL", "GMMPFAUDLR", "GMRP&UI", "GRWRHITECH", "GODREJAGRO", "GOKEX", "GOKULAGRO", "GREAVESCOT", "GAEL", 
	"GNFC", "GPPL", "GSFC", "HGINFRA", "HAPPSTMNDS", "HCG", "HEMIPROP", "HERITGFOOD", "HCC", "IFBIND", "IIFLCAPS", "INOXINDIA", 
	"INDIAGLYCO", "INDIASHLTR", "IMFA", "INDIGOPNTS", "ICIL", "INOXGREEN", "IONEXCHANG", "JKLAKSHMI", "JKPAPER", "JAIBALAJI", 
	"JAMNAAUTO", "JSFB", "JAYNECOIND", "JSLL", "JLHL", "JUSTDIAL", "JYOTHYLAB", "KNRCON", "KPIGREEN", "KRBL", "KRN", "KSB", 
	"KANSAINER", "KTKBANK", "KSCL", "KIRLOSBROS", "KIRLPNU", "KITEX", "LXCHEM", "IXIGO", "LLOYDSENGG", "LLOYDSENT", 
	"LUMAXTECH", "MOIL", "MSTCLTD", "MTARTECH", "MAHSCOOTER", "MAHSEAMLES", "MANORAMA", "MARKSANS", "MASTEK", "MEDPLUS", 
	"METROPOLIS", "MIDHANI", "BECTORFOOD", "NEOGEN", "NESCO", "NFL", "NAZARA", "NETWORK18", "OPTIEMUS", "ORIENTCEM", 
	"ORKLAINDIA", "OSWALPUMPS", "PNGJL", "PCJEWELLER", "PNCINFRA", "PTC", "PARAS", "PARKHOSPS", "PGIL" "PICCADIL", 
	"POWERMECH", "PRAJIND", "PRICOLLTD", "PFOCUS", "PRSMJOHNSN", "PRIVISCL", "PRUDENT", "PURVA", "QPOWER", "QUESS", 
	"RAIN", "RALLIS", "RCF", "RATEGAIN", "RTNINDIA", "RTNPOWER", "RAYMONDLSL", "REDTAPE", "REFEX", "RELAXO", "RELIGARE", 
	"RBA", "ROUTE", "RUBICON", "SKFINDUS", "SKFINDIA", "SKYGOLD", "SMLMAH", "SAATVIKGL", "SAFARI", "SAMHI", "SANDUMA", 
	"SANSERA", "SENCO", "STYL", "SHAILY", "SHAKTIPUMP", "SHARDACROP", "SHAREINDIA", "SFL", "SHILPAMED", "RENUKA", 
	"SHRIPISTON", "SKIPPER", "SMARTWORKS", "SOUTHBANK", "LOTUSDEV", "STARCEMENT", "SWSOLAR", "STLTECH", "STAR", 
	"STYRENIX", "SUBROS", "SUDARSCHEM", "SUDEEPPHRM", "SPARC", "SUNTECK", "SUPRIYA", "SURYAROSNI", "TARC", "TDPOWERSYS", 
	"TSFINV", "TVSSCS", "TMB", "TANLA", "TEXRAIL", "THANGAMAYL", "ANUP", "THOMASCOOK", "THYROCARE", "TI", "TIMETECHNO", 
	"TIPSMUSIC", "TRANSRAILL", "TRIVENI", "UJJIVANSFB", "VGUARD", "VMART", "VIPIND", "V2RETAIL", "WABAG", "VAIBHAVGBL", 
	"DBREALTY", "VARROC", "MANYAVAR", "VIKRAMSOLR", "VIYASH", "VOLTAMP", "WAAREERTL", "WAKEFIT", "WEWORK", "WEBELSOLAR", 
	"WELENT", "WESTLIFE", "YATHARTH", "ZAGGLE"
]

@st.cache_data(ttl=86_400, show_spinner=False)
def get_index_constituents() -> list[str]:
    """Fetch from NSE, fallback to hardcoded list if blocked."""
    url = f"https://indices.nseindia.com/api/equity-stockIndices?index={INDEX_NAME.replace(' ', '%20')}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
        "Host": "indices.nseindia.com",
    }
    try:
        sess = requests.Session()
        sess.headers.update(headers)
        sess.get("https://www.nseindia.com/", timeout=10)
        time.sleep(0.5)
        resp = sess.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            symbols = [item["symbol"] for item in data.get("data", []) if item.get("symbol")]
            if len(symbols) > 200:  # Sanity check
                return symbols
    except Exception:
        pass
    
    # Fallback triggered if NSE blocks or fails
    st.warning("⚠️ NSE API blocked/failed. Using recent cached constituent snapshot.", icon="⚠️")
    return list(set(_FALLBACK_CONSTITUENTS)) # Remove duplicates if any


def _rsi_wilder(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder-smoothed RSI."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _analyse(symbol: str, df: pd.DataFrame) -> dict | None:
    """Apply all Stage-2 breakout filters."""
    close = df["Close"]
    vol = df["Volume"]

    dma20 = close.rolling(DMA_FAST).mean()
    dma50 = close.rolling(DMA_SLOW).mean()
    avg_vol = vol.rolling(VOL_AVG_PERIOD).mean()
    rsi = _rsi_wilder(close, RSI_PERIOD)

    c = close.iloc[-1]
    c_prev = close.iloc[-2]
    v_today = vol.iloc[-1]
    d20, d50 = dma20.iloc[-1], dma50.iloc[-1]
    d20_prev, d50_prev = dma20.iloc[-2], dma50.iloc[-2]
    av = avg_vol.iloc[-1]
    r = rsi.iloc[-1]

    if pd.isna([d20, d50, av, r]).any():
        return None

    above_20 = c > d20
    above_50 = c > d50
    vol_ratio = v_today / av if av > 0 else 0
    vol_surge = vol_ratio >= VOL_RATIO_THRESHOLD
    rsi_ok = RSI_LO <= r <= RSI_HI
    min_vol_ok = v_today >= MIN_VOLUME

    if not (above_20 and above_50 and vol_surge and rsi_ok and min_vol_ok):
        return None

    fresh = (c_prev <= d20_prev and c > d20) or (c_prev <= d50_prev and c > d50)

    return {
        "Symbol": symbol,
        "Close": round(c, 2),
        "Day_Chg": round((c - c_prev) / c_prev * 100, 2),
        "DMA_20": round(d20, 2),
        "DMA_50": round(d50, 2),
        "Above_20": round((c - d20) / d20 * 100, 2),
        "Above_50": round((c - d50) / d50 * 100, 2),
        "Volume": int(v_today),
        "Avg_Vol_10D": int(av),
        "Vol_Ratio": round(vol_ratio, 2),
        "RSI": round(r, 1),
        "Breakout": "🔴 Fresh" if fresh else "🟢 Sustained",
        "Data_Date": str(df.index[-1].date()),
    }


@st.cache_data(ttl=1800, show_spinner="⏳ Bulk-fetching EOD data for 250 stocks (takes ~10s)...")
def run_screener():
    """Full pipeline: constituents -> bulk fetch -> analyse."""
    symbols = get_index_constituents()
    if not symbols:
        return pd.DataFrame(), 0, 0, 0, ""

    total = len(symbols)
    tickers = [f"{s}.NS" for s in symbols]

    # BULK FETCH: 1 API call instead of 250
    try:
        raw_df = yf.download(
            tickers, period=HISTORY_PERIOD, group_by='ticker', 
            auto_adjust=True, threads=True, progress=False
        )
    except Exception as e:
        st.error(f"Yahoo Finance download failed: {e}")
        return pd.DataFrame(), total, 0, total, ""

    # Parse multi-index dataframe
    results = []
    latest_date = ""
    
    for t in tickers:
        sym = t.replace(".NS", "")
        try:
            # Handle single ticker (flat df) vs multi-ticker (multi-index df)
            if len(tickers) == 1:
                sub = raw_df.dropna(how='all')
            else:
                sub = raw_df[t].dropna(how='all')
                
            if len(sub) >= max(DMA_SLOW, RSI_PERIOD) + 2:
                sub.columns = [c[0] if isinstance(c, tuple) else c for c in sub.columns]
                res = _analyse(sym, sub)
                if res:
                    results.append(res)
                    latest_date = res["Data_Date"]
        except Exception:
            continue

    errors = total - len(results)
    df_out = pd.DataFrame(results)

    if not df_out.empty:
        df_out = df_out.sort_values("Vol_Ratio", ascending=False)

    return df_out, total, len(results), errors, latest_date


# ══════════════════════════════════════════════
# UI LAYER
# ══════════════════════════════════════════════

def _card(css_class: str, number, label: str) -> str:
    return f'<div class="card {css_class}"><div class="num">{number}</div><div class="lbl">{label}</div></div>'

def main():
    # ── Working Auto-Refresh Component ──
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh every 5 min", value=True, key="auto_ref")
    if auto_refresh:
        st_autorefresh(interval=REFRESH_SEC * 1000, limit=None, key="datarefresh")

    with st.sidebar:
        st.markdown('<p class="sb-head">⚙️ Controls</p>', unsafe_allow_html=True)
        if st.button("🔍 Force Run Screener Now", use_container_width=True, type="primary"):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.markdown('<p class="sb-head">📋 Screening Criteria</p>', unsafe_allow_html=True)
        st.markdown("""
            <div class="criteria">
            <strong>Stage 2 Breakout:</strong><br>
            ✅ Close &gt; 20-DMA <em>and</em> 50-DMA<br>
            ✅ Volume &ge; 150 % of 10-day avg<br>
            ✅ RSI(14) between 50 – 70<br>
            ✅ Volume &ge; 1,00,000 shares<br><br>
            <strong>Breakout tags:</strong><br>
            🔴 <em>Fresh</em> — crossed DMA today<br>
            🟢 <em>Sustained</em> — already above DMAs
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.caption("ℹ️ **Data Source:** NSE constituents + Yahoo Finance EOD. Run after 7 PM IST for complete data.")

    # ── header ──
    now_str = datetime.now().strftime("%d %b %Y · %I:%M %p")
    st.markdown('<p class="hero-title">🚀 Stage 2 Breakout Screener</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="hero-sub">Nifty Microcap 250 · EOD Analysis · {now_str}</p>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # ── run screener ──
    df, total, fetched, errors, data_date = run_screener()
    n_pass = len(df)

    # ── metric cards ──
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(_card("card-purple", total, "Total Constituents"), unsafe_allow_html=True)
    c2.markdown(_card("card-slate", fetched, "Valid Data Fetched"), unsafe_allow_html=True)
    c3.markdown(_card("card-teal", n_pass, "Breakout Stocks"), unsafe_allow_html=True)
    c4.markdown(_card("card-rose", errors, "Skipped / Errors"), unsafe_allow_html=True)

    if data_date:
        st.caption(f"📅 Latest trading date in dataset: **{data_date}**")

    # ── results table ──
    st.markdown("---")
    if df.empty:
        st.info("📊 **No stocks passed the Stage 2 Breakout criteria today.**\n\nPossible reasons:\n• Market in consolidation / correction\n• Insufficient volume on breakout attempts\n• RSI outside 50-70 range for most breakouts\n• Run after 7 PM IST for complete EOD data")
    else:
        st.markdown(f"### 🎯 {n_pass} Stock{'s' if n_pass != 1 else ''} Passed All Criteria")
        st.caption("Click any column header to sort · Hover for full values")

        st.dataframe(
            df.drop(columns=["Data_Date"]),
            use_container_width=True,
            hide_index=True,
            height=min(620, max(360, n_pass * 38 + 44)),
            column_config={
                "Symbol":      st.column_config.TextColumn("Ticker", width="small"),
                "Close":       st.column_config.NumberColumn("Close (₹)", format="₹ %.2f", width="medium"),
                "Day_Chg":     st.column_config.NumberColumn("Day Chg %", format="%.2f %%", width="medium"),
                "DMA_20":      st.column_config.NumberColumn("20 DMA (₹)", format="₹ %.2f", width="medium"),
                "DMA_50":      st.column_config.NumberColumn("50 DMA (₹)", format="₹ %.2f", width="medium"),
                "Above_20":    st.column_config.NumberColumn("Above 20 DMA %", format="%.2f %%", width="medium"),
                "Above_50":    st.column_config.NumberColumn("Above 50 DMA %", format="%.2f %%", width="medium"),
                "Volume":      st.column_config.NumberColumn("Volume", format="%,d", width="medium"),
                "Avg_Vol_10D": st.column_config.NumberColumn("10D Avg Vol", format="%,d", width="medium"),
                "Vol_Ratio":   st.column_config.NumberColumn("Vol Ratio", format="%.2f x", width="medium"),
                "RSI":         st.column_config.NumberColumn("RSI (14)", format="%.1f", width="medium"),
                "Breakout":    st.column_config.TextColumn("Breakout", width="medium"),
            },
        )

        csv = df.to_csv(index=False).encode("utf-8")
        stamp = datetime.now().strftime("%Y%m%d")
        st.download_button(
            label="📥 Download CSV", data=csv,
            file_name=f"stage2_breakout_microcap250_{stamp}.csv",
            mime="text/csv", use_container_width=True,
        )

if __name__ == "__main__":
    main()