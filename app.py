#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Stage 2 Breakout Screener — Nifty Microcap 250           ║
║   Criteria: Price > 20 & 50 DMA | Vol > 150% 10D Avg      ║
║             RSI 50-70 | Volume > 1 Lakh                    ║
║   Source: NSE (constituents) + Yahoo Finance (EOD prices)  ║
╚══════════════════════════════════════════════════════════════╝
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
INDEX_NAME = "NIFTY MICROCAP 250"
REFRESH_SEC = 300          # auto-refresh interval (5 min)
MAX_WORKERS = 8            # parallel yfinance threads
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
    page_title="Stage 2 Breakout Screener | Nifty Microcap 250",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# CUSTOM CSS
# ──────────────────────────────────────────────
st.markdown(
    """
<style>
    /* ── metric cards ── */
    .card {
        padding: 1.25rem 1rem;
        border-radius: 0.85rem;
        color: #fff;
        text-align: center;
        box-shadow: 0 4px 14px rgba(0,0,0,.12);
    }
    .card .num { font-size: 2.1rem; font-weight: 800; line-height: 1.1; }
    .card .lbl { font-size: 0.82rem; opacity: 0.9; margin-top: 2px; }
    .card-purple  { background: linear-gradient(135deg,#667eea,#764ba2); }
    .card-teal    { background: linear-gradient(135deg,#11998e,#38ef7d); }
    .card-rose    { background: linear-gradient(135deg,#f093fb,#f5576c); }
    .card-slate   { background: linear-gradient(135deg,#475569,#1e293b); }

    /* ── criteria box ── */
    .criteria {
        background: #f8fafc;
        border-left: 4px solid #667eea;
        padding: 1rem 1.25rem;
        border-radius: 0 0.6rem 0.6rem 0;
        font-size: 0.92rem;
        line-height: 1.7;
    }

    /* ── header ── */
    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        background: linear-gradient(90deg,#667eea,#f5576c);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
    }
    .hero-sub {
        text-align: center;
        color: #64748b;
        font-size: 0.95rem;
        margin-top: -4px;
    }

    /* ── table tweaks ── */
    .stDataFrame { font-size: 0.88rem; }

    /* ── sidebar section headings ── */
    .sb-head {
        font-weight: 700;
        font-size: 0.95rem;
        margin-bottom: 0.4rem;
    }
</style>
""",
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════
# DATA LAYER — all pure functions, no UI
# ══════════════════════════════════════════════

@st.cache_data(ttl=86_400, show_spinner=False)          # refresh once a day
def _get_index_constituents() -> list[dict]:
    """
    Hit the NSE indices API to get every symbol in NIFTY MICROCAP 250.
    Returns [{'symbol': 'XYZ', 'company_name': '...'}, ...]
    """
    url = (
        "https://indices.nseindia.com/api/equity-stockIndices"
        f"?index={INDEX_NAME.replace(' ', '%20')}"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
        "Host": "indices.nseindia.com",
    }

    sess = requests.Session()
    sess.headers.update(headers)

    # Land on the main page first to pick up cookies
    try:
        sess.get("https://www.nseindia.com/", timeout=10)
        time.sleep(0.4)
        resp = sess.get(url, timeout=15)
    except requests.RequestException:
        return []

    if resp.status_code != 200:
        return []

    data = resp.json()
    out = []
    for item in data.get("data", []):
        sym = item.get("symbol", "")
        if not sym:
            continue
        meta = item.get("meta", {})
        out.append(
            {
                "symbol": sym,
                "company_name": meta.get("companyName", sym),
            }
        )
    return out


def _fetch_one(args: tuple[str, str]):
    """Return (symbol, DataFrame | None) for a single ticker."""
    symbol, period = args
    try:
        df = yf.Ticker(f"{symbol}.NS").history(
            period=period, auto_adjust=True, progress=False
        )
        if df is not None and len(df) >= max(DMA_SLOW, RSI_PERIOD) + 2:
            return symbol, df
    except Exception:
        pass
    return symbol, None


def _rsi_wilder(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder-smoothed RSI (same as TradingView / Chartink)."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _analyse(symbol: str, df: pd.DataFrame) -> dict | None:
    """
    Apply all Stage-2 breakout filters.
    Returns a result dict if the stock passes, else None.
    """
    close = df["Close"]
    vol = df["Volume"]

    dma20 = close.rolling(DMA_FAST).mean()
    dma50 = close.rolling(DMA_SLOW).mean()
    avg_vol = vol.rolling(VOL_AVG_PERIOD).mean()
    rsi = _rsi_wilder(close, RSI_PERIOD)

    c = close.iloc[-1]
    c_prev = close.iloc[-2]
    v = close.iloc[-1]          # shorthand — but we need *volume*
    v_today = vol.iloc[-1]
    d20 = dma20.iloc[-1]
    d50 = dma50.iloc[-1]
    av = avg_vol.iloc[-1]
    r = rsi.iloc[-1]

    # Previous-day DMA values for "fresh breakout" detection
    d20_prev = dma20.iloc[-2]
    d50_prev = dma50.iloc[-2]

    # Any NaN ⇒ skip
    if pd.isna([d20, d50, av, r]).any():
        return None

    # ── filters ──
    above_20 = c > d20
    above_50 = c > d50
    vol_ratio = v_today / av if av > 0 else 0
    vol_surge = vol_ratio >= VOL_RATIO_THRESHOLD
    rsi_ok = RSI_LO <= r <= RSI_HI
    min_vol_ok = v_today >= MIN_VOLUME

    if not (above_20 and above_50 and vol_surge and rsi_ok and min_vol_ok):
        return None

    # Fresh breakout = today's close crossed above a DMA it was below yesterday
    fresh = (c_prev <= d20_prev and c > d20) or (c_prev <= d50_prev and c > d50)

    return {
        "Symbol": symbol,
        "Company": "",
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


@st.cache_data(ttl=1800, show_spinner="🔍 Screening 250 microcap stocks …")
def run_screener():
    """
    Full pipeline: constituents → fetch → analyse.
    Returns (results_df, total, fetched_ok, fetch_errors, data_date).
    """
    constituents = _get_index_constituents()
    if not constituents:
        return pd.DataFrame(), 0, 0, 0, ""

    total = len(constituents)
    symbols = [c["symbol"] for c in constituents]
    name_map = {c["symbol"]: c["company_name"] for c in constituents}

    # Parallel fetch
    raw: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {
            pool.submit(_fetch_one, (s, HISTORY_PERIOD)): s for s in symbols
        }
        for fut in as_completed(futs):
            sym, df = fut.result()
            if df is not None:
                raw[sym] = df

    # Analyse
    rows = []
    latest_date = ""
    for sym, df in raw.items():
        res = _analyse(sym, df)
        if res:
            res["Company"] = name_map.get(sym, sym)
            rows.append(res)
            latest_date = res["Data_Date"]

    errors = total - len(raw)
    df_out = pd.DataFrame(rows)

    if not df_out.empty:
        col_order = [
            "Symbol", "Company", "Close", "Day_Chg",
            "DMA_20", "DMA_50", "Above_20", "Above_50",
            "Volume", "Avg_Vol_10D", "Vol_Ratio", "RSI",
            "Breakout", "Data_Date",
        ]
        df_out = df_out[col_order].sort_values("Vol_Ratio", ascending=False)

    return df_out, total, len(raw), errors, latest_date


# ══════════════════════════════════════════════
# UI LAYER
# ══════════════════════════════════════════════

def _card(css_class: str, number, label: str) -> str:
    return (
        f'<div class="card {css_class}">'
        f'<div class="num">{number}</div>'
        f'<div class="lbl">{label}</div></div>'
    )


def main():
    # ── sidebar controls ──
    with st.sidebar:
        st.markdown('<p class="sb-head">⚙️ Controls</p>', unsafe_allow_html=True)
        auto_refresh = st.checkbox(
            "🔄 Auto-refresh every 5 min", value=True, key="auto_ref"
        )
        if st.button("🔍 Run Screener Now", use_container_width=True, type="primary"):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.markdown('<p class="sb-head">📋 Screening Criteria</p>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="criteria">
            <strong>Stage 2 Breakout:</strong><br>
            ✅ Close &gt; 20-DMA <em>and</em> 50-DMA<br>
            ✅ Volume &ge; 150 % of 10-day avg<br>
            ✅ RSI(14) between 50 – 70<br>
            ✅ Volume &ge; 1,00,000 shares<br><br>
            <strong>Breakout tags:</strong><br>
            🔴 <em>Fresh</em> — crossed above a DMA today<br>
            🟢 <em>Sustained</em> — already above both DMAs
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown('<p class="sb-head">ℹ️ About</p>', unsafe_allow_html=True)
        st.markdown(
            """
            Identifies early **Stage 2 advances** per
            Stan Weinstein's methodology in the Nifty Microcap 250
            universe.

            **Data:** Constituents from NSE · Prices from Yahoo
            Finance (`.NS` tickers).

            **Best time to run:** After 7 PM IST for complete
            end-of-day data.
            """
        )

        st.markdown("---")
        st.markdown('<p class="sb-head">⚠️ Disclaimer</p>', unsafe_allow_html=True)
        st.markdown(
            "<small>For educational use only. Not financial advice. "
            "Microcap stocks are illiquid and high-risk. "
            "Do your own due diligence.</small>",
            unsafe_allow_html=True,
        )

    # ── auto-refresh JS (client-side) ──
    if auto_refresh:
        st.markdown(
            f"<script>setTimeout(()=>{{location.reload()}},{REFRESH_SEC*1000})</script>",
            unsafe_allow_html=True,
        )

    # ── header ──
    now_str = datetime.now().strftime("%d %b %Y · %I:%M %p")
    st.markdown('<p class="hero-title">🚀 Stage 2 Breakout Screener</p>', unsafe_allow_html=True)
    st.markdown(
        f'<p class="hero-sub">Nifty Microcap 250 · EOD Analysis · {now_str}</p>',
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── run screener ──
    df, total, fetched, errors, data_date = run_screener()
    n_pass = len(df)

    # ── metric cards ──
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(_card("card-purple", total, "Total Constituents"), unsafe_allow_html=True)
    c2.markdown(_card("card-slate", fetched, "Data Retrieved"), unsafe_allow_html=True)
    c3.markdown(_card("card-teal", n_pass, "Breakout Stocks"), unsafe_allow_html=True)
    c4.markdown(_card("card-rose", errors, "Skipped / Errors"), unsafe_allow_html=True)

    # ── data-date badge ──
    if data_date:
        st.caption(f"📅 Latest trading date in dataset: **{data_date}**")

    # ── results table ──
    st.markdown("---")
    if df.empty:
        st.info(
            "📊 **No stocks passed the Stage 2 Breakout criteria today.**\n\n"
            "Possible reasons:\n"
            "• Market in consolidation / correction\n"
            "• Insufficient volume on breakout attempts\n"
            "• RSI outside 50-70 range for most breakouts\n"
            "• Run after 7 PM IST for complete EOD data"
        )
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
                "Company":     st.column_config.TextColumn("Company Name", width="large"),
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

        # ── download ──
        csv = df.to_csv(index=False).encode("utf-8")
        stamp = datetime.now().strftime("%Y%m%d")
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"stage2_breakout_microcap250_{stamp}.csv",
            mime="text/csv",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()