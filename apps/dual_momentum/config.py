"""
Dual Momentum ETF App — configuration, universe definition, tax rules, defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

# ---------------------------------------------------------------------------
# ETF Universe
# Each entry: display name, tax category, TRI index name (None = yfinance only),
# optional liquidity warning.
# ---------------------------------------------------------------------------

ETF_UNIVERSE: dict[str, dict] = {
    # --- Broad Market ---
    "NIFTYBEES.NS": {
        "name": "Nifty 50",
        "group": "Broad Market",
        "tax_cat": "equity",
        "tri_name": "NIFTY 50",
        "liquidity_warn": None,
    },
    "JUNIORBEES.NS": {
        "name": "Nifty Next 50",
        "group": "Broad Market",
        "tax_cat": "equity",
        "tri_name": "NIFTY NEXT 50",
        "liquidity_warn": None,
    },
    "MID150BEES.NS": {
        "name": "Nifty Midcap 150",
        "group": "Broad Market",
        "tax_cat": "equity",
        "tri_name": "NIFTY MIDCAP 150",
        "liquidity_warn": None,
    },
    "SMALL250BES.NS": {
        "name": "Nifty Smallcap 250",
        "group": "Broad Market",
        "tax_cat": "equity",
        "tri_name": "NIFTY SMALLCAP 250",
        "liquidity_warn": None,
    },
    # --- Factor / Momentum ---
    "MOMOMENTUM.NS": {
        "name": "Nifty 200 Momentum 30",
        "group": "Factor",
        "tax_cat": "equity",
        "tri_name": "NIFTY200 MOMENTUM 30",
        "liquidity_warn": None,
    },
    "MOMENTUM50.NS": {
        "name": "Nifty 500 Momentum 50",
        "group": "Factor",
        "tax_cat": "equity",
        "tri_name": "NIFTY500 MOMENTUM 50",
        "liquidity_warn": None,
    },
    "MOMIDMTM.NS": {
        "name": "Midcap 150 Momentum 50",
        "group": "Factor",
        "tax_cat": "equity",
        "tri_name": "NIFTY MIDCAP150 MOMENTUM 50",
        "liquidity_warn": "Low AUM (~₹39 Cr) — wide bid-ask spreads possible.",
    },
    "ALPL30IETF.NS": {
        "name": "Nifty Alpha Low Vol 30",
        "group": "Factor",
        "tax_cat": "equity",
        "tri_name": "NIFTY ALPHA LOW-VOLATILITY 30",
        "liquidity_warn": None,
    },
    # --- Alternatives ---
    "GOLDBEES.NS": {
        "name": "Gold",
        "group": "Alternatives",
        "tax_cat": "gold",
        "tri_name": None,
        "liquidity_warn": None,
    },
    "SILVERCASE.NS": {
        "name": "Silver",
        "group": "Alternatives",
        "tax_cat": "silver",
        "tri_name": None,
        "liquidity_warn": None,
    },
    "MON100.NS": {
        "name": "Nasdaq 100 (US Tech)",
        "group": "Alternatives",
        "tax_cat": "overseas",
        "tri_name": None,
        "liquidity_warn": None,
    },
    # --- Cash ---
    "LIQUIDCASE.NS": {
        "name": "Cash (Overnight)",
        "group": "Cash",
        "tax_cat": "debt",
        "tri_name": None,
        "liquidity_warn": None,
    },
}

CASH_TICKER = "LIQUIDCASE.NS"

# Tickers that use TRI as history backbone (list for ordered reference)
TRI_TICKERS = [t for t, m in ETF_UNIVERSE.items() if m["tri_name"] is not None]

# ---------------------------------------------------------------------------
# Tax Rules (post FY2024-25 budget)
# ---------------------------------------------------------------------------


@dataclass
class TaxRule:
    stcg_rate: float  # short-term capital gains rate
    ltcg_rate: float  # long-term capital gains rate
    lt_months: int  # holding period (months) to qualify as long-term
    income_tax: bool  # True = always taxed as income (no LTCG benefit)


TAX_RULES: dict[str, TaxRule] = {
    # Equity ETFs (>65% Indian equity)
    "equity": TaxRule(stcg_rate=0.20, ltcg_rate=0.125, lt_months=12, income_tax=False),
    # Gold ETF
    "gold": TaxRule(stcg_rate=0.20, ltcg_rate=0.125, lt_months=24, income_tax=False),
    # Silver ETF
    "silver": TaxRule(stcg_rate=0.20, ltcg_rate=0.125, lt_months=24, income_tax=False),
    # Overseas / international ETF (MON100)
    "overseas": TaxRule(stcg_rate=0.30, ltcg_rate=0.125, lt_months=24, income_tax=False),
    # Debt / overnight ETF (LiquidCase) — always income tax regardless of holding period
    # lt_months is irrelevant when income_tax=True; set to 0 so the field is never
    # accidentally relied upon as a threshold.
    "debt": TaxRule(stcg_rate=0.30, ltcg_rate=0.30, lt_months=0, income_tax=True),
}

EDUCATION_CESS = 0.04  # 4% on tax amount
SURCHARGE_RATE = 0.10  # 10% optional surcharge (income > ₹50L)

# ---------------------------------------------------------------------------
# Momentum Lookback Options
# Values are number of weekly bars to look back.
# Reference: https://engineeredportfolio.com/2018/05/02/accelerating-dual-momentum-investing/
# The Engineered Portfolio uses 1M + 3M + 6M composite return as signal.
# ---------------------------------------------------------------------------

LOOKBACK_OPTIONS: dict[str, int] = {
    "1 Month (4 weeks)": 4,
    "3 Months (13 weeks)": 13,
    "6 Months (26 weeks)": 26,
    "12 Months (52 weeks)": 52,
}

DEFAULT_LOOKBACKS = ["1 Month (4 weeks)", "3 Months (13 weeks)", "6 Months (26 weeks)"]

# ---------------------------------------------------------------------------
# Rebalance
# ---------------------------------------------------------------------------

REBALANCE_FREQ_OPTIONS = ["Monthly", "Weekly"]
REBALANCE_DAY_OPTIONS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
DEFAULT_REBALANCE_FREQ = "Monthly"
DEFAULT_REBALANCE_DAY = "Wednesday"

# Number of consecutive weeks cash must be absent before exiting weekly mode
WEEKLY_MODE_EXIT_WEEKS = 4

# ---------------------------------------------------------------------------
# Portfolio defaults
# ---------------------------------------------------------------------------

DEFAULT_CORPUS = 1_000_000  # ₹10 lakhs
DEFAULT_TOP_N = 4
DEFAULT_TXN_COST_PCT = 0.001  # 0.1% round-trip per trade

WeightMode = Literal["equal", "score"]
DEFAULT_WEIGHT_MODE: WeightMode = "equal"

# ---------------------------------------------------------------------------
# Data paths
# ---------------------------------------------------------------------------

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_DIR = os.path.join(_ROOT, "data")
TRI_PARQUET = os.path.join(DATA_DIR, "dual_momentum_tri.parquet")
DELTA_PARQUET = os.path.join(DATA_DIR, "dual_momentum_delta.parquet")
REPO_RATE_CSV = os.path.join(DATA_DIR, "repo_rate.csv")
NSE_HOLIDAYS_JSON = os.path.join(_ROOT, "nse_holidays.json")
