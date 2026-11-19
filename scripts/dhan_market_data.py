"""Dhan daily candles for the shared publisher; credentials stay in env or .env."""

from __future__ import annotations

import io
import os
import time

import numpy as np
import pandas as pd
import requests

import market_data as md

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
HISTORY_URL = "https://api.dhan.co/v2/charts/historical"
AUTH_ERRORS = {"DH-901", "DH-902", "806", "807", "808", "809", "810"}


def load_credentials():
    values = {}
    path = md.ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() in {"DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN"}:
                values[key.strip()] = value.strip().strip("\"'")
    for key in ("DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN"):
        values[key] = os.environ.get(key) or values.get(key, "")
        if not values[key]:
            raise RuntimeError(f"Set {key} in the environment or the project's ignored .env file")
    return values


def resolve_instrument(master, symbol, kind):
    """Resolve exact NSE symbols, never fuzzy-match company names or another exchange."""
    if kind == "index_price":
        candidates = master[master.SEM_INSTRUMENT_NAME.eq("INDEX") & master.SEM_EXM_EXCH_ID.eq("NSE")]
        wanted = md.index_key(symbol)
    else:
        candidates = master[
            master.SEM_EXM_EXCH_ID.eq("NSE") & master.SEM_SEGMENT.eq("E") & master.SEM_INSTRUMENT_NAME.eq("EQUITY")
        ]
        wanted = symbol.removesuffix(".NS")
    matched = pd.Series(False, index=candidates.index)
    for column in ("SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME", "SEM_CUSTOM_SYMBOL"):
        if column in candidates:
            names = candidates[column]
            if kind == "index_price":
                names = names.map(md.index_key)
            matched |= names.eq(wanted)
    candidates = candidates[matched]
    if "SEM_SERIES" in candidates and candidates.SEM_SERIES.eq("EQ").any():
        candidates = candidates[candidates.SEM_SERIES.eq("EQ")]
    candidates = candidates.drop_duplicates("SEM_SMST_SECURITY_ID")
    if len(candidates) != 1:
        raise ValueError(f"Expected one exact NSE instrument for {symbol}; found {len(candidates)}")
    row = candidates.iloc[0]
    return {
        "securityId": row.SEM_SMST_SECURITY_ID,
        "exchangeSegment": "IDX_I" if kind == "index_price" else "NSE_EQ",
        "instrument": "INDEX" if kind == "index_price" else "EQUITY",
    }


def parse_candles(payload, symbol, kind, start, target):
    fields = ("timestamp", "open", "high", "low", "close", "volume")
    if not isinstance(payload, dict) or not all(isinstance(payload.get(k), list) for k in fields):
        raise ValueError("Missing daily candle arrays")
    if len({len(payload[k]) for k in fields}) != 1:
        raise ValueError("Daily candle arrays have different lengths")
    dates = pd.to_datetime(payload["timestamp"], unit="s", utc=True)
    dates = dates.tz_convert("Asia/Kolkata").tz_localize(None).normalize()
    frame = pd.DataFrame(
        dict(
            symbol=symbol,
            date=dates,
            Open=payload["open"],
            High=payload["high"],
            Low=payload["low"],
            Close=payload["close"],
            Volume=payload["volume"],
            series_type=kind,
        )
    )
    frame = frame[frame.date.between(pd.Timestamp(start), pd.Timestamp(target))]
    frame = md.normalize_prices(frame)
    numeric = frame[["Open", "High", "Low", "Close", "Volume"]]
    if not np.isfinite(numeric.to_numpy()).all():
        raise ValueError("Non-finite daily candle values")
    if frame[["Open", "High", "Low"]].le(0).any().any():
        raise ValueError("Non-positive OHLC values")
    if (frame.High + 0.02 < frame[["Open", "Low", "Close"]].max(axis=1)).any():
        raise ValueError("High below candle prices")
    if (frame.Low - 0.02 > frame[["Open", "High", "Close"]].min(axis=1)).any():
        raise ValueError("Low above candle prices")
    return frame


class DhanDownloader:
    """Read-only API client, paced below Dhan's five data requests per second."""

    provider = "dhan"
    adjustment_policy = "dhan_daily_corporate_action_adjusted"

    def __init__(self):
        credentials = load_credentials()
        # Keep the bearer token off the public instrument-master request.
        self.headers = {
            "access-token": credentials["DHAN_ACCESS_TOKEN"],
            "client-id": credentials["DHAN_CLIENT_ID"],
        }
        self.session = requests.Session()
        self.master = None
        self.last_request = 0.0
        self.errors = {}

    def _load_master(self):
        try:
            response = self.session.get(MASTER_URL, timeout=60)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Dhan instrument download failed ({type(exc).__name__})") from None
        master = pd.read_csv(io.StringIO(response.text), dtype=str, keep_default_na=False)
        required = {"SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_INSTRUMENT_NAME", "SEM_SMST_SECURITY_ID"}
        if not required.issubset(master) or not {"SEM_TRADING_SYMBOL", "SM_SYMBOL_NAME"}.intersection(master):
            raise RuntimeError("Dhan instrument master schema is unsupported")
        for column in master:
            master[column] = master[column].str.strip()
        self.master = master

    def _history(self, instrument, symbol, kind, start, target):
        body = instrument | {
            "oi": False,
            "fromDate": str(pd.Timestamp(start).date()),
            # Dhan's daily endpoint excludes toDate.
            "toDate": str((pd.Timestamp(target) + pd.Timedelta(days=1)).date()),
        }
        for attempt in range(3):
            time.sleep(max(0, 0.25 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            try:
                response = self.session.post(HISTORY_URL, headers=self.headers, json=body, timeout=40)
            except requests.RequestException as exc:
                raise RuntimeError(f"Dhan connection failed ({type(exc).__name__}); source retained") from None
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            code = str(payload.get("errorCode", "")) if isinstance(payload, dict) else ""
            if response.status_code in {401, 403} or code in AUTH_ERRORS:
                raise RuntimeError(f"Dhan authentication/subscription failed (HTTP {response.status_code}, {code})")
            retryable = response.status_code == 429 or response.status_code >= 500 or code in {"805", "DH-904"}
            if retryable and attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            if not response.ok or code:
                raise ValueError(f"Dhan history request failed (HTTP {response.status_code}, code {code})")
            return parse_candles(payload, symbol, kind, start, target)

    def __call__(self, instruments, start, target):
        if self.master is None:
            self._load_master()
        frames = []
        for ticker, (symbol, kind) in instruments.items():
            try:
                instrument = resolve_instrument(self.master, symbol, kind)
                frame = self._history(instrument, symbol, kind, start, target)
                if frame.empty:
                    raise ValueError("No daily observations returned")
                frames.append(frame)
                self.errors.pop(ticker, None)
            except ValueError as exc:
                self.errors[ticker] = str(exc)
        result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=md.PRICE_COLUMNS)
        failures = [f"{ticker}: {self.errors[ticker]}" for ticker in instruments if ticker in self.errors]
        return result, "; ".join(failures[:10]) or None
