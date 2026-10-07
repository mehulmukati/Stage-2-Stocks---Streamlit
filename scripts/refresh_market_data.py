"""Single market publisher used by GitHub Actions and explicit manual refreshes.

Page reads never call this. A publisher lock serializes downloads and publication.
History is retained; partial failures preserve accepted observations and are reported.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_data as md

INDEX_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 100": "^CNX100",
    "Nifty 500": "^CRSLDX",
    "Nifty Next 50": "^NSMIDCP",
    "Nifty Midcap 150": "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMLCAP250.NS",
    "Nifty Microcap 250": "NIFTYMICROCAP250.NS",
}


def completed_session(now=None):
    from config import IST
    from data import get_last_valid_trading_date, load_nse_holidays

    now = now or datetime.now(IST)
    day = now.date() if now.hour >= 19 else (pd.Timestamp(now) - pd.Timedelta(days=1)).date()
    return get_last_valid_trading_date(str(day), load_nse_holidays())


def reshape(raw, requests):
    """requests is {vendor ticker: (stored identity, series type)}."""
    frames = []
    for ticker, (symbol, kind) in requests.items():
        if raw is None or raw.empty:
            continue
        if isinstance(raw.columns, pd.MultiIndex):
            if ticker in raw.columns.get_level_values(0):
                sub = raw[ticker].copy()
            elif ticker in raw.columns.get_level_values(1):
                sub = raw.xs(ticker, axis=1, level=1).copy()
            else:
                continue
        elif len(requests) == 1:
            sub = raw.copy()
        else:
            continue
        if "Close" not in sub:
            continue
        sub = sub[sub.Close.notna() & sub.Close.gt(0)]
        if sub.empty:
            continue
        sub.index = pd.to_datetime(sub.index).tz_localize(None).normalize()
        sub.index.name = "date"
        for column in ("Open", "High", "Low", "Volume"):
            if column not in sub:
                sub[column] = float("nan")
        sub = sub.reset_index().assign(symbol=symbol, series_type=kind)
        sub = sub[md.PRICE_COLUMNS]
        if kind in {"equity", "etf_adjusted"}:
            sub = sub.dropna(subset=["Open", "High", "Low", "Volume"])
        frames.append(sub)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=md.PRICE_COLUMNS)


def download(requests, start, target, attempts=2):
    last_error = None
    accepted = pd.DataFrame(columns=md.PRICE_COLUMNS)
    pending = dict(requests)
    for attempt in range(attempts):
        try:
            raw = yf.download(
                list(pending),
                start=str(pd.Timestamp(start).date()),
                end=str((pd.Timestamp(target) + pd.Timedelta(days=1)).date()),
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
                timeout=20,
            )
            result = reshape(raw, pending)
            if not result.empty:
                accepted = pd.concat([accepted, result], ignore_index=True).drop_duplicates(
                    ["series_type", "symbol", "date"], keep="last"
                )
            updated = set(
                zip(
                    accepted.loc[
                        accepted.date.eq(pd.Timestamp(target))
                        & (accepted.series_type.eq("index_price") | accepted.Volume.gt(0)),
                        "symbol",
                    ],
                    accepted.loc[
                        accepted.date.eq(pd.Timestamp(target))
                        & (accepted.series_type.eq("index_price") | accepted.Volume.gt(0)),
                        "series_type",
                    ],
                )
            )
            pending = {ticker: identity for ticker, identity in pending.items() if identity not in updated}
            if not pending:
                return accepted, None
            last_error = "target session unavailable: " + ", ".join(pending)
        except Exception as exc:
            last_error = str(exc)
        if attempt + 1 < attempts:
            time.sleep(1)
    return accepted, last_error


def merge_observations(existing, new):
    """Do not allow an invalid partial observation to erase accepted data."""
    if new.empty:
        return existing
    new = md.normalize_prices(new)
    return md.normalize_prices(
        pd.concat([existing, new], ignore_index=True).drop_duplicates(["series_type", "symbol", "date"], keep="last")
    )


def revision_needs_full_history(existing, new):
    """Adjusted-close revisions in the overlap require a full-symbol refresh.

    Otherwise a dividend/split adjustment could create a seam in long history.
    """
    if new.empty or existing.empty:
        return set()
    overlap = existing.merge(new, on=["series_type", "symbol", "date"], suffixes=("_old", "_new"))
    overlap = overlap[overlap.series_type.isin(["equity", "etf_adjusted"])]
    changed = (overlap.Close_old - overlap.Close_new).abs() > overlap.Close_old.abs().mul(1e-5).clip(lower=0.01)
    return set(zip(overlap.loc[changed, "series_type"], overlap.loc[changed, "symbol"]))


def refresh_prices(
    target_date=None, force_full=False, emit=lambda level, msg: print(msg), fetch=download, only_series=None
):
    target = target_date or completed_session()
    target_ts = pd.Timestamp(target)
    with md.publisher_lock():
        snapshot = md.load_snapshot()
        existing = snapshot.prices
        pending_rebuild = set(existing.attrs.get("history_rebuild_required", []))
        rebuilt = set()
        current = snapshot.symbols(as_of=target)
        # Preserve and refresh recent ex-members too; suspended/delisted histories remain stored.
        equities = existing[existing.series_type == "equity"]
        maxima = equities.groupby("symbol").date.max()
        active = set(maxima[maxima >= target_ts - pd.Timedelta(days=14)].index) | current
        requests = {f"{s}.NS": (s, "equity") for s in sorted(active) if not s.startswith("DUMMY")}
        requests.update({ticker: (name, "index_price") for name, ticker in INDEX_TICKERS.items()})
        from apps.dual_momentum.config import ETF_UNIVERSE

        requests.update({ticker: (ticker, "etf_adjusted") for ticker in ETF_UNIVERSE})
        if only_series:
            requests = {ticker: identity for ticker, identity in requests.items() if identity[1] in only_series}
        additions = []
        errors = {}
        retained = []
        # Group by each instrument's own coverage, not a global maximum.
        groups = {}
        coverage = existing.groupby(["series_type", "symbol"]).date.agg(["min", "max"])
        for ticker, (symbol, kind) in requests.items():
            bounds = coverage.loc[(kind, symbol)] if (kind, symbol) in coverage.index else None
            if force_full or bounds is None or symbol in pending_rebuild:
                start = target_ts - pd.DateOffset(years=10)
                if bounds is not None:
                    start = min(start, bounds["min"])
            else:
                start = bounds["max"] - pd.Timedelta(days=7)
            groups.setdefault(str(start.date()), {})[ticker] = (symbol, kind)
        for start, group in groups.items():
            items = list(group.items())
            for position in range(0, len(items), 80):
                batch = dict(items[position : position + 80])
                emit("info", f"Refreshing {len(batch)} series from {start} through {target}")
                new, error = fetch(batch, start, target)
                if new.empty:
                    errors.update({ticker: error or "no observations" for ticker in batch})
                    retained.extend(batch)
                    continue
                try:
                    new = md.normalize_prices(new[new.date <= target_ts])
                except ValueError as exc:
                    errors.update({ticker: f"invalid response: {exc}" for ticker in batch})
                    retained.extend(batch)
                    continue
                corrections = revision_needs_full_history(existing, new)
                returned_identities = set(zip(new.series_type, new.symbol))
                corrections.update(
                    ("equity", symbol) for symbol in pending_rebuild if ("equity", symbol) in returned_identities
                )
                if corrections:
                    corrected = {
                        ticker: identity for ticker, identity in batch.items() if identity[::-1] in corrections
                    }
                    oldest = existing[
                        pd.MultiIndex.from_frame(existing[["series_type", "symbol"]]).isin(corrections)
                    ].date.min()
                    if pd.Timestamp(start) <= oldest:
                        # The original request already covers all stored history.
                        full, full_error = new.copy(), error
                    else:
                        full, full_error = fetch(
                            corrected, str(min(oldest, target_ts - pd.DateOffset(years=10)).date()), target
                        )
                    try:
                        full = md.normalize_prices(full[full.date <= target_ts]) if not full.empty else full
                    except ValueError as exc:
                        full_error = f"invalid adjusted-history response: {exc}"
                        full = pd.DataFrame(columns=md.PRICE_COLUMNS)
                    successful = set()
                    previous_groups = {
                        identity: frame
                        for identity, frame in existing.loc[
                            pd.MultiIndex.from_frame(existing[["series_type", "symbol"]]).isin(corrections)
                            & existing.date.le(target_ts)
                        ].groupby(["series_type", "symbol"])
                    }
                    replacement_groups = {
                        identity: frame for identity, frame in full.groupby(["series_type", "symbol"])
                    }
                    for identity in corrections:
                        kind, symbol = identity
                        previous = previous_groups.get(identity, pd.DataFrame(columns=md.PRICE_COLUMNS))
                        replacement = replacement_groups.get(identity, pd.DataFrame(columns=md.PRICE_COLUMNS))
                        # A partial upstream response must not create an adjustment seam.
                        if not replacement.empty and set(previous.date).issubset(set(replacement.date)):
                            successful.add(identity)
                            rebuilt.add(symbol)
                    full = (
                        full[full.apply(lambda r: (r.series_type, r.symbol) in successful, axis=1)]
                        if not full.empty
                        else full
                    )
                    # Reject only symbols whose long adjustment correction could not be obtained.
                    bad = corrections - successful
                    if not full.empty:
                        new = new[~new.apply(lambda r: (r.series_type, r.symbol) in corrections, axis=1)]
                        new = pd.concat([new, full], ignore_index=True)
                    else:
                        new = new[~new.apply(lambda r: (r.series_type, r.symbol) in corrections, axis=1)]
                    for kind, symbol in bad:
                        errors[symbol] = (
                            full_error or "adjusted-history correction incomplete; previous history retained"
                        )
                returned = set(zip(new.symbol, new.series_type))
                for ticker, identity in batch.items():
                    if identity not in returned:
                        errors.setdefault(ticker, "no usable observations returned")
                        retained.append(ticker)
                if not new.empty:
                    additions.append(new)
        new = pd.concat(additions, ignore_index=True) if additions else pd.DataFrame(columns=md.PRICE_COLUMNS)
        if new.empty:
            raise RuntimeError("Shared refresh returned no accepted observations; prior files retained")
        merged = merge_observations(existing, new)
        from apps.dual_momentum.config import CASH_TICKER

        actual = merged[(merged.series_type == "etf_adjusted") & (merged.symbol == CASH_TICKER)].set_index("date").Close
        cash_metadata = {}
        if not actual.empty:
            from apps.dual_momentum.data import _load_liquidcase

            proxy = _load_liquidcase(emit, etf_series=actual)
            proxy = proxy[proxy.index <= target_ts]
            cash = pd.DataFrame(
                dict(symbol=CASH_TICKER, date=proxy.index, Close=proxy.values, series_type="cash_proxy")
            )
            for column in ("Open", "High", "Low", "Volume"):
                cash[column] = float("nan")
            merged = merge_observations(merged, cash)
            import hashlib

            from apps.dual_momentum.config import REPO_RATE_CSV

            rate_path = Path(REPO_RATE_CSV)
            cash_metadata = {
                "cash_proxy_policy": "repo-rate backfill before first actual ETF quote",
                "repo_rate_sha256": (
                    hashlib.sha256(rate_path.read_bytes()).hexdigest() if rate_path.exists() else None
                ),
            }
        # Embed actual coverage, not just the requested date, in this same file.
        observed = (
            merged[(merged.series_type == "equity") & merged.Close.gt(0) & merged.Volume.gt(0)]
            .groupby("symbol")
            .date.max()
        )
        missing = sorted(s for s in current if s not in observed or observed[s] < target_ts)
        report = {
            "target_date": target,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "missing_target_symbols": missing,
            "errors": errors,
            "retained_series": sorted(set(retained)),
            "adjustment_policy": "yahoo_auto_adjust",
            "source": "shared publisher",
            "constituent_revision": snapshot.constituent_revision,
            "history_rebuild_required": sorted(pending_rebuild - rebuilt),
        }
        report.update(cash_metadata)
        revision = md.write_source(merged, md.PRICE_PATH, report, expected_revision=snapshot.price_revision)
        emit(
            "warning" if missing else "success",
            f"Published price revision {revision[:12]}; {len(missing)} constituents lack target-session prices",
        )
        return report | {"revision": revision}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--target-date")
    parser.add_argument("--series-type", action="append", choices=["equity", "index_price", "etf_adjusted"])
    args = parser.parse_args()
    refresh_prices(args.target_date, args.full, only_series=args.series_type)
