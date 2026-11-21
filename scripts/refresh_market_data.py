"""Single market publisher used by GitHub Actions and explicit manual refreshes.

Page reads never call this. A publisher lock serializes downloads and publication.
History is retained; partial failures preserve accepted observations and are reported.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import market_data as md  # noqa: E402 -- repo imports need the script's parent on sys.path
from equity_data import (  # noqa: E402
    classify_equity_rows,
    observation_diagnostics,
    prepare_publication,
    tradable_equity_rows,
)

INDEX_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty 100": "^CNX100",
    "Nifty 500": "^CRSLDX",
    "Nifty Next 50": "^NSMIDCP",
    "Nifty Midcap 150": "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMLCAP250.NS",
    "Nifty Microcap 250": "NIFTYMICROCAP250.NS",
}


class PublicationRejected(RuntimeError):
    """Carry rejected candidate diagnostics without publishing its prices."""

    def __init__(self, message, coverage, errors, provider_diagnostics):
        super().__init__(message)
        self.candidate_coverage = coverage
        self.provider_errors = errors
        self.provider_diagnostics = provider_diagnostics


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
        if kind == "etf_adjusted":
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
    if "price_status" in existing:
        verified = existing[existing.price_status.eq("verified_mark")]
        if not verified.empty:
            keys = ["series_type", "symbol", "date"]
            matched = new.merge(
                verified[keys + ["Close", "verification_source", "verified_at"]],
                on=keys,
                how="left",
                suffixes=("", "_verified"),
            )
            agrees = (matched.Close - matched.Close_verified).abs().le(0.01) & matched.Volume.eq(0)
            for column in ("price_status", "verification_source", "verified_at"):
                if column in new:
                    new[column] = new[column].astype(object)
            new.loc[agrees, "price_status"] = "verified_mark"
            for column in ("verification_source", "verified_at"):
                candidate = column + "_verified" if column + "_verified" in matched else column
                new.loc[agrees, column] = matched.loc[agrees, candidate].to_numpy()
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


def history_is_complete(previous, replacement):
    """Require every accepted observation, excluding known non-trading placeholders.

    Yahoo omits zero-volume holiday/suspension rows carried by legacy files.
    Those rows must not permanently prevent a full adjusted-history rebuild.
    An omitted trading quote may be retained when nearby overlapping
    observations on both sides confirm an unchanged adjustment basis.
    Unknown-volume close-only history still requires replacement coverage.
    """
    if replacement.empty:
        return False
    required = previous[previous.Volume.ne(0)]
    missing = required.loc[~required.date.isin(replacement.date)]
    if missing.empty:
        return True
    if not missing.Volume.gt(0).all():
        return False
    overlap = required.loc[required.Volume.gt(0)].merge(
        replacement.loc[replacement.Volume.gt(0)], on="date", suffixes=("_old", "_new")
    )
    overlap = overlap.sort_values("date")
    for day in missing.date:
        before = overlap[overlap.date.lt(day)].tail(1)
        after = overlap[overlap.date.gt(day)].head(1)
        if before.empty or after.empty:
            return False
        neighbors = pd.concat([before, after])
        if (neighbors.date - day).abs().gt(pd.Timedelta(days=7)).any():
            return False
        tolerance = neighbors.Close_old.abs().mul(1e-5).clip(lower=0.01)
        if (neighbors.Close_old - neighbors.Close_new).abs().gt(tolerance).any():
            return False
    return True


def price_coverage(prices, current, target_date):
    target = pd.Timestamp(target_date)
    eligible = prices[prices.date.le(target) & prices.Close.gt(0)]
    equities = tradable_equity_rows(eligible[eligible.series_type.eq("equity")])
    observed = equities.groupby("symbol").date.max()
    missing = sorted(s for s in current if s not in observed or observed[s] < target)
    dates = observed.reindex(sorted(current)).dropna()
    benchmarks = eligible[eligible.series_type.eq("index_price")].groupby("symbol").date.max()
    missing_benchmarks = sorted(
        name for name in ("Nifty 50", "Nifty 100", "Nifty 500") if name not in benchmarks or benchmarks[name] < target
    )
    return {
        "missing_target_symbols": missing,
        "current_symbol_count": len(current),
        "target_session_symbol_count": len(current) - len(missing),
        "actual_latest_date": str(dates.min().date()) if len(dates) == len(current) and len(dates) else None,
        "max_price_date": str(dates.max().date()) if len(dates) else None,
        "missing_target_benchmarks": missing_benchmarks,
        "publication_status": "partial" if missing or missing_benchmarks else "fresh",
    }


def validate_publication(coverage, target, min_coverage=0.94):
    """Reject a market-wide lag before replacing the accepted price revision."""
    failures = []
    count = coverage["current_symbol_count"]
    fresh = coverage["target_session_symbol_count"]
    if not fresh:
        failures.append(f"no current constituents have a tradable {target} price")
    elif not count or fresh / count < min_coverage:
        failures.append(f"only {fresh}/{count} constituents cover {target}; at least {min_coverage:.0%} required")
    if coverage["missing_target_benchmarks"]:
        failures.append("benchmarks lack target-session prices: " + ", ".join(coverage["missing_target_benchmarks"]))
    if failures:
        raise RuntimeError("Shared publication is incomplete: " + "; ".join(failures))


def check_publication(target_date=None):
    """Check the accepted shared revision without downloading or writing data."""
    snapshot = md.load_snapshot()
    target = target_date or completed_session()
    coverage = price_coverage(snapshot.prices, snapshot.symbols(as_of=target), target)
    validate_publication(coverage, target)
    return coverage


def market_requests(snapshot, target, only_series=None):
    target_ts = pd.Timestamp(target)
    current = snapshot.symbols(as_of=target)
    # Preserve and refresh recent ex-members too; suspended/delisted histories remain stored.
    equities = snapshot.prices[snapshot.prices.series_type == "equity"]
    maxima = equities.groupby("symbol").date.max()
    active = set(maxima[maxima >= target_ts - pd.Timedelta(days=14)].index) | current
    requests = {f"{s}.NS": (s, "equity") for s in sorted(active) if not s.startswith("DUMMY")}
    requests.update({ticker: (name, "index_price") for name, ticker in INDEX_TICKERS.items()})
    from apps.dual_momentum.config import ETF_UNIVERSE

    requests.update({ticker: (ticker, "etf_adjusted") for ticker in ETF_UNIVERSE})
    if only_series:
        requests = {ticker: identity for ticker, identity in requests.items() if identity[1] in only_series}
    return requests


def usable_series_dates(prices, target):
    eligible = prices[prices.date.le(pd.Timestamp(target)) & prices.Close.gt(0)]
    equity = tradable_equity_rows(classify_equity_rows(eligible[eligible.series_type.eq("equity")]))
    indices = eligible[eligible.series_type.eq("index_price")]
    etfs = eligible[eligible.series_type.eq("etf_adjusted") & eligible.Volume.gt(0)]
    etfs = etfs.dropna(subset=["Open", "High", "Low", "Volume"])
    return pd.concat([equity, indices, etfs]).groupby(["series_type", "symbol"]).date.max()


def missing_requested_series(prices, requests, target):
    dates = usable_series_dates(prices, target)
    return {
        ticker: (symbol, kind)
        for ticker, (symbol, kind) in requests.items()
        if dates.get((kind, symbol)) != pd.Timestamp(target)
    }


def validate_response(frame, requests, target, errors):
    """Isolate bad series so they cannot discard other usable provider responses."""
    frames = []
    missing = set(md.PRICE_COLUMNS) - set(frame)
    if missing:
        errors.update({ticker: f"Missing price columns: {sorted(missing)}" for ticker in requests})
        return pd.DataFrame(columns=md.PRICE_COLUMNS)
    tickers = {(kind, symbol): ticker for ticker, (symbol, kind) in requests.items()}
    for identity, group in frame.groupby(["series_type", "symbol"]):
        if identity not in tickers:
            continue
        try:
            group = group.copy()
            group.date = pd.to_datetime(group.date, errors="raise")
            group = md.normalize_prices(group[group.date.le(pd.Timestamp(target))])
        except ValueError as exc:
            errors[tickers[identity]] = f"Invalid provider response: {exc}"
            continue
        frames.append(group)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=md.PRICE_COLUMNS)


def fetch_history(
    existing, requests, target, fetch, sources, pending_rebuild, force_full, emit, provider, unavailable_error=None
):
    """Validate one provider's response using the shared history-rebuild rules."""
    target_ts = pd.Timestamp(target)
    rebuilt = set()
    provider_error = unavailable_error

    def fetch_once(batch, start, end):
        nonlocal provider_error
        if provider_error is None:
            try:
                return fetch(batch, start, end)
            except RuntimeError as exc:
                provider_error = str(exc)
                emit("warning", f"{provider} stopped: {provider_error}")
        return pd.DataFrame(columns=md.PRICE_COLUMNS), provider_error

    source_changes = {
        (kind, symbol)
        for symbol, kind in requests.values()
        if f"{kind}:{symbol}" in sources and sources[f"{kind}:{symbol}"] != provider
    }
    switching_rows = pd.MultiIndex.from_frame(existing[["series_type", "symbol"]]).isin(source_changes)
    if existing.loc[switching_rows, "date"].gt(target_ts).any():
        raise RuntimeError("A provider change must cover the latest stored session to preserve its adjustment basis")
    additions = []
    errors = {}
    retained = []
    # Group by each instrument's own coverage, not a global maximum.
    groups = {}
    coverage = existing.groupby(["series_type", "symbol"]).date.agg(["min", "max"])
    for ticker, (symbol, kind) in requests.items():
        bounds = coverage.loc[(kind, symbol)] if (kind, symbol) in coverage.index else None
        if force_full or bounds is None or symbol in pending_rebuild or (kind, symbol) in source_changes:
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
            new, error = fetch_once(batch, start, target)
            if new.empty:
                errors.update({ticker: error or "no observations" for ticker in batch})
                retained.extend(batch)
                continue
            new = validate_response(new, batch, target, errors)
            corrections = revision_needs_full_history(existing, new)
            returned_identities = set(zip(new.series_type, new.symbol))
            # Even identical overlap prices cannot prove two providers use
            # the same adjustment convention across the entire history.
            corrections.update(source_changes & returned_identities)
            corrections.update(
                ("equity", symbol) for symbol in pending_rebuild if ("equity", symbol) in returned_identities
            )
            if corrections:
                corrected = {ticker: identity for ticker, identity in batch.items() if identity[::-1] in corrections}
                oldest = existing[
                    pd.MultiIndex.from_frame(existing[["series_type", "symbol"]]).isin(corrections)
                ].date.min()
                if pd.Timestamp(start) <= oldest:
                    # The original request already covers all stored history.
                    full, full_error = new.copy(), error
                else:
                    full, full_error = fetch_once(
                        corrected, str(min(oldest, target_ts - pd.DateOffset(years=10)).date()), target
                    )
                full = validate_response(full, corrected, target, errors) if not full.empty else full
                successful = set()
                previous_groups = {
                    identity: frame
                    for identity, frame in existing.loc[
                        pd.MultiIndex.from_frame(existing[["series_type", "symbol"]]).isin(corrections)
                        & existing.date.le(target_ts)
                    ].groupby(["series_type", "symbol"])
                }
                replacement_groups = {identity: frame for identity, frame in full.groupby(["series_type", "symbol"])}
                for identity in corrections:
                    kind, symbol = identity
                    previous = previous_groups.get(identity, pd.DataFrame(columns=md.PRICE_COLUMNS))
                    replacement = replacement_groups.get(identity, pd.DataFrame(columns=md.PRICE_COLUMNS))
                    # A partial upstream response must not create an adjustment seam.
                    if history_is_complete(previous, replacement):
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
                    errors[symbol] = full_error or "adjusted-history correction incomplete; previous history retained"
            returned = set(zip(new.symbol, new.series_type))
            for ticker, identity in batch.items():
                if identity not in returned:
                    errors.setdefault(ticker, "no usable observations returned")
                    retained.append(ticker)
                else:
                    symbol, kind = identity
                    observations = new[new.symbol.eq(symbol) & new.series_type.eq(kind)]
                    if kind != "index_price":
                        observations = observations[observations.Volume.gt(0)]
                    latest = observations.date.max()
                    if pd.isna(latest) or latest < target_ts:
                        available = str(latest.date()) if pd.notna(latest) else "none"
                        errors.setdefault(ticker, f"no tradable {target} observation; latest returned: {available}")
            if not new.empty:
                additions.append(new)
    errors.update(getattr(fetch, "errors", {}))
    new = pd.concat(additions, ignore_index=True) if additions else pd.DataFrame(columns=md.PRICE_COLUMNS)
    return new, errors, retained, rebuilt


def refresh_prices(
    target_date=None,
    force_full=False,
    emit=lambda level, msg: print(msg),
    fetch=None,
    only_series=None,
    require_coverage=False,
    provider="dhan",
    fallback_fetch=None,
):
    unavailable_error = None
    if fetch is None:
        if provider == "dhan":
            from scripts.dhan_market_data import DhanDownloader

            try:
                fetch = DhanDownloader()
            except RuntimeError as exc:
                unavailable_error = str(exc)
                emit("warning", f"Dhan unavailable: {unavailable_error}")
        elif provider == "yahoo":
            fetch = download
        else:
            raise ValueError(f"Unknown market-data provider: {provider}")
    provider = getattr(fetch, "provider", provider if fetch is None else "yahoo")
    adjustment_policy = getattr(fetch, "adjustment_policy", "yahoo_auto_adjust")
    target = target_date or completed_session()
    target_ts = pd.Timestamp(target)
    with md.publisher_lock():
        snapshot = md.load_snapshot()
        existing = snapshot.prices
        sources = dict(snapshot.price_metadata.get("series_sources", {}))
        policies = dict(snapshot.price_metadata.get("series_adjustment_policies", {}))
        for kind, symbol in existing[["series_type", "symbol"]].drop_duplicates().itertuples(index=False):
            if kind != "cash_proxy":
                sources.setdefault(f"{kind}:{symbol}", "yahoo")
                policies.setdefault(f"{kind}:{symbol}", "yahoo_auto_adjust")
        pending_rebuild = set(existing.attrs.get("history_rebuild_required", []))
        current = snapshot.symbols(as_of=target)
        requests = market_requests(snapshot, target, only_series)
        new, errors, retained, rebuilt = fetch_history(
            existing, requests, target, fetch, sources, pending_rebuild, force_full, emit, provider, unavailable_error
        )
        for kind, symbol in new[["series_type", "symbol"]].drop_duplicates().itertuples(index=False):
            sources[f"{kind}:{symbol}"] = provider
            policies[f"{kind}:{symbol}"] = adjustment_policy
        merged = merge_observations(existing, new)
        provider_diagnostics = {provider: dict(errors)}
        fallback_requests = {}
        fallback_accepted = []
        if provider == "dhan":
            fallback_requests = missing_requested_series(merged, requests, target)
            if fallback_requests:
                emit("info", f"Trying Yahoo fallback for {len(fallback_requests)} unresolved series")
                fallback, fallback_errors, _, fallback_rebuilt = fetch_history(
                    merged,
                    fallback_requests,
                    target,
                    fallback_fetch or download,
                    sources,
                    pending_rebuild - rebuilt,
                    force_full,
                    emit,
                    "yahoo",
                )
                provider_diagnostics["yahoo"] = dict(fallback_errors)
                # A stale fallback must not overwrite a newer accepted Dhan observation.
                before = usable_series_dates(merged, target)
                after = usable_series_dates(fallback, target)
                improved = {
                    identity for identity, day in after.items() if identity not in before or day > before[identity]
                }
                fallback = fallback[pd.MultiIndex.from_frame(fallback[["series_type", "symbol"]]).isin(improved)]
                fallback_accepted = sorted(
                    ticker for ticker, (symbol, kind) in fallback_requests.items() if (kind, symbol) in improved
                )
                for kind, symbol in improved:
                    sources[f"{kind}:{symbol}"] = "yahoo"
                    policies[f"{kind}:{symbol}"] = "yahoo_auto_adjust"
                rebuilt.update(fallback_rebuilt & {symbol for kind, symbol in improved})
                merged = merge_observations(merged, fallback)
                new = pd.concat([new, fallback], ignore_index=True).drop_duplicates(
                    ["series_type", "symbol", "date"], keep="last"
                )
                errors.update(fallback_errors)
                unresolved = missing_requested_series(merged, requests, target)
                for ticker, (symbol, kind) in requests.items():
                    if ticker not in unresolved:
                        errors.pop(ticker, None)
                        errors.pop(symbol, None)
                retained = [ticker for ticker in retained if ticker in unresolved]
        if new.empty:
            raise PublicationRejected(
                "Shared refresh returned no accepted observations; prior files retained",
                price_coverage(existing, current, target),
                errors,
                provider_diagnostics,
            )
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
        merged, removed, repairs = prepare_publication(merged)
        merged = md.normalize_prices(merged)
        # Embed actual coverage, not just the requested date, in this same file.
        coverage_report = price_coverage(merged, current, target)
        if require_coverage:
            try:
                validate_publication(coverage_report, target)
            except RuntimeError as exc:
                raise PublicationRejected(str(exc), coverage_report, errors, provider_diagnostics) from exc
        missing = coverage_report["missing_target_symbols"]
        report = {
            "target_date": target,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "missing_target_symbols": missing,
            "errors": errors,
            "retained_series": sorted(set(retained)),
            "primary_provider": provider,
            "provider_diagnostics": provider_diagnostics,
            "fallback_requested_series": sorted(fallback_requests),
            "fallback_accepted_series": fallback_accepted,
            "missing_target_series": sorted(missing_requested_series(merged, requests, target)),
            "adjustment_policy": "per_series; see series_adjustment_policies",
            "series_sources": sources,
            "series_adjustment_policies": policies,
            "source": "shared publisher",
            "constituent_revision": snapshot.constituent_revision,
            "history_rebuild_required": sorted(pending_rebuild - rebuilt),
            "cleanup_removed_holiday_rows": len(removed),
            "cleanup_verified_repairs": repairs,
        }
        report.update(coverage_report)
        report["equity_observation_policy"] = observation_diagnostics(merged)
        diagnostics = report["equity_observation_policy"]
        emit(
            "info",
            f"Retained {diagnostics['excluded_rows']:,} uncertain equity prices for review; "
            f"{diagnostics['nonflat_zero_volume_rows']:,} zero-volume candles are non-flat and need source review",
        )
        report.update(cash_metadata)
        revision = md.write_source(merged, md.PRICE_PATH, report, expected_revision=snapshot.price_revision)
        emit(
            "warning" if report["publication_status"] == "partial" else "success",
            f"Published price revision {revision[:12]}; "
            f"{coverage_report['target_session_symbol_count']}/{len(current)} constituents cover {target}; "
            f"{len(coverage_report['missing_target_benchmarks'])} benchmarks lack target-session prices",
        )
        return report | {"revision": revision}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--target-date")
    parser.add_argument("--check-coverage", action="store_true", help="Check accepted publication without downloading")
    parser.add_argument("--require-coverage", action="store_true", help="Validate target coverage before publishing")
    parser.add_argument("--series-type", action="append", choices=["equity", "index_price", "etf_adjusted"])
    parser.add_argument("--provider", choices=["dhan", "yahoo"], default="dhan")
    args = parser.parse_args()
    if args.check_coverage:
        print(check_publication(args.target_date))
    else:
        diagnostic_path = md.ROOT / "outputs" / "manual_price_refresh.json"
        diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            report = refresh_prices(
                args.target_date,
                args.full,
                only_series=args.series_type,
                require_coverage=args.require_coverage,
                provider=args.provider,
            )
        except RuntimeError as exc:
            report = {"status": "failed", "target_date": args.target_date, "error": str(exc)}
            if isinstance(exc, PublicationRejected):
                report.update(
                    candidate_coverage=exc.candidate_coverage,
                    provider_errors=exc.provider_errors,
                    provider_diagnostics=exc.provider_diagnostics,
                )
            diagnostic_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            print(f"Refresh failed: {exc}. Diagnostics: {diagnostic_path}")
            sys.exit(1)
        diagnostic_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Refresh diagnostics: {diagnostic_path}")
