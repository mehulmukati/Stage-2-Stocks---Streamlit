"""Read-only acceptance audit against the real shared files; writes an audit CSV/report."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd

import data
import data_backtest as db
import market_data as md
from backtest_engine import _precompute_all_metrics, rank_universe_at_date
from momentum_engine import _calculate_avg_sharpe
from momentum_ranking import rank_momentum_frame


def verify(as_of, output=None):
    snapshot = md.load_snapshot(as_of)
    constituents = snapshot.constituents()
    screen = data._load_and_score(constituents, True, as_of_date=as_of, snapshot=snapshot)
    loaded = db.load_ohlcv_for_backtest(snapshot=snapshot)
    quant = db.load_quant_ohlcv_snapshot(snapshot=snapshot)
    assert screen.attrs["source_revisions"] == loaded.source_revisions == quant.source_revisions == snapshot.revisions
    settings = dict(
        min_history_days=252,
        minimum_median_volume=100_000,
        max_stale_sessions=3,
        min_annual_return=7,
        pct_from_52w_high=25,
        max_circuits=18,
        close_above_200dma=True,
        pos_days_3m_min=45,
        pos_days_6m_min=45,
        pos_days_12m_min=45,
    )
    method = "Average of 3/6/9/12 months"
    ranked = rank_momentum_frame(screen, method, **settings)
    calendar = pd.bdate_range(snapshot.prices.date.min(), as_of)
    calendar = calendar[~calendar.strftime("%Y-%m-%d").isin(data.load_nse_holidays())]
    prices = {symbol: loaded.symbol_data[symbol] for symbol in snapshot.symbols() if symbol in loaded.symbol_data}
    fast = _precompute_all_metrics(prices)
    fast_rank = rank_universe_at_date(
        prices,
        pd.Timestamp(as_of),
        method,
        snapshot.symbols(),
        precomputed=fast,
        trading_calendar=calendar,
        **settings,
    )
    scalar_rank = rank_universe_at_date(
        prices, pd.Timestamp(as_of), method, snapshot.symbols(), trading_calendar=calendar, **settings
    )
    assert ranked.Symbol.tolist() == fast_rank == scalar_rank, "Eligible symbols / rank ordering mismatch"
    differences = []
    for _, row in ranked.iterrows():
        backtest_row = fast[row.Symbol].iloc[-1]
        score = _calculate_avg_sharpe(backtest_row, method)
        assert score == row.Avg_Sharpe, f"Score mismatch for {row.Symbol}"
        assert backtest_row.Close == row.Close, f"Price mismatch for {row.Symbol}"
        differences.append(
            dict(
                Symbol=row.Symbol,
                Rank=len(differences) + 1,
                Close=row.Close,
                Score=score,
                Price_Date=row["Price Date"],
                Price_Revision=snapshot.price_revision,
                Constituent_Revision=snapshot.constituent_revision,
            )
        )
    report = dict(
        as_of=as_of,
        settings=settings,
        source_revisions=snapshot.revisions,
        eligible_count=len(ranked),
        universe_count=len(snapshot.symbols()),
        passed=True,
        top15=ranked.Symbol.head(15).tolist(),
        selected={
            symbol: next((i + 1 for i, s in enumerate(fast_rank) if s == symbol), None)
            for symbol in ["LOTUSDEV", "UNIMECH", "CPPLUS", "SHREEJISPG"]
        },
    )
    if output:
        output = Path(output)
        output.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(differences).to_csv(output / "rank_alignment.csv", index=False)
        (output / "rank_alignment.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output", default="outputs/market_data_acceptance")
    args = parser.parse_args()
    verify(args.date, args.output)
