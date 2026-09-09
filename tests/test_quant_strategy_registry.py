import numpy as np
import pandas as pd

from ha_ema_engine import HAEMAStrategyConfig
from quant_strategy import SIGNAL_COLUMNS
from quant_strategy_registry import HAEMAAdapter, IchimokuAdapter, build_strategy


def _prices(periods: int = 500) -> pd.DataFrame:
    index = pd.bdate_range("2020-01-01", periods=periods)
    close = pd.Series(np.linspace(100, 500, periods), index=index)
    return pd.DataFrame(
        {"Open": close * 0.999, "High": close * 1.01, "Low": close * 0.99, "Close": close, "Volume": 2_000_000},
        index=index,
    )


def _ha_event_prices() -> pd.DataFrame:
    weekly_ohlc = [
        (100.0, 110.0, 90.0, 100.0),
        (100.0, 130.0, 100.0, 120.0),
        (120.0, 125.0, 100.0, 110.0),
        (100.0, 110.0, 80.0, 90.0),
        (85.0, 95.0, 80.0, 90.0),
    ]
    frames = []
    for week, (open_, high, low, close) in enumerate(weekly_ohlc):
        dates = pd.bdate_range(pd.Timestamp("2025-01-06") + pd.Timedelta(weeks=week), periods=5)
        path = pd.Series(np.linspace(open_, close, 5), index=dates)
        frames.append(pd.DataFrame({"Open": path, "High": high, "Low": low, "Close": path, "Volume": 200_000.0}))
    return pd.concat(frames)


def test_all_builtin_adapters_honor_the_shared_contract():
    for adapter in (HAEMAAdapter(), IchimokuAdapter(timeframe="Weekly")):
        signals = adapter.compute(_prices())
        assert tuple(signals.columns) == SIGNAL_COLUMNS
        assert signals["Decision_Date"].is_monotonic_increasing
        executable = signals["Execution_Date"].notna()
        assert (signals.loc[executable, "Execution_Date"] > signals.loc[executable, "Decision_Date"]).all()


def test_registry_rejects_unknown_methods():
    try:
        build_strategy("future_method")
    except ValueError as exc:
        assert "Unknown quant strategy" in str(exc)
    else:
        raise AssertionError("Unknown strategies must fail explicitly")


def test_strategy_metadata_declares_its_actual_data_dependencies():
    ha = HAEMAAdapter()
    ichimoku = IchimokuAdapter(timeframe="Weekly")

    assert ha.definition.uses_liquidity_filter is True
    assert "Volume" in ha.definition.required_price_columns
    assert ha.definition.warmup_periods == 52
    assert ichimoku.definition.uses_liquidity_filter is False
    assert "Volume" not in ichimoku.definition.required_price_columns
    assert ichimoku.definition.warmup_periods == 78


def test_ha_batch_adapter_matches_single_symbol_event_contract():
    adapter = HAEMAAdapter(
        HAEMAStrategyConfig(
            ema_fast=2,
            ema_slow=3,
            average_volume_length=1,
            minimum_average_volume=100,
            return_length=1,
            minimum_return_pct=0,
        )
    )
    source = _ha_event_prices()

    single = adapter.compute(source)
    batch = adapter.compute_many({"TEST": source})["TEST"]

    pd.testing.assert_frame_equal(batch, single)
