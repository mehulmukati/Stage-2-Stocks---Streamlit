"""Circuit directions checked against known percentage changes, not each other."""

import numpy as np
import pandas as pd
import pytest

from momentum_engine import _count_circuits, precompute_metrics


@pytest.mark.parametrize("direction", [-1, 1])
def test_both_directions_count_only_returns_inside_each_circuit_band(direction):
    days = pd.bdate_range("2024-01-02", periods=252)
    returns = np.full(251, 0.001)
    # Three in-band observations and four out-of-band observations.
    changes = [4.95, 10.05, 19.95, 4.8, 5.2, 9.8, 20.2]
    returns[-len(changes) :] = np.array(changes) * direction / 100
    close = np.r_[100.0, 100.0 * np.cumprod(1 + returns)]
    bars = pd.DataFrame({c: close for c in ["Open", "High", "Low", "Close"]}, index=days)
    bars["Volume"] = 1000000
    assert _count_circuits(bars, calendar=days) == 3
    assert precompute_metrics(bars, calendar=days).iloc[-1].Circuit_Count == 3
