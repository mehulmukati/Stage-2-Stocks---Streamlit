"""An unchanged price revision must not keep scores from an older calculation."""

from types import SimpleNamespace

import pandas as pd
import pytest

import backtest_provenance as provenance
import data


def test_momentum_memory_cache_is_rebuilt_when_calculation_changes(monkeypatch):
    snapshot = SimpleNamespace(revisions=("prices", "membership"), constituents=lambda: {"X": ["A"]})
    monkeypatch.setattr(data.shared_market, "load_snapshot", lambda *_: snapshot)
    revision = ["old"]
    monkeypatch.setattr(data, "calculation_revision", lambda: revision[0])
    calls = []

    def score(*_):
        calls.append(revision[0])
        return pd.DataFrame({"model": [revision[0]]})

    monkeypatch.setattr(data, "_load_and_score", score)
    monkeypatch.setattr(data, "_score_cache", {"momentum": {"data": None}})
    first, _, _ = data.resolve_screener_data(True, as_of_date="2026-10-20")
    same, _, source = data.resolve_screener_data(True, as_of_date="2026-10-20")
    assert first is same and source == "memory"
    revision[0] = "new"
    fresh, _, source = data.resolve_screener_data(True, as_of_date="2026-10-20")
    assert fresh.model.iloc[0] == "new" and source == "shared"
    assert calls == ["old", "new"]


@pytest.mark.parametrize("paired", [False, True])
def test_backtest_result_rejects_old_calculation_with_unchanged_prices(tmp_path, monkeypatch, paired):
    monkeypatch.setattr(provenance, "ROOT", tmp_path)
    names = ["backtest_engine.py", "backtest_accounting.py", "momentum_engine.py", "research_acceptance.py"]
    for name in names:
        (tmp_path / name).write_text("before")
    manifest = {"code_and_registry_sha256": {name: provenance.sha256(tmp_path / name) for name in names}}
    result = (
        {"run_manifests": {"Classic": manifest, "Displacement": manifest}} if paired else {"run_manifest": manifest}
    )
    assert provenance.result_calculations_current(result)
    (tmp_path / "momentum_engine.py").write_text("repaired lower circuit bounds")
    assert not provenance.result_calculations_current(result)
    assert not provenance.result_calculations_current({})


def test_backtest_ui_discards_a_stale_calculation_before_display(monkeypatch):
    from streamlit.testing.v1 import AppTest

    import app_backtest
    import market_data
    from backtest_accounting import ACCOUNTING_VERSION

    monkeypatch.setattr(app_backtest, "_poll_job", lambda *_: False)
    monkeypatch.setattr(market_data, "source_revisions", lambda: ("prices", "members"))
    script = "import streamlit as st\n" "import app_backtest\n" "app_backtest.backtest_results({})\n"
    at = AppTest.from_string(script)
    at.session_state["backtest_cached_result"] = dict(
        source_revisions=("prices", "members"),
        equity_policy_version=market_data.EQUITY_POLICY_VERSION,
        accounting_version=ACCOUNTING_VERSION,
        run_manifests={"Classic": {"code_and_registry_sha256": {"momentum_engine.py": "old"}}},
    )
    at.run(timeout=30)
    assert not at.exception
    assert "backtest_cached_result" not in at.session_state
    assert "calculation rules changed" in at.info[0].value
