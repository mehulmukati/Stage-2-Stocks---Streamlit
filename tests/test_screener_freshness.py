import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from data import screener_freshness


def test_actual_price_date_is_independent_of_requested_session():
    frame = pd.DataFrame({"Price Date": ["2026-10-16", "2026-10-16"]})
    result = screener_freshness(frame, "2026-10-19")
    assert result["latest_price_date"] == "2026-10-16"
    assert result["current_count"] == 0
    assert result["outdated_count"] == 2


def test_mixed_and_missing_dates_remain_visible_as_incomplete():
    frame = pd.DataFrame({"Price Date": ["2026-10-19", "2026-10-16", None]})
    result = screener_freshness(frame, "2026-10-19")
    assert result["latest_price_date"] == "2026-10-19"
    assert result["oldest_price_date"] == "2026-10-16"
    assert result["current_count"] == 1
    assert result["outdated_count"] == 2
    assert screener_freshness(pd.DataFrame(index=[0]), "2026-10-19")["latest_price_date"] is None


@pytest.mark.parametrize("source", ["shared", "memory"])
@pytest.mark.parametrize("price_date", ["2026-10-16", "2026-10-19"])
def test_banner_warns_on_actual_price_lag_even_from_memory(source, price_date):
    at = AppTest.from_string(
        "import pandas as pd\n"
        "from app import _render_source_banner\n"
        f"frame = pd.DataFrame({{'Price Date': ['{price_date}']}})\n"
        f"_render_source_banner('{source}', '2026-10-19', frame=frame)\n"
    ).run(timeout=30)
    assert not at.exception
    assert f"Latest price date: {price_date}" in at.caption[0].value
    assert "Target trading session: 2026-10-19" in at.caption[0].value
    if price_date == "2026-10-16":
        assert len(at.warning) == 1
        assert "0/1" in at.warning[0].value
    else:
        assert len(at.warning) == 0


def test_prior_session_cache_is_checked_against_new_target():
    at = AppTest.from_string(
        "import pandas as pd\n"
        "from app import _render_source_banner\n"
        "frame = pd.DataFrame({'Price Date': ['2026-10-19']})\n"
        "_render_source_banner('memory', '2026-10-19', frame=frame, target_date='2026-10-21')\n"
    ).run(timeout=30)
    assert not at.exception
    assert "Target trading session: 2026-10-21" in at.caption[0].value
    assert "Price data is incomplete for 2026-10-21" in at.warning[0].value


@pytest.mark.parametrize("kind", ["stage2", "momentum"])
def test_new_publication_recalculates_once_without_clicking_run_or_fetching_prices(monkeypatch, kind):
    import streamlit as st
    import yfinance as yf

    import app

    calls = []
    monkeypatch.setattr(app.shared_market, "source_revisions", lambda: ("new-prices", "members"))
    monkeypatch.setattr(app, "_score_cache", {kind: {"date": None, "data": None}})
    monkeypatch.setattr(app, "st_autorefresh", lambda **kw: None)
    monkeypatch.setattr(yf, "download", lambda *a, **kw: pytest.fail("UI contacted the price provider"))

    def poll(actual_kind, worker, params):
        assert st.session_state.pop(f"{kind}_run_triggered")
        target = st.session_state.get("requested_target", "2026-10-19")
        if kind == "momentum":
            assert params == {"as_of_date": target}
        calls.append(actual_kind)
        frame = pd.DataFrame({"Price Date": [target], "Stage 2 Days": [1], "Stage 2 Since": [target]})
        frame.attrs["source_revisions"] = ("new-prices", "members")
        frame.attrs["equity_policy_version"] = app.shared_market.EQUITY_POLICY_VERSION
        st.session_state[f"{kind}_cached_result"] = dict(df=frame, cache_date=target, source="shared")
        return True

    monkeypatch.setattr(app, "_poll_job", poll)
    at = AppTest.from_string(
        "import streamlit as st\nimport pandas as pd\nimport app\n"
        f"kind = '{kind}'\n"
        "target = st.session_state.get('requested_target', '2026-10-19')\n"
        "key = kind + '_cached_result'\n"
        "if key not in st.session_state:\n"
        "    frame = pd.DataFrame({'Price Date': ['2026-10-16']})\n"
        "    frame.attrs['source_revisions'] = ('old-prices', 'members')\n"
        "    st.session_state[key] = dict(df=frame, cache_date='2026-10-19')\n"
        "if not app._invalidate_legacy_score_result(kind, st.session_state[key], target):\n"
        "    st.success('Current results: ' + st.session_state[key]['df']['Price Date'].iloc[0])\n"
    ).run(timeout=30)
    assert not at.exception
    assert calls == [kind]
    at.run()
    assert not at.exception
    assert at.success[0].value == "Current results: 2026-10-19"
    assert calls == [kind]
    at.session_state["requested_target"] = "2026-10-21"
    at.run()
    assert not at.exception
    assert calls == [kind, kind]


def test_failed_automatic_recalculation_does_not_loop_on_same_revision(monkeypatch):
    import app

    calls = []
    monkeypatch.setattr(app.shared_market, "source_revisions", lambda: ("prices", "members"))
    monkeypatch.setattr(app, "_poll_job", lambda *args: calls.append(args))
    monkeypatch.setattr(app, "st_autorefresh", lambda **kw: None)
    at = AppTest.from_string(
        "from app import _start_score_refresh\n_start_score_refresh('stage2', '2026-10-19')\n"
    ).run(timeout=30)
    at.run()
    assert not at.exception
    assert len(calls) == 1
    assert "Click Run to retry" in at.warning[0].value
