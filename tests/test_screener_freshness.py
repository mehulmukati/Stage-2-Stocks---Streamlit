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
