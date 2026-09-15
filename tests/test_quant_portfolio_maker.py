import ast
import importlib
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import apps.quant_portfolio_maker as qpm_navigation
import quant_portfolio_metrics
from apps.quant_portfolio_maker import ENHANCED_ICHIMOKU_PAGE, HA_EMA_PAGE, PAGE_LABELS, PORTFOLIO_LAB_PAGE
from apps.quant_portfolio_maker import app as quant_portfolio_maker

ROOT = Path(__file__).resolve().parents[1]


def test_quant_portfolio_maker_exposes_strategy_pages_and_portfolio_lab():
    assert PAGE_LABELS == (ENHANCED_ICHIMOKU_PAGE, HA_EMA_PAGE, PORTFOLIO_LAB_PAGE)
    assert PAGE_LABELS == ("☁️ Enhanced Ichimoku", "🕯️ HA + EMA Trend", "📊 Portfolio Lab")


def test_quant_portfolio_maker_import_does_not_import_root_app():
    source = (ROOT / "apps" / "quant_portfolio_maker" / "app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imported_from = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    }

    assert "app" not in imported_modules
    assert "app" not in imported_from


def test_quant_portfolio_maker_widget_namespace_is_isolated():
    assert quant_portfolio_maker._ICHI_STATE_PREFIX.startswith("qpm_")
    assert quant_portfolio_maker._ichi_key("choice") == "qpm_ichimoku_strategy_choice"


def test_direct_entrypoint_refreshes_a_stale_navigation_package(monkeypatch):
    monkeypatch.delattr(qpm_navigation, "PORTFOLIO_LAB_PAGE")
    refreshed = importlib.reload(quant_portfolio_maker)

    assert refreshed.PORTFOLIO_LAB_PAGE == "📊 Portfolio Lab"


def test_portfolio_ui_refreshes_stale_metrics_module(monkeypatch):
    portfolio_module = importlib.import_module("apps.quant_portfolio_maker.portfolio")
    monkeypatch.delattr(quant_portfolio_metrics, "slice_and_rebase_nav")
    monkeypatch.setattr(quant_portfolio_metrics, "QUANT_PORTFOLIO_METRICS_VERSION", 1)

    refreshed = importlib.reload(portfolio_module)

    assert callable(refreshed.slice_and_rebase_nav)
    assert refreshed.portfolio_metrics.QUANT_PORTFOLIO_METRICS_VERSION == 2


def test_root_app_keeps_basic_ichimoku_and_lazy_loads_quant_application():
    source = (ROOT / "app.py").read_text(encoding="utf-8")

    assert '"☁️ Ichimoku Chart"' in source
    assert "strategy_signals=strategy_signals" not in source
    assert "from apps.quant_portfolio_maker.app import render_quant_portfolio_maker" in source
    assert '"Quant-Portfolio-Maker": (' in source
    assert "QPM_PAGE_LABELS" in source
    assert "import ha_ema_engine" not in source
    assert "import ichimoku_strategy" not in source


def test_portfolio_lab_mounts_autorefresh_on_the_initial_run_click():
    source = (ROOT / "apps" / "quant_portfolio_maker" / "portfolio.py").read_text(encoding="utf-8")

    requested = source.index('run_requested = st.session_state.get("quant_portfolio_run_triggered", False)')
    autorefresh = source.index("if run_requested or")
    polling = source.index('if _poll_job("quant_portfolio"')

    assert requested < autorefresh < polling


def test_portfolio_lab_nav_defaults_to_log_scale_with_linear_toggle():
    source = (ROOT / "apps" / "quant_portfolio_maker" / "portfolio.py").read_text(encoding="utf-8")

    assert 'st.toggle("Logarithmic Y-axis", value=True' in source
    assert 'nav_figure.update_yaxes(type="log" if use_log_scale else "linear")' in source


def test_performance_slider_drives_rebased_chart_before_selected_period_stats():
    source = (ROOT / "apps" / "quant_portfolio_maker" / "portfolio.py").read_text(encoding="utf-8")

    slider = source.index('"Analysis period"')
    rebase = source.index("period, rebased = slice_and_rebase_nav")
    chart = source.index('title="Selected-period performance · base 100"')
    stats = source.index('st.markdown("#### Selected-period statistics")')

    assert slider < rebase < chart < stats
    assert "_compute_summary_stats(rebased)" in source


def test_relative_strength_ranking_control_is_scoped_to_ha_ema():
    source = (ROOT / "apps" / "quant_portfolio_maker" / "portfolio.py").read_text(encoding="utf-8")

    ichimoku_branch = source.index('if strategy_label == "Ichimoku":')
    ha_branch = source.index("    else:", ichimoku_branch)
    ranking_control = source.index('"Entry ranking"')

    assert ichimoku_branch < ha_branch < ranking_control
    assert 'ranking_method = "strategy_score"' in source[ichimoku_branch:ha_branch]


@pytest.mark.parametrize("page", PAGE_LABELS)
def test_main_app_can_open_each_quant_portfolio_maker_page(page):
    app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=30).run()
    assert not app.exception

    app.button(key=f"nav_{page}").click().run()

    assert not app.exception


def test_main_app_portfolio_lab_mounts_its_sidebar_controls():
    app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=30).run()

    app.button(key=f"nav_{PORTFOLIO_LAB_PAGE}").click().run()

    assert not app.exception
    assert app.selectbox(key="qpm_pf_strategy").value == "HA + EMA Trend"
    assert app.multiselect(key="qpm_pf_indices").value
    assert any(button.label == "Run Quant Portfolio" for button in app.button)


def test_quant_portfolio_maker_can_run_as_a_direct_streamlit_entrypoint():
    app = AppTest.from_file(str(ROOT / "apps" / "quant_portfolio_maker" / "app.py"), default_timeout=30).run()

    assert not app.exception
    assert app.radio(key="qpm_standalone_page").value == ENHANCED_ICHIMOKU_PAGE
