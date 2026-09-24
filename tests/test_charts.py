import math

import pandas as pd

from charts import (
    HA_EMA_COLORS,
    ICHIMOKU_COLORS,
    ha_ema_chart_figure,
    ha_ema_equity_figure,
    ichimoku_chart_figure,
    ichimoku_strategy_comparison_figure,
    phase_chart_figure,
    stage2_breadth_count_figure,
    stage2_breadth_percent_figure,
)
from ha_ema_engine import HAEMAStrategyConfig, compute_ha_ema_signals
from ichimoku_engine import compute_ichimoku
from stage2_engine import compute_rolling_stage2
from strategy_replay import replay_single_stock

from .conftest import make_ohlcv


def test_stage2_breadth_figures_have_all_phase_layers():
    daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "Eligible": [10, 10],
            "Not Stage 2": [4, 3],
            "Early": [2, 2],
            "Likely": [2, 3],
            "Strong": [2, 2],
            "Stage 2": [6, 7],
            "Stage 2 %": [60.0, 70.0],
            "Strong %": [20.0, 20.0],
            "Average Score": [3.5, 3.8],
        }
    )
    for figure in (stage2_breadth_count_figure(daily), stage2_breadth_percent_figure(daily)):
        assert [trace.name for trace in figure.data] == ["Strong", "Likely", "Early", "Not Stage 2"]

    overlay = stage2_breadth_count_figure(daily, {"Nifty 50": pd.Series([22000, 22100], index=daily["date"])})
    assert overlay.data[-1].yaxis == "y2"
    assert overlay.layout.yaxis2.type == "log"


def test_phase_chart_displays_stage_duration_summary():
    rolled = compute_rolling_stage2(make_ohlcv(300, close=[100 + i * 0.2 for i in range(300)]))
    figure = phase_chart_figure(rolled, "TEST")
    assert figure.layout.annotations
    assert any("d" in annotation.text for annotation in figure.layout.annotations)


def test_phase_chart_score_matches_daily_indicators_on_independent_axis():
    rolled = compute_rolling_stage2(make_ohlcv(300, close=[100 + i * 0.2 for i in range(300)]))
    valid = rolled.dropna(subset=["MA200"])
    for use_log_scale in (True, False):
        figure = phase_chart_figure(rolled, "TEST", use_log_scale=use_log_scale)
        score = next(trace for trace in figure.data if trace.yaxis == "y2")
        assert list(score.x) == list(valid.index)
        assert list(score.y) == list(valid["Score"])
        assert figure.layout.yaxis.type == ("log" if use_log_scale else "linear")
        assert figure.layout.yaxis2.type == "linear"
        assert tuple(figure.layout.yaxis2.range) == (0, 8)
        assert figure.layout.yaxis2.dtick == 1


def test_ichimoku_chart_contains_core_traces_and_projection():
    close = [100.0 + i * 0.2 for i in range(100)]
    data = compute_ichimoku(make_ohlcv(100, close=close))
    figure = ichimoku_chart_figure(data, "TEST")
    names = {trace.name for trace in figure.data}

    assert {"TEST", "Tenkan (9)", "Kijun (26)", "Senkou A", "Senkou B", "Chikou (−26)"}.issubset(names)
    assert {"Bullish TK cross", "Bearish TK cross"}.issubset(names)
    assert max(figure.data[0].x) > data[~data["IsFuture"].astype(bool)].index.max()


def test_cloud_segments_use_regime_colors():
    data = compute_ichimoku(make_ohlcv(100, close=[100.0 + i * 0.2 for i in range(100)]))
    figure = ichimoku_chart_figure(data, "TEST")
    fill_colors = {trace.fillcolor for trace in figure.data if getattr(trace, "fill", None) == "tonexty"}
    assert ICHIMOKU_COLORS["bullish_cloud"] in fill_colors


def test_chart_controls_hide_optional_traces_and_set_linear_axis():
    data = compute_ichimoku(make_ohlcv(100, close=[100.0 + i * 0.2 for i in range(100)]))
    figure = ichimoku_chart_figure(data, "TEST", use_log_scale=False, show_chikou=False, show_crossovers=False)
    names = {trace.name for trace in figure.data}
    assert "Chikou (−26)" not in names
    assert "Bullish TK cross" not in names
    assert figure.layout.yaxis.type == "linear"


def test_ichimoku_chart_colors_follow_light_and_dark_themes():
    data = compute_ichimoku(make_ohlcv(100, close=[100.0 + i * 0.2 for i in range(100)]))
    dark_figure = ichimoku_chart_figure(data, "TEST", theme="dark")
    light_figure = ichimoku_chart_figure(data, "TEST", theme="light")

    assert dark_figure.layout.plot_bgcolor == "#0f1420"
    assert dark_figure.layout.font.color == "#8b93a7"
    assert light_figure.layout.plot_bgcolor == "#ffffff"
    assert light_figure.layout.font.color == "#475569"

    dark_candles = next(trace for trace in dark_figure.data if trace.type == "candlestick")
    light_candles = next(trace for trace in light_figure.data if trace.type == "candlestick")
    assert dark_candles.increasing.line.color == "#f8fafc"
    assert light_candles.increasing.line.color == "#0f766e"


def test_daily_chart_defaults_to_roughly_one_year_of_price_history():
    data = compute_ichimoku(make_ohlcv(400, close=[100.0 + i * 0.2 for i in range(400)]))
    figure = ichimoku_chart_figure(data, "TEST")
    observed = data[~data["IsFuture"].astype(bool)]
    assert figure.layout.xaxis.range[0] == observed.index[-260]
    assert figure.layout.xaxis.range[1] == data.index[-1]


def test_chart_title_identifies_weekly_timeframe():
    data = compute_ichimoku(make_ohlcv(400, close=[100.0 + i * 0.2 for i in range(400)]), timeframe="Weekly")
    figure = ichimoku_chart_figure(data, "TEST", timeframe="Weekly")
    assert "Weekly Ichimoku" in figure.layout.title.text


def test_log_chart_overlays_use_log_axis_coordinates():
    data = compute_ichimoku(make_ohlcv(100, close=[300.0 + i for i in range(100)]))
    figure = ichimoku_chart_figure(data, "TEST", use_log_scale=True)
    observed = data[~data["IsFuture"].astype(bool)]
    expected = {
        math.log10(float(observed["High"].max())),
        math.log10(float(observed["Low"].min())),
        math.log10(float(observed["Close"].iloc[-1])),
    }

    assert {annotation.y for annotation in figure.layout.annotations} == expected
    assert figure.layout.shapes[-1].y0 == float(observed["Close"].iloc[-1])
    assert figure.layout.shapes[-1].y1 == float(observed["Close"].iloc[-1])


def test_linear_chart_overlays_keep_raw_price_coordinates():
    data = compute_ichimoku(make_ohlcv(100, close=[300.0 + i for i in range(100)]))
    figure = ichimoku_chart_figure(data, "TEST", use_log_scale=False)
    observed = data[~data["IsFuture"].astype(bool)]
    expected = {
        float(observed["High"].max()),
        float(observed["Low"].min()),
        float(observed["Close"].iloc[-1]),
    }

    assert {annotation.y for annotation in figure.layout.annotations} == expected
    assert figure.layout.shapes[-1].y0 == float(observed["Close"].iloc[-1])
    assert figure.layout.shapes[-1].y1 == float(observed["Close"].iloc[-1])


def test_ichimoku_strategy_overlay_distinguishes_signal_and_next_open_execution():
    data = compute_ichimoku(make_ohlcv(100, close=[100.0 + i * 0.2 for i in range(100)]))
    observed = data[~data["IsFuture"].astype(bool)].copy()
    observed["Signal"] = ""
    observed["Signal_Reason"] = ""
    signal_date = observed.index[-3]
    execution_date = observed.index[-2]
    observed.loc[signal_date, ["Signal", "Signal_Reason"]] = ["BUY", "Test setup"]

    figure = ichimoku_chart_figure(data, "TEST", strategy_signals=observed, strategy_name="Balanced")
    signal = next(trace for trace in figure.data if trace.name == "Balanced BUY signal")
    execution = next(trace for trace in figure.data if trace.name == "Balanced BUY execution")

    assert list(signal.x) == [signal_date]
    assert list(execution.x) == [execution_date]
    assert list(execution.y) == [observed.at[execution_date, "Open"]]


def test_ichimoku_strategy_comparison_highlights_selected_curve():
    index = pd.bdate_range("2025-01-01", periods=3)
    comparison = pd.DataFrame(
        {
            "Aggressive": [100_000.0, 101_000.0, 102_000.0],
            "Balanced": [100_000.0, 102_000.0, 104_000.0],
            "Conservative": [100_000.0, 100_500.0, 101_000.0],
            "Custom": [100_000.0, 99_000.0, 103_000.0],
        },
        index=index,
    )
    figure = ichimoku_strategy_comparison_figure(comparison, selected_strategy="Balanced")
    widths = {trace.name: trace.line.width for trace in figure.data}

    assert widths["Balanced"] > widths["Aggressive"]
    assert figure.layout.yaxis.title.text == "Account value (₹)"


def test_ha_ema_chart_defaults_to_normal_candles_with_emas_and_signal_traces():
    source = make_ohlcv(300, close=[100.0 + i for i in range(300)])
    config = HAEMAStrategyConfig(minimum_return_pct=0.0)
    data = compute_ha_ema_signals(source, config)
    figure = ha_ema_chart_figure(data, "TEST")
    names = {trace.name for trace in figure.data}

    assert {
        "Normal OHLC",
        "EMA 10",
        "EMA 30",
        "BUY execution",
        "EXIT execution",
        "BUY signal close",
        "EXIT signal close",
    }.issubset(names)
    candles = next(trace for trace in figure.data if trace.name == "Normal OHLC")
    assert candles.type == "candlestick"
    assert list(candles.open) == list(data["Open"])
    assert "Actual close" not in names


def test_ha_ema_view_toggle_changes_only_candles_and_actual_close_reference():
    source = make_ohlcv(300, close=[100.0 + i for i in range(300)])
    data = compute_ha_ema_signals(source, HAEMAStrategyConfig(minimum_return_pct=0.0))
    data["Signal"] = ""
    data.loc[data.index[30], "Signal"] = "BUY"

    normal = ha_ema_chart_figure(data, "TEST", candle_style="Normal OHLC")
    heikin_ashi = ha_ema_chart_figure(data, "TEST", candle_style="Heikin-Ashi")
    normal_names = {trace.name for trace in normal.data}
    ha_names = {trace.name for trace in heikin_ashi.data}

    assert "Normal OHLC" in normal_names
    assert "Actual close" not in normal_names
    assert {"Heikin-Ashi", "Actual close"}.issubset(ha_names)
    ha_candles = next(trace for trace in heikin_ashi.data if trace.name == "Heikin-Ashi")
    assert list(ha_candles.open) == list(data["HA_Open"])

    for marker_name in ("BUY execution", "EXIT execution", "BUY signal close", "EXIT signal close"):
        normal_marker = next(trace for trace in normal.data if trace.name == marker_name)
        ha_marker = next(trace for trace in heikin_ashi.data if trace.name == marker_name)
        assert list(normal_marker.x) == list(ha_marker.x)
        assert list(normal_marker.y) == list(ha_marker.y)


def test_ha_ema_chart_shades_completed_winning_phase_from_execution_dates():
    source = make_ohlcv(300, close=[100.0 + i for i in range(300)])
    data = compute_ha_ema_signals(source, HAEMAStrategyConfig(minimum_return_pct=0.0))
    data["Signal"] = ""
    data.loc[data.index[-20], "Signal"] = "BUY"
    data.loc[data.index[-5], "Signal"] = "EXIT"
    figure = ha_ema_chart_figure(data, "TEST")

    phase = next(shape for shape in figure.layout.shapes if shape.fillcolor == HA_EMA_COLORS["winning_phase"])
    assert phase.x0 == data["Execution_Date"].iloc[-19]
    assert phase.x1 == data["Execution_Date"].iloc[-4]


def test_ha_ema_triangles_show_next_open_execution_not_signal_close():
    source = make_ohlcv(300, close=[100.0 + i for i in range(300)])
    data = compute_ha_ema_signals(source, HAEMAStrategyConfig(minimum_return_pct=0.0))
    data["Signal"] = ""
    signal_position = 30
    data.loc[data.index[signal_position], "Signal"] = "BUY"

    figure = ha_ema_chart_figure(data, "TEST")
    execution = next(trace for trace in figure.data if trace.name == "BUY execution")
    signal = next(trace for trace in figure.data if trace.name == "BUY signal close")

    assert execution.x[0] == data["Execution_Date"].iloc[signal_position + 1]
    assert execution.y[0] == data["Open"].iloc[signal_position + 1]
    assert signal.x[0] == data.index[signal_position]
    assert signal.y[0] == data["Close"].iloc[signal_position]


def test_ha_ema_equity_chart_includes_same_start_buy_and_hold_series():
    source = make_ohlcv(300, close=[100.0 + i for i in range(300)])
    data = compute_ha_ema_signals(source, HAEMAStrategyConfig(minimum_return_pct=0.0))
    data["Signal"] = ""
    data.loc[data.index[20], "Signal"] = "BUY"
    data.loc[data.index[40], "Signal"] = "EXIT"
    replay = replay_single_stock(data)
    figure = ha_ema_equity_figure(replay["equity"])

    assert {trace.name for trace in figure.data} == {
        "Pre-tax value",
        "After realised-tax estimate",
        "Buy & hold",
    }
