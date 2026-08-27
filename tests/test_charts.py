import math

from charts import ICHIMOKU_COLORS, ichimoku_chart_figure
from ichimoku_engine import compute_ichimoku

from .conftest import make_ohlcv


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
