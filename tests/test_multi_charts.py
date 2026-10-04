"""Batch chart input, analysis reuse, and all-pages PDF behavior."""

from io import BytesIO

from pypdf import PdfReader

import multi_charts

from .conftest import make_ohlcv


def test_ticker_inputs_preserve_first_occurrence_order():
    uploaded = b"Symbol\nRELIANCE\nTCS\nRELIANCE\n"
    assert multi_charts.parse_tickers(uploaded, "hdfcbank, TCS\nINFY") == ["RELIANCE", "TCS", "HDFCBANK", "INFY"]


def test_analysis_uses_full_history_for_short_display_window(monkeypatch):
    source = make_ohlcv(320, close=[100 + i * 0.2 for i in range(320)])
    monkeypatch.setattr(multi_charts.data, "fetch_chart_data", lambda ticker: source)
    multi_charts.analyze.clear()
    record = multi_charts.analyze("TEST", "Stage 2 Phase")
    assert "error" not in record
    assert len(record["data"]) == 320
    assert record["state"]["score"] >= 0
    fig = multi_charts.tile_figure(record, "6 months")
    assert fig.axes[0].has_data()
    multi_charts.plt.close(fig)


def test_each_analysis_draws_from_the_shared_history(monkeypatch):
    source = make_ohlcv(400, close=[100 + i * 0.15 for i in range(400)])
    monkeypatch.setattr(multi_charts.data, "fetch_chart_data", lambda ticker: source)
    multi_charts.analyze.clear()
    for analysis in multi_charts.ANALYSES:
        record = multi_charts.analyze("TEST", analysis)
        assert "error" not in record, (analysis, record)
        fig = multi_charts.tile_figure(record, "1 year")
        assert fig.axes[0].has_data()
        multi_charts.plt.close(fig)


def test_log_price_axis_uses_plain_labels_for_major_and_minor_ticks(monkeypatch):
    source = make_ohlcv(400, close=[2500 + i * 4 for i in range(400)])
    monkeypatch.setattr(multi_charts.data, "fetch_chart_data", lambda ticker: source)
    multi_charts.analyze.clear()
    record = multi_charts.analyze("TEST", "Ichimoku")
    fig = multi_charts.tile_figure(record, "1 year")
    axis = fig.axes[0].yaxis
    assert axis.get_major_formatter()(4000) == "4,000"
    assert axis.get_minor_formatter()(600) == "600"
    fig.canvas.draw()
    labels = [label.get_text() for label in fig.axes[0].get_yticklabels(which="both") if label.get_text()]
    assert labels
    assert all("10^" not in label and "×" not in label for label in labels)
    multi_charts.plt.close(fig)


def test_all_pdf_pages_keep_grid_and_error_tiles():
    records = [
        {"ticker": f"TICKER{i}", "analysis": "Stage 2 Phase", "error": "No price history available"} for i in range(5)
    ]
    pdf = multi_charts.build_pdf(records, "Stage 2 Phase", "1 year", rows=2, columns=2)
    reader = PdfReader(BytesIO(pdf))
    assert len(reader.pages) == 2
    assert "TICKER0" in reader.pages[0].extract_text()
    assert "TICKER4" in reader.pages[1].extract_text()
    assert reader.pages[0].mediabox == reader.pages[1].mediabox


def test_full_view_links_are_text_links_and_encode_the_symbol():
    assert multi_charts.full_view_link("Stage 2 Phase", "M&M") == (
        "[Open full Stage 2 Phase view](?multi_view=phase&multi_ticker=M%26M)"
    )
    assert "multi_view=ichimoku" in multi_charts.full_view_link("Ichimoku", "TCS")
    assert "multi_view=ha-ema" in multi_charts.full_view_link("Heikin-Ashi + EMA", "TCS")


def test_page_items_keep_edges_and_current_page_visible():
    assert multi_charts._page_items(1, 3) == [1, 2, 3]
    assert multi_charts._page_items(1, 20) == [1, 2, 3, 4, None, 20]
    assert multi_charts._page_items(10, 20) == [1, None, 9, 10, 11, None, 20]
    assert multi_charts._page_items(20, 20) == [1, None, 17, 18, 19, 20]
