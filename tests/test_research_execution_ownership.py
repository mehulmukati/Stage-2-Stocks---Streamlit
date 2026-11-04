import pandas as pd
import pytest

from backtest_accounting import QuantityLedger
from research_execution import daily_close_quotes, intraday_quotes, resolve_quote
from research_ownership import merger_claim


def test_common_execution_cannot_buy_stale_or_sell_a_locked_claim():
    data = pd.DataFrame(
        {"Close": [100, 1, 130], "Volume": [20, 0, 25]},
        index=pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
    )
    cutoff = pd.Timestamp("2020-01-02 15:30", tz="Asia/Kolkata")
    daily = daily_close_quotes(data, "same primary closes")
    intraday = intraday_quotes(data, daily.index, "same primary closes")
    assert resolve_quote(daily, cutoff, "BUY", legally_tradable=True) is None
    assert resolve_quote(daily, cutoff, "SELL", legally_tradable=False) is None
    sell = resolve_quote(daily, cutoff, "SELL", legally_tradable=True)
    assert sell == resolve_quote(intraday, cutoff, "SELL", legally_tradable=True)
    assert sell.assumed and sell.price == 100 and sell.age == pd.Timedelta(days=1)
    cutoff += pd.Timedelta(days=1)
    assert resolve_quote(daily, cutoff, "BUY", legally_tradable=True).price == 130


@pytest.mark.parametrize("pari_passu", [True, False])
def test_locked_claim_preserves_cost_quantity_and_date(pari_passu):
    start, suspend, list_day = pd.to_datetime(["2018-12-27", "2018-12-28", "2019-01-16"])
    old = pd.Series({"Close": 300, "adjustment_factor": 0.8}, name=start)
    new = pd.DataFrame(
        {"Close": [30, 32], "Volume": [25, 30], "adjustment_factor": [0.5, 0.5]}, index=[suspend, list_day]
    )
    actions, marks = merger_claim("OLD", "NEW", suspend, list_day, 13.9, old, new, mark_from_listed_class=pari_passu)
    book = QuantityLedger(
        cash=12,
        lots={"OLD": [{"date": start, "price": 250, "shares": 2}]},
        marks={"OLD": 300},
        mark_dates={"OLD": start},
    )
    book.transfer(suspend, actions[0])
    book.mark(suspend, {"CLAIM:OLD": marks})
    ratio = 13.9 * 0.8 / 0.5
    assert book.quantities() == {"CLAIM:OLD": 2}
    assert book.nav() == pytest.approx(12 + 2 * (30 * ratio if pari_passu else 300))
    assert book.mark_dates["CLAIM:OLD"] == (suspend if pari_passu else start)
    assert marks.empty or (marks.Volume.eq(0) & ~marks.is_tradable).all()
    book.transfer(list_day, actions[1])
    book.mark(list_day, {"NEW": new})
    assert book.quantities()["NEW"] == pytest.approx(2 * ratio)
    assert book.lots["NEW"][0]["price"] * book.quantities()["NEW"] == pytest.approx(500)
    assert book.lots["NEW"][0]["date"] == start
    assert book.cash == 12 and book.fees == book.tax_paid == 0


def test_muhurat_close_uses_the_evening_cutoff_on_both_quote_adapters():
    day = pd.Timestamp("2019-10-27")
    frame = pd.DataFrame({"Close": [100.0, 110.0], "Volume": [10, 20]}, index=[pd.Timestamp("2019-10-25"), day])
    normal_close = day.tz_localize("Asia/Kolkata") + pd.Timedelta(hours=15, minutes=30)
    actual_cutoff = day.tz_localize("Asia/Kolkata") + pd.Timedelta(hours=19, minutes=15)
    daily = daily_close_quotes(frame, "exchange")
    intraday = intraday_quotes(frame, [pd.Timestamp("2019-10-25 15:30", tz="Asia/Kolkata"), actual_cutoff], "exchange")
    for quotes in (daily, intraday):
        assert resolve_quote(quotes, normal_close, "BUY", legally_tradable=True) is None
        prior = resolve_quote(quotes, normal_close, "SELL", legally_tradable=True)
        assert prior.price == 100 and prior.assumed
        fill = resolve_quote(quotes, actual_cutoff, "BUY", legally_tradable=True)
        assert fill.price == 110 and fill.available_at == actual_cutoff and not fill.assumed


@pytest.mark.parametrize(
    "old,new,suspension,listing,nominal,has_listed_peer",
    [
        ("INE286K01024", "INE285K01026", "2018-08-08", "2018-12-04", 1.0, False),
        ("INE688I01017", "INE092T01019", "2018-12-28", "2019-01-16", 13.9, True),
    ],
)
def test_app_engine_holds_locked_claim_then_sells_successor(
    monkeypatch, old, new, suspension, listing, nominal, has_listed_peer
):
    import backtest_engine as engine

    suspension, listing = map(pd.Timestamp, (suspension, listing))
    days = pd.bdate_range(suspension - pd.Timedelta(days=25), listing + pd.Timedelta(days=4))
    buy = days[3]
    last = days[days < suspension][-1]

    def quotes(index, close):
        return pd.DataFrame({"Close": close, "High": close, "Volume": 1000, "adjustment_factor": 1.0}, index=index)

    old_quotes = quotes(days[days < suspension], 100.0)
    successor = quotes(days if has_listed_peer else days[days >= listing], 100.0 / nominal)
    actions, marks = merger_claim(
        old,
        new,
        suspension,
        listing,
        nominal,
        old_quotes.loc[last],
        successor,
        mark_from_listed_class=has_listed_peer,
    )
    if not marks.empty:
        marks["price_status"] = "verified_mark"
        marks["verification_source"] = "fixture same-class listed close"
        marks["verified_at"] = marks.index.astype(str)
    claim = actions[0]["successor_symbol"]
    monkeypatch.setattr(engine, "get_rebalance_dates", lambda *_: [buy, suspension, listing])
    initial = [old, new] if has_listed_peer else [old]

    def ranking(_frames, as_of, *_args, **_kwargs):
        return (initial if as_of < buy else []), {}

    monkeypatch.setattr(engine, "rank_universe_at_date", ranking)
    frames = {old: old_quotes, new: successor}
    if not marks.empty:
        frames[claim] = marks
    cfg = engine.BacktestConfig(
        m=len(initial),
        n=len(initial) + 1,
        rebalance_freq="weekly",
        sort_method="1 year",
        start_date=str(days[0].date()),
        end_date=str(days[-1].date()),
        market_calendar=days,
        corporate_actions=actions,
        min_history_days=1,
    )
    result = engine.run_backtest(frames, {}, cfg)
    assert "error" not in result, result.get("error")
    book = "Full Rebalance"
    trades = result["execution_ledgers"][book]
    snapshots = {d["date"]: d for d in result["daily_ledgers"][book]}
    old_quantity = snapshots[buy]["quantities"][old]
    assert snapshots[suspension]["quantities"][claim] == pytest.approx(old_quantity)
    if not has_listed_peer:
        assert snapshots[suspension]["stale_marks"][claim] == str(last.date())
    else:
        assert snapshots[suspension]["marks"][claim] == pytest.approx(100)
        assert any(t["date"] == suspension and t["symbol"] == new and t["side"] == "SELL" for t in trades)
    assert all(t["symbol"] != claim or t["side"] == "TRANSFER" for t in trades)
    conversion_sale = next(t for t in trades if t["side"] == "SELL" and t["date"] == listing and t["symbol"] == new)
    assert conversion_sale["quantity"] == pytest.approx(old_quantity * nominal)
    assert snapshots[listing]["quantities"] == {}
    fifo = [lot for lot in result["closed_lots"][book] if lot["date"] == listing]
    assert sum(lot["basis"] for lot in fifo) == pytest.approx(old_quantity * 100 / (1 - cfg.transaction_cost_pct))
    assert all(lot["acquired"] == buy for lot in fifo)
    assert all(
        d["nav"] == pytest.approx(d["cash"] + d["stock_value"] - d["tax_payable"] - d["tax_accrued"])
        for d in snapshots.values()
    )
