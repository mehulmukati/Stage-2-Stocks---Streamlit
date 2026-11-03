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
