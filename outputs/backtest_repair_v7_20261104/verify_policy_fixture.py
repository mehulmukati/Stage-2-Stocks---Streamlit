"""Check adapted ownership and missing-sale rules when historical books do not exercise them."""
import json

import pandas as pd

from run_matched import OUT, OWN, QuantityLedger, compare, load_native, native_book, simulate

CONFIG = dict(m=2, n=3, transaction_cost_pct=.001, stcg_rate=.20, ltcg_rate=.125)


def frames_from_quotes(quotes):
    frames = {}
    for day, prices in quotes.items():
        for symbol, price in prices.items():
            frames.setdefault(symbol, {})[day] = price
    return {s: pd.DataFrame({"Close": pd.Series(g)}).sort_index() for s, g in frames.items()}


def pair(native, frames, actions, days, ranks, quotes, config=CONFIG):
    args = (config, frames, actions, days, ranks, ranks, quotes, 0, True)
    a, assumed = simulate(QuantityLedger, *args)
    b, other = simulate(native, *args)
    assert assumed == other
    compare(a, b)
    return a, assumed


def ownership(native, event):
    old, new = event["old"], event["successor"]
    buy, suspension, listing = pd.to_datetime([event["last_old_close"], event["suspension"],
                                               event["first_newly_allotted_tradable"]])
    claim = "CLAIM:" + old
    ratio = event["actions"][1]["share_ratio"]
    peer = event["name"] == "CAPF"
    days = pd.DatetimeIndex([buy, suspension, listing])
    quotes = {buy: {old: 100.0}, suspension: {}, listing: {new: 100.0/ratio}}
    if peer:
        quotes[buy][new] = 100.0/ratio
        quotes[suspension][new] = 100.0/ratio
    ranks = {buy: list(quotes[buy]), suspension: [], listing: []}
    frames = frames_from_quotes(quotes)
    if peer:
        frames[claim] = pd.DataFrame({"Close": [100.0]}, index=[suspension])
    a, assumed = pair(native, frames, event["actions"], days, ranks, quotes)
    assert not assumed
    original = a.daily[0]["quantities"][old]
    assert a.daily[1]["quantities"] == {claim: original}
    assert a.daily[1]["marks"][claim] == 100.0
    assert (claim in a.daily[1]["stale_marks"]) != peer
    assert a.daily[-1]["quantities"] == {}
    closed = [lot for lot in a.closed_lots if lot["date"] == listing]
    assert len(closed) == 1 and closed[0]["acquired"] == buy
    assert abs(closed[0]["basis"] - original*100/(1-CONFIG["transaction_cost_pct"])) < 1e-9
    if peer:
        assert any(t["symbol"] == new and t["date"] == suspension and t["side"] == "SELL" for t in a.trades)
    return dict(case=event["name"], locked_mark=100, release_ratio=ratio, sale_quantity=closed[0]["quantity"])


def missing_muhurat(native):
    buy, execution = pd.to_datetime(["2019-10-25", "2019-10-27"])
    quotes = {buy: {"A": 100.0}, execution: {"B": 110.0}}
    ranks = {buy: ["A"], execution: ["A", "B"]}
    book, assumed = pair(native, frames_from_quotes(quotes), [], pd.DatetimeIndex([buy, execution]),
                         ranks, quotes, dict(CONFIG, m=1))
    assert len(assumed) == 1 and assumed[0]["age_seconds"] == 51.75*3600
    assert assumed[0]["cutoff"].endswith("T19:15:00+05:30")
    assert set(book.daily[-1]["quantities"]) == {"B"}
    return assumed[0]


if __name__ == "__main__":
    adapter = native_book(load_native(), CONFIG["transaction_cost_pct"])
    events = json.loads(OWN.read_text())["events"]
    output = dict(ownership=[ownership(adapter, event) for event in events],
                  missing_muhurat=missing_muhurat(adapter))
    (OUT / "policy_fixtures.json").write_text(json.dumps(output, indent=2, default=str))
    print(json.dumps(output, indent=2, default=str))
