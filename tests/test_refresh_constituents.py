import pytest

from scripts.refresh_constituents import SOURCES, parse_symbols, refresh


def _csv(count, prefix="STOCK"):
    return (
        "Company Name,Symbol,ISIN Code\n" + "".join(f"Company {i},{prefix}{i},INE{i:09d}\n" for i in range(count))
    ).encode()


@pytest.mark.parametrize(
    "raw",
    [
        b"<html>Access denied</html>",
        _csv(20),
        b"Company Name,Symbol,ISIN Code\nA,,INE000000000\n",
        b"Company Name,Symbol,ISIN Code\nA,A,INE000000000\nA,A,INE000000000\n",
    ],
)
def test_rejects_invalid_download(raw):
    with pytest.raises(ValueError):
        parse_symbols(raw, 50)


def test_retains_temporary_corporate_action_constituents():
    raw = _csv(250) + b"Temporary,DUMMYINGL1,INE000000000\n"
    assert "DUMMYINGL1" in parse_symbols(raw, 250)


def test_failed_refresh_preserves_shared_revision(market):
    import market_data as md

    before = md.source_revisions()
    calls = []

    def fetch(url):
        calls.append(url)
        return _csv(50) if len(calls) == 1 else b"<html>Unavailable</html>"

    with pytest.raises(ValueError):
        refresh(md.PRICE_PATH.parent.parent, fetch)
    assert md.source_revisions() == before


def test_successful_refresh_records_sources_and_preserves_history(market):
    import market_data as md

    _, history, days = market
    counts = {filename: count for filename, count in SOURCES.values()}
    metadata = refresh(md.PRICE_PATH.parent.parent, lambda url: _csv(counts[url.rsplit("/", 1)[-1]]))
    snapshot = md.load_snapshot()
    assert set(snapshot.constituents()) == set(SOURCES)
    assert sum(map(len, snapshot.constituents().values())) == 750
    assert set(metadata["sources"]) == set(SOURCES)
    assert snapshot.constituent_revision == metadata["revision"]
    assert snapshot.constituents(as_of=days[-11]) == {"Nifty 50": ["A", "LOWVOL"]}
    assert snapshot.membership.attrs["membership_date_basis"] == "observed_not_effective"
