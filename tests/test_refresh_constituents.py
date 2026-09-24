import json

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


def test_failed_refresh_preserves_snapshot_and_metadata(tmp_path):
    snapshot = tmp_path / "constituents.json"
    metadata = tmp_path / "constituents.meta.json"
    snapshot.write_text("original snapshot")
    metadata.write_text("original metadata")
    calls = []

    def fetch(url):
        calls.append(url)
        return _csv(50) if len(calls) == 1 else b"<html>Unavailable</html>"

    with pytest.raises(ValueError):
        refresh(tmp_path, fetch)
    assert len(calls) == 2
    assert snapshot.read_text() == "original snapshot"
    assert metadata.read_text() == "original metadata"


def test_successful_refresh_records_all_sources(tmp_path):
    counts = {filename: count for filename, count in SOURCES.values()}
    metadata = refresh(tmp_path, lambda url: _csv(counts[url.rsplit("/", 1)[-1]]))
    snapshot = json.loads((tmp_path / "constituents.json").read_text())
    assert set(snapshot) == set(SOURCES)
    assert sum(map(len, snapshot.values())) == 750
    assert set(metadata["sources"]) == set(SOURCES)
    assert json.loads((tmp_path / "constituents.meta.json").read_text()) == metadata
