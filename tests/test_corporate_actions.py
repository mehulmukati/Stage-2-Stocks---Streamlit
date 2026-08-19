import pandas as pd

from corporate_actions import load_corporate_actions, load_index_replacements


def test_jbchemicals_merger_registry_record():
    actions = load_corporate_actions()
    action = next(item for item in actions if item["old_symbol"] == "JBCHEPHARM")

    assert action["event_type"] == "merger"
    assert action["successor_symbol"] == "TORNTPHARM"
    assert action["effective_date"] == pd.Timestamp("2026-07-17")
    assert action["last_trading_date"] == pd.Timestamp("2026-07-16")
    assert action["share_ratio"] == 0.51


def test_jbchemicals_index_replacements_are_registered():
    replacements = load_index_replacements()
    by_index = {item["index_name"]: item for item in replacements}

    assert by_index["NIFTY SMALLCAP 250"]["removed_symbol"] == "JBCHEPHARM"
    assert by_index["NIFTY SMALLCAP 250"]["added_symbol"] == "PFOCUS"
    assert by_index["NIFTY MICROCAP 250"]["removed_symbol"] == "PFOCUS"
    assert by_index["NIFTY MICROCAP 250"]["added_symbol"] == "GRINDWELL"
    assert all(item["effective_date"] == pd.Timestamp("2026-07-17") for item in replacements)
