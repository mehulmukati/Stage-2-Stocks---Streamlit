"""Check every new price-lineage edge against original API/bulletin and dated IDs.

Does not call production action parsers or lineage builders.
"""

from pathlib import Path
import io
import json
import re
import sys
import zipfile

import numpy as np
import pandas as pd

OUT = Path(__file__).parent
ROOT = OUT.parents[1]


def ratio(text):
    text = " ".join(text.lower().split())
    split = re.search(r"(?:rs\.?|re\.?)\s*([\d.]+).*?to\s*(?:rs\.?|re\.?)\s*([\d.]+)", text)
    bonus = re.search(r"bonus(?: issue)?\s*[-:]?\s*(\d+)\s*[:/]\s*(\d+)", text)
    if split and any(s in text for s in ["split", "splt", "sub-division"]):
        value = float(split[1]) / float(split[2])
    else:
        value = 1.0
    if bonus:
        value *= 1 + int(bonus[1]) / int(bonus[2])
    return value


def main(model_name):
    folder = OUT / model_name
    edges = json.loads((folder / "identity_edges.json").read_text())
    api = json.loads((OUT / "evidence/nse_actions_2014_2020.json").read_text())
    raw = pd.read_parquet(folder / "raw_warmup_and_scope_quotes.parquet")
    by_ref = raw.set_index("source_row_id")
    assert by_ref.index.is_unique
    existing = json.loads(
        (ROOT / "outputs/backtest_repair_v3_20261103/candidate_v3/reviewed_lifecycle_decisions.json").read_text()
    )
    old = {
        (r["old_security_id"].removeprefix("ISIN:"), r["new_security_id"].removeprefix("ISIN:"), r["ex_date"])
        for r in existing
        if r["applicable_to_scope"]
    }
    checked = []
    for e in edges:
        if (e["old_isin"], e["new_isin"], e["date"]) in old:
            continue
        api_refs = [r for r in e["evidence"] if r.startswith("nse_actions_2014_2020.json:")]
        assert len(api_refs) == 1
        a = api[int(api_refs[0].split(":")[-1])]
        assert a["series"] in ["EQ", "BE", "BZ"]
        assert pd.to_datetime(a["exDate"], format="%d-%b-%Y") == pd.Timestamp(e["date"])
        assert np.isclose(ratio(a["subject"]), e["share_ratio"])
        refs = [r for r in e["evidence"] if r.startswith("cm")]
        g = by_ref.loc[refs]
        assert set(g.ISIN) == {e["old_isin"], e["new_isin"]}
        assert g.SYMBOL.nunique() == 1
        before = g[g.ISIN.eq(e["old_isin"])].date.max()
        after = g[g.ISIN.eq(e["new_isin"])].date.min()
        assert before < after
        corroborated = []
        if a["isin"] not in {e["old_isin"], e["new_isin"]}:
            for ref in [r for r in e["evidence"] if r.startswith("PR")]:
                archive, member, row = ref.split(":")
                with zipfile.ZipFile(OUT / "evidence" / archive) as z:
                    b = pd.read_csv(io.BytesIO(z.read(member)), dtype=str).iloc[int(row) - 2]
                assert b.SYMBOL.strip() == g.SYMBOL.iloc[0]
                assert pd.to_datetime(b.EX_DT.strip(), format="%d/%m/%Y") == pd.Timestamp(e["date"])
                assert np.isclose(ratio(b.PURPOSE), e["share_ratio"])
                corroborated.append(ref)
            assert corroborated, "An obsolete API ISIN cannot authorize the continuity"
        checked.append(
            dict(
                old_isin=e["old_isin"],
                new_isin=e["new_isin"],
                date=e["date"],
                ratio=e["share_ratio"],
                original_bulletin_required=bool(corroborated),
            )
        )
    report = dict(
        passed=True,
        new_edges_independently_checked=len(checked),
        previous_round_edges_rechecked_separately=len(old),
        obsolete_API_ISIN_edges_with_original_bulletins=sum(r["original_bulletin_required"] for r in checked),
        checks=checked,
        production_action_or_graph_imported=False,
        accepted=False,
    )
    (OUT / ("independent_" + model_name) / "independent_lineage_reconciliation.json").write_text(
        json.dumps(report, indent=2)
    )
    print(json.dumps({k: v for k, v in report.items() if k != "checks"}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
