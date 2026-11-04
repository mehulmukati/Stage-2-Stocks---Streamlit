"""Check every staged Close against the old model and the exact three reviewed factors."""
import json

import numpy as np
import pandas as pd

from run_matched import OUT, PRICE, sha


if __name__ == "__main__":
    staged = OUT / "provisional_close_revision.parquet"
    new = pd.read_parquet(staged)
    old = pd.read_parquet(PRICE, columns=list(new.columns))
    order = ["research_security_id", "date"]
    old = old.sort_values(order).reset_index(drop=True)
    assert len(old) == len(new) == 939941
    immutable = [c for c in new if c not in {"Close", "adjustment_factor"}]
    pd.testing.assert_frame_equal(new[immutable], old[immutable], check_exact=True)
    assert not new.accepted_for_backtest.any()
    factor = np.ones(len(new))
    applied = json.loads((OUT / "rights_application.json").read_text())
    for row in applied:
        selected = new.research_security_id.eq(row["family"]) & new.date.lt(row["ex"])
        assert int(selected.sum()) == row["changed_rows"]
        factor[selected] *= row["factor"]
    np.testing.assert_array_equal(new.Close, old.Close * factor)
    np.testing.assert_array_equal(new.adjustment_factor, old.adjustment_factor * factor)
    report = dict(rows=len(new), rows_with_changed_basis=int((factor != 1.0).sum()),
                  staged_sha256=sha(staged), source_sha256=sha(PRICE), accepted=False)
    (OUT / "independent_stage_verification.json").write_text(json.dumps(report, indent=2))
    print(report)
