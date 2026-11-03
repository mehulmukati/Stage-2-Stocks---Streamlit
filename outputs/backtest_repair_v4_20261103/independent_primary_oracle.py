"""Independent arithmetic and calendar reconstruction, without production price-model imports.

This verifies construction from recorded terms, not completeness or truth of all
entitlements. Those are separate acceptance blockers.
"""

from pathlib import Path
from decimal import Decimal
import hashlib
import json
import sys

import numpy as np
import pandas as pd
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from historical_universe import parse_primary_holidays

OUT = Path(__file__).parent
R3 = ROOT / "outputs/backtest_repair_v3_20261103"


def main(model_name):
    folder = OUT / model_name
    target = OUT / ("independent_" + model_name)
    target.mkdir(exist_ok=False)
    prices = pd.read_parquet(folder / "primary_adjusted_research_candidate.parquet")
    events = pd.read_csv(folder / "action_decisions.csv", parse_dates=["date"])
    applied = events[events.applied]
    field_errors = []
    checked = 0
    for family, g in prices.groupby("research_security_id"):
        g = g.sort_values("date")
        factors = np.ones(len(g))
        own = applied[applied.research_security_id.eq(family)]
        for a in own.to_dict("records"):
            before = g[g.date < a["date"]]
            assert not before.empty
            previous = Decimal(str(before.iloc[-1].CLOSE))
            assert np.isclose(float(previous), a["previous_raw_close"])
            assert before.iloc[-1].date == pd.Timestamp(a["previous_quote_date"])
            if a["kind"] == "reviewed_fully_paid_rights":
                rights = Decimal(87) / Decimal(38)
                assert a["symbol"] == "IDEA" and a["date"] == pd.Timestamp("2019-03-29")
                factor = (previous + rights * Decimal("12.5")) / ((Decimal(1) + rights) * previous)
            else:
                factor = (previous - Decimal(str(a["cash"]))) / previous / Decimal(str(a["share_ratio"]))
            assert np.isclose(float(factor), a["factor"], rtol=1e-12)
            factors[g.date < a["date"]] *= float(factor)
            checked += 1
        assert np.allclose(factors, g.adjustment_factor, rtol=1e-12, atol=1e-12)
        for raw, adjusted in [("OPEN", "Open"), ("HIGH", "High"), ("LOW", "Low"), ("CLOSE", "Close")]:
            error = np.nanmax(np.abs(g[raw].to_numpy() * factors - g[adjusted].to_numpy()))
            field_errors.append(float(error))
            assert np.allclose(g[raw] * factors, g[adjusted], rtol=1e-12, atol=1e-9, equal_nan=True)
        assert g.TOTTRDQTY.equals(g.Volume.rename("TOTTRDQTY"))
        assert not g.accepted_for_backtest.any()
    # Published example in NSE/FAOP/40530, annexure 1 (indicative P=30.25).
    example = (Decimal("30.25") + Decimal(87) / Decimal(38) * Decimal("12.5")) / (
        Decimal("30.25") * (Decimal(1) + Decimal(87) / Decimal(38))
    )
    assert abs(example - Decimal("0.591603")) < Decimal("0.000001")
    report = dict(
        passed=True,
        rows=len(prices),
        independently_reconstructed_applied_factors=checked,
        maximum_price_absolute_error=max(field_errors),
        nominal_volume_unchanged=True,
        NSE_IDEA_indicative_annex_example=str(example),
        production_price_model_imported=False,
        acceptance=False,
        limits="Arithmetic reconstruction from recorded primary terms; unresolved action identity, missing terms, source conflicts and tradability are NOT certified",
    )
    (target / "independent_primary_arithmetic.json").write_text(json.dumps(report, indent=2))

    closed = set()
    special = set()
    evidence = []
    for year, name in [(2014, "CMTR25326"), (2015, "CMTR28337"), (2016, "CMTR31297"), (2017, "CMTR33746")]:
        p = (OUT if year < 2017 else R3) / "evidence" / (name + ".pdf")
        text = "\n".join(page.extract_text(extraction_mode="layout") for page in PdfReader(p).pages)
        c, s = parse_primary_holidays(text, year)
        closed |= c
        special |= s
        evidence.append(dict(path=str(p.relative_to(ROOT)), sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
    for name, day in [("CMTR26360", "2014-04-24"), ("CMTR27792", "2014-10-15")]:
        p = OUT / "evidence" / (name + ".pdf")
        text = " ".join(PdfReader(p).pages[0].extract_text().split())
        assert "CAPITAL MARKET" in text and "trading holiday" in text
        assert pd.Timestamp(day).strftime("%B %d") in text
        closed.add(pd.Timestamp(day))
        evidence.append(
            dict(path=str(p.relative_to(ROOT)), sha256=hashlib.sha256(p.read_bytes()).hexdigest(), holiday=day)
        )
    p = OUT / "evidence/CMTR26045.pdf"
    text = PdfReader(p).pages[0].extract_text()
    assert "Live Trading" in text and "March 22, 2014" in text
    special.add(pd.Timestamp("2014-03-22"))
    evidence.append(
        dict(path=str(p.relative_to(ROOT)), sha256=hashlib.sha256(p.read_bytes()).hexdigest(), live="2014-03-22")
    )
    # The exchange annual report explicitly notes this special date below a
    # figure comparing near-month futures AND underlying cash Nifty movement.
    p = OUT / "evidence/NSEIL_Annual_Report_2015.pdf"
    text = PdfReader(p).pages[11].extract_text()
    assert "Cash segment" in text and "Special trading sessions on February 28, 2015" in text
    special.add(pd.Timestamp("2015-02-28"))
    evidence.append(
        dict(
            path=str(p.relative_to(ROOT)),
            sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
            page=12,
            live="2015-02-28",
            limitation="Cash movement figure footnote plus original CM bhavcopy; CD28927 is currency-segment evidence only",
        )
    )
    expected = (set(pd.bdate_range("2014-01-01", "2017-06-30")) - closed) | {
        d for d in special if pd.Timestamp("2014-01-01") <= d <= pd.Timestamp("2017-06-30")
    }
    actual = set(prices.loc[prices.date < "2017-07-01", "date"])
    calendar = dict(
        expected=len(expected),
        observed=len(actual),
        missing=sorted(str(d.date()) for d in expected - actual),
        unexpected=sorted(str(d.date()) for d in actual - expected),
        evidence=evidence,
        accepted_calendar_changed=False,
    )
    assert not calendar["missing"] and not calendar["unexpected"] and len(actual) == 862
    (target / "independent_warmup_calendar.json").write_text(json.dumps(calendar, indent=2))
    print(json.dumps(report, indent=2))
    print("Calendar: 862 expected/observed; zero unexplained differences")


if __name__ == "__main__":
    main(sys.argv[1])
