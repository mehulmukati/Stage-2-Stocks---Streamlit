"""Offline parsing and evidence joins for bounded historical constituent repair.

Only primary, dated NSE Indices rows establish company-name/symbol observations.
NSE bhavcopy rows establish dated symbol/ISIN observations. No fuzzy resolver or
current survivor list is used here. Snapshots alone do not certify intervening days.
"""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
from pathlib import Path

import pandas as pd
from pypdf import PdfReader


def legal_name_key(value):
    """Punctuation/case and standard Limited/Ltd abbreviation only."""
    text = str(value).casefold().replace("&", " and ")
    text = re.sub(r"\blimited\b", "ltd", text)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def parse_primary_holidays(text, year):
    """Read numbered primary calendar dates using an explicit date format.

    Dateutil can misread `September 13,2018`; normalize punctuation and parse
    the captured components explicitly. Muhurat dates override holiday rows.
    """
    month = "January|February|March|April|May|June|July|August|September|October|November|December"
    pattern = rf"({month})\s+(\d{{1,2}}),?\s*(20\d{{2}})"

    def convert(match):
        return pd.to_datetime(f"{match[1]} {match[2]} {match[3]}", format="%B %d %Y")

    closed = set()
    for match in re.finditer(r"^\s*\d+\s+" + pattern, text, re.M):
        day = convert(match)
        if day.year != year:
            raise ValueError("Mixed primary calendar years")
        closed.add(day)
    special = set()
    for paragraph in re.finditer(r"Muhurat\s+Trading\s+will\s+be\s+conducted(.{0,150})", text, re.I | re.S):
        match = re.search(pattern, paragraph[1])
        if match:
            day = convert(match)
            if day.year != year:
                raise ValueError("Mixed primary calendar years")
            special.add(day)
    if not closed or not special:
        raise ValueError("Incomplete primary holiday/Muhurat calendar")
    return closed, special


def read_nifty500_archive(path):
    path = Path(path)
    with zipfile.ZipFile(path) as archive:
        members = [n for n in archive.namelist() if "500" in n and n.lower().endswith(".pdf")]
        if len(members) != 1:
            raise ValueError("Expected one explicit Nifty 500 constituent PDF")
        member = members[0]
        payload = archive.read(member)
    pdf = PdfReader(io.BytesIO(payload))
    rows = []
    date = None
    for page_number, page in enumerate(pdf.pages, start=1):
        chunks = []
        page.extract_text(
            visitor_text=lambda t, cm, tm, fd, fs: chunks.append((tm[4], tm[5], t.strip())) if t.strip() else None
        )
        text = page.extract_text()
        date_pattern = (
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+\d{1,2},\s+\d{4}"
        )
        matches = re.findall(date_pattern, text)
        date_match = re.search(date_pattern, text)
        if not matches or not date_match or "Constituents of" not in text:
            raise ValueError("Missing dated constituent page header")
        observed = pd.Timestamp(date_match.group()).normalize()
        if date is not None and observed != date:
            raise ValueError("Mixed observation dates in archive")
        date = observed
        current = None
        for x, y, token in chunks:
            if 20 <= x <= 60 and y < 705 and re.fullmatch(r"[A-Z0-9&_.-]+", token):
                current = dict(
                    symbol=token,
                    name="",
                    date=date,
                    page=page_number,
                    source_row_id=f"{path.name}:{member}:page{page_number}:y{y:.2f}",
                    archive=path.name,
                    pdf_member=member,
                    pdf_sha256=hashlib.sha256(payload).hexdigest(),
                )
                rows.append(current)
            elif current is not None and 100 <= x <= 115 and y < 705:
                current["name"] += " " + token.replace("\n", " ")
        for row in rows:
            row["name"] = " ".join(row["name"].split())
    result = pd.DataFrame(rows)
    if result.empty or result.name.eq("").any() or result.symbol.duplicated().any():
        raise ValueError("Incomplete/duplicate constituent extraction")
    return result


def read_bhavcopy(path):
    with zipfile.ZipFile(path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError("Expected one NSE trading CSV")
        frame = pd.read_csv(archive.open(members[0]))
    required = {"SYMBOL", "ISIN", "SERIES", "TIMESTAMP", "CLOSE"}
    if required - set(frame):
        raise ValueError("Incomplete primary trading identifiers")
    dates = pd.to_datetime(frame.TIMESTAMP, format="%d-%b-%Y", errors="coerce")
    missing = dates.isna()
    dates.loc[missing] = pd.to_datetime(frame.loc[missing, "TIMESTAMP"], format="%d-%b-%y", errors="coerce")
    if dates.isna().any() or dates.nunique() != 1:
        raise ValueError("Invalid/mixed primary trading dates")
    filename_date = re.fullmatch(r"cm(\d{2}[A-Z]{3}\d{4})bhav\.csv\.zip", Path(path).name, re.I)
    if filename_date and dates.iloc[0] != pd.to_datetime(filename_date[1], format="%d%b%Y"):
        raise ValueError("Primary trading date does not match archive date")
    frame["date"] = dates
    frame["archive"] = Path(path).name
    frame["source_row_id"] = [f"{Path(path).name}:{members[0]}:{i + 2}" for i in range(len(frame))]
    return frame


def validate_constituent_cache(frame, archive_path):
    """Bind every cached extraction row to the exact raw PDF it claims.

    A digest copied from a different archive, absent bindings, or mixed
    payloads must fail before any reconstruction is attempted.
    """
    required = {"archive", "pdf_member", "pdf_sha256", "date", "symbol", "name", "source_row_id"}
    if required - set(frame) or frame.empty:
        raise ValueError("Incomplete constituent cache")
    path = Path(archive_path)
    with zipfile.ZipFile(path) as archive:
        members = [n for n in archive.namelist() if "500" in n and n.lower().endswith(".pdf")]
        if len(members) != 1:
            raise ValueError("Expected one explicit Nifty 500 constituent PDF")
        member = members[0]
        digest = hashlib.sha256(archive.read(member)).hexdigest()
    if not frame.archive.eq(path.name).all() or not frame.pdf_member.eq(member).all():
        raise ValueError("Constituent cache/archive member mismatch")
    if not frame.pdf_sha256.eq(digest).all():
        raise ValueError("Constituent cache PDF digest mismatch")
    if frame.symbol.duplicated().any() or frame.name.isna().any() or frame.name.eq("").any():
        raise ValueError("Incomplete/duplicate constituent cache rows")
    if pd.to_datetime(frame.date).nunique() != 1 or frame.source_row_id.duplicated().any():
        raise ValueError("Mixed dates/duplicate constituent cache references")
    return frame


def match_event_identity(event, observations, bhav):
    """Verify an event identity at its effective date, without projecting a
    modern point backwards. Exact legal name appears in a nearby historical
    constituent observation; the symbol/ISIN is independently present in the
    exchange file on the event date. Ambiguity remains unresolved.
    """
    day = pd.Timestamp(event["EVENT_DATE"])
    keys = observations["_name_key"] if "_name_key" in observations else observations.name.map(legal_name_key)
    nearby = observations[
        keys.eq(legal_name_key(event["SCRIP_NAME"])) & observations.date.sub(day).abs().le(pd.Timedelta(days=35))
    ]
    traded = bhav[bhav.date.eq(day) & bhav.SERIES.isin(["EQ", "BE", "BZ"])]
    joined = nearby.merge(traded, left_on="symbol", right_on="SYMBOL", suffixes=("_index", "_exchange"))
    evidence_basis = "event-date exchange identity"
    if joined.empty:
        # A delisting/exclusion can have no event-day trade. Identity does not
        # require a fill: retain dated prior ISIN evidence, never invent a bar.
        prior = bhav[
            bhav.date.le(day) & bhav.date.ge(day - pd.Timedelta(days=35)) & bhav.SERIES.isin(["EQ", "BE", "BZ"])
        ]
        joined = nearby.merge(prior, left_on="symbol", right_on="SYMBOL", suffixes=("_index", "_exchange"))
        evidence_basis = "prior exchange identity within 35 days; no event-day execution inferred"
    ids = joined[["SYMBOL", "ISIN"]].drop_duplicates()
    if len(ids) != 1:
        return None, "missing/ambiguous exact dated name-symbol-ISIN evidence"
    symbol, isin = ids.iloc[0]
    if not re.fullmatch(r"IN[A-Z0-9]{10}", str(isin)):
        return None, "invalid primary security identifier"
    row = joined.iloc[(joined.date_index - day).abs().argsort()].iloc[0]
    return (
        dict(
            security_id="ISIN:" + isin,
            issuer_id=None,
            issuer_status="legal issuer identifier not independently established; ISIN is security-specific",
            name=event["SCRIP_NAME"],
            exchange_symbol=symbol,
            vendor_symbol=None,
            valid_from=str(day.date()),
            valid_to=str(day.date()),
            source="https://www.niftyindices.com/Indices_-_Market_Capitalisation_and_Weightage/" + row.archive_index,
            observed_at=str(row.date_index.date()),
            verified_at="2026-11-03",
            retrieved_at="2026-11-03",
            confidence="verified",
            decision="reviewed",
            evidence=[row.source_row_id_index, row.source_row_id_exchange],
            validity_basis=evidence_basis
            + " plus exact nearby historical constituent name; no unobserved alias interval",
        ),
        None,
    )
