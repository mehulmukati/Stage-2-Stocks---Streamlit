"""Explicit reviewed action overrides; never fit entitlements to a price gap."""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd


def reviewed_cash_share_action(share_ratio, amount, denomination):
    if denomination not in {"pre_action_share", "post_action_share"}:
        raise ValueError("A reviewed cash/share action requires its cash denomination")
    if not np.isfinite([share_ratio, amount]).all() or share_ratio <= 0 or amount < 0:
        raise ValueError("Invalid independently reviewed terms")
    return dict(
        kind="reviewed_cash_and_shares",
        supported=True,
        share_ratio=float(share_ratio),
        cash=float(amount) * (share_ratio if denomination == "post_action_share" else 1),
        cash_denomination=denomination,
        stated_cash_amount=float(amount),
    )


def apply_reviewed_decisions(actions, decisions, root, family):
    """Bind each decision to its source record, date, dated ID and evidence bytes."""
    result = [dict(a) for a in actions]
    lookup = {(a["source_row_id"], str(pd.Timestamp(a["date"]).date())): a for a in result}
    for d in decisions:
        if d["research_security_id"] != family or d["status"] != "resolved":
            continue
        key = (d["source_row_id"], d["date"])
        if key not in lookup:
            raise ValueError("Reviewed decision does not bind an existing action record")
        action = lookup[key]
        if action["subject"] != d["original_subject"]:
            raise ValueError("Primary action text changed since review")
        kind = d["normalization"]["kind"]
        subject = action["subject"].lower()
        if kind == "reviewed_cash_and_shares" and not any(word in subject for word in ["bonus", "split"]):
            raise ValueError("A cash/share review cannot replace a different entitlement type")
        if kind == "reviewed_fully_paid_rights" and "rights" not in subject:
            raise ValueError("Rights review does not bind a rights event")
        if not d.get("dated_identity_source_row") or not d.get("evidence"):
            raise ValueError("Dated identity and primary evidence required")
        for evidence in d["evidence"]:
            path = (Path(root) / evidence["path"]).resolve()
            if not path.is_relative_to(Path(root).resolve()):
                raise ValueError("Evidence path escapes the retained source workspace")
            if hashlib.sha256(path.read_bytes()).hexdigest() != evidence["sha256"]:
                raise ValueError("Reviewed entitlement evidence changed")
        action.update(d["normalization"])
        action.update(
            review_decision_id=d["decision_id"],
            review_evidence=d["evidence"],
            reason="Explicit primary-terms and dated-security attribution review",
        )
    return result
