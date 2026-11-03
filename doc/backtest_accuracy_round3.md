# Round 3 historical data policy and reproduction

Round 3 continues commit `6981092`. The complete accounting engine and publication-boundary fixes remain unchanged. Final evidence and results are in `outputs/backtest_repair_v3_20261103`; see its `implementation_report.md` for completion counts and unresolved facts.

The final membership candidate is `candidate_v3`; the price audit is `price_audit_v4`. Both remain unaccepted for production backtesting. A reconciled membership history is distinct from validated adjusted prices and portfolio entitlements. No active registry, accepted market file, app coverage flag, strategy default or Live Signal checkpoint is changed.

Two membership views are deliberate. The raw-identifier view follows actual dated identifiers in unchanged NSE trading observations. The economic view follows evidenced ex-dates and explicitly projects raw identifier lags for comparison. Original monthly observations are never rewritten. A split of a nonmember does not create index membership, distinct old/new share classes cannot be collapsed, and a corporate action does not become an index entry/exit. Yes Bank's 2020 reconstruction retains its conditional lock-in limitation.

Economic security lineage does not multiply already-adjusted research quantities or credit split, bonus or dividend entitlements again. It does not establish nominal statutory tax basis. Vendor observation availability, historical security identity, adjustment correctness, reported volume and execution eligibility remain separate facts. Missing prices never remove an eligible security or authorize replacement by a merger successor's earlier performance.

The builder validates PDF extraction-cache bindings against the archived payload before use. Inherited proposal metadata is explicitly `LEGACY_*`; `IDENTITY_STATUS`, `EVENT_TIMING_STATUS`, `EVENT_CHAIN_STATUS` and `DAILY_RECONSTRUCTION_ACCEPTED` describe different evidence layers. A calendar's publication year, holiday date, Muhurat exception, and trading-file timestamp are parsed explicitly rather than inferred from retrieval dates or file availability alone.

## Reproduce offline in this checkout

Use the recorded Python/runtime dependencies and retained source payloads. Acquisition receipts bind the downloaded bytes; a fresh network fetch may contain vendor restatements and is not an identical input. The offline builder/audits do not fetch data or publish accepted files.

From the repository root, with the same dependency environment:

```powershell
python outputs/backtest_repair_v3_20261103/build_candidate.py candidate_replay
python outputs/backtest_repair_v3_20261103/audit_prices.py price_audit_replay
python -m pytest tests -q -p no:cacheprovider
```

Output directory names must be new; construction refuses an existing target. The price audit uses the frozen final candidate. The independent membership, price, calendar and existing portfolio-ledger audit sources and results are retained alongside the manifests. Run independent checks in a new output copy to preserve delivered historical check results.

The manifest/source snapshots identify exact executed code. Primary archives and existing adjusted sources remain separate. A combined Nifty 500 certificate, if later justified, must stay scoped to profiles requesting the supported parent set; it cannot broaden a single-component profile or manufacture early Microcap membership. Publication must retain the repaired interval isolation and existing revision/backup protections. A fixed matched strategy matrix is allowed only after its full data scope is supported.
