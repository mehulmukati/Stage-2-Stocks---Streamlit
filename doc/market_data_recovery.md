# Automatic market-data recovery

The shared GitHub publisher owns market-price downloads. Screener pages read the
accepted files and calculate results locally; viewing or rerendering a page never
downloads prices from the vendor.

## Attempts and acceptance

| Scheduled slot (IST) | Target |
| --- | --- |
| 7:45 PM | Latest completed NSE cash-equity session |
| 8:45 PM | Same session, if coverage still fails |
| 10:45 PM | Same session, if coverage still fails |
| 8:45 AM next morning | Prior completed session; final recovery attempt |

Each run uses its workflow creation time and triggering cron slot to determine the
target. Queue delays across midnight cannot silently advance an evening retry to a
different session. Holidays and weekends use the existing NSE cash-equity calendar.
Automatic vendor attempts stop after the session's next-morning slot, even when later
holiday/weekend runs still resolve to that old session. Those runs may check coverage
and maintain the existing alert, but do not restart an exhausted retry window.
GitHub schedules can be delayed or dropped; these are independent recovery chances,
not a guarantee of exact execution times. See
[GitHub's scheduling limitations](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

Before contacting the vendor, the runner checks accepted target-session coverage.
At least 95% of current stocks and all three core benchmarks (Nifty 50, Nifty 100,
Nifty 500) must cover the target. A successful check skips constituent verification,
price downloads, and source writes. Isolated missing stocks remain recorded and
visible in the app; passing this gate does not claim every stock is current.

When coverage fails, the runner verifies constituents once and invokes the shared
price publisher once. Its adapter batches series and retries incomplete responses
twice. Validation happens before the atomic, revision-checked price write. A rejected
response preserves the previous price file; a failed run never commits source files.
The workflow concurrency group serializes scheduled publications. A conflicting Git
push fails rather than overwriting newer repository changes.

## Diagnostics and alerts

Every attempt records its target, status, actual accepted price date, coverage,
missing stocks/benchmarks, and failure reason in a JSON report and workflow summary.
The report is retained as a workflow artifact for 14 days, independently of the
accepted price file.

Intermediate provider failures leave a warning in the run report without opening an
issue or failing the overall workflow. The final morning failure marks the workflow
failed and opens one dated GitHub issue. Repeated failures for the same target do not
create additional issues. A successful publication or accepted-coverage check closes
previous recovery issues through that target. Only issues created by the Actions bot
with the recovery marker are managed. Final setup failures also raise an alert, with
the failed run linked when coverage diagnostics could not be produced.

Alerts use GitHub repository issues and Actions notifications. Email/web delivery
depends on the recipient's GitHub notification settings; this does not configure an
external email, Slack, or other messaging service.

Manual workflow dispatch supports an optional target session, a forced download,
and a final-attempt flag. Normal manual runs use the same skip and validation rules.

## App behavior and request ledger

An active Stage 2 or Momentum screener checks local source revisions once per minute.
New sources, a new current session, or an outdated result schema trigger one background
recalculation from accepted local files. Initial visits also calculate automatically.
Failed automatic calculations are not resubmitted for the same revision and session;
Run remains available for an explicit retry. Momentum historical date selections are
preserved; a date that was following the latest session advances with that session.

| Flow | Upstream requests | Shared writes |
| --- | --- | --- |
| Accepted target / later retry after success | None | None |
| Missing target / scheduled recovery | One constituent verification and batched price adapter calls | Validated atomic price publication, then Git commit |
| Page open / periodic revision check | None | None |
| Automatic screener recalculation | None | None; revision-keyed result cache only |
| Final failed attempt | GitHub issue API; no extra vendor retry | Diagnostic artifact and one deduplicated alert |

The regression suite simulates outage, recovery, and a subsequent no-fetch skip;
checks holiday/queue-delay target selection; verifies rejected writes preserve file
bytes; tests automatic UI recalculation without vendor access; and exercises alert
creation, deduplication, and closure against mocked GitHub APIs.
