# Dhan token renewal

The market-data workflow renews an active Dhan Web token before each download.
It also runs renewal-only slots every six hours, including weekends and holidays.
All slots share the `shared-market-publisher` concurrency group. GitHub can delay
or drop scheduled runs. An expired token requires a new login through Dhan Web.

## GitHub activation

These steps require an account that can manage this repository's environments
and their secrets.

1. Create the GitHub environment `market-data` under repository Settings,
   Environments. Avoid required reviewers or wait timers for this automated job.
2. Create a fine-grained GitHub personal access token scoped to this repository
   with repository **Environments, read and write** permission. The account must
   have permission to manage its environment secrets. Set an appropriate expiry
   and replace the writer credential before it expires. If the repository is
   unavailable in the token's repository selector, its owner must supply an
   appropriately scoped credential.
3. Add these three secrets to the `market-data` environment.

| Secret | Value |
| --- | --- |
| `DHAN_CLIENT_ID` | Dhan client ID |
| `DHAN_ACCESS_TOKEN` | Active token generated from Dhan Web |
| `DHAN_TOKEN_WRITER` | GitHub credential with the environment-secret permission above |

The standard workflow `GITHUB_TOKEN` cannot replace the writer credential for
environment-secret management. Dhan's API key and API secret are not used by the
Web-token renewal endpoint.

Deploy the source changes and manually run **Refresh Shared Market Data** once.
Check that the renewal step succeeds and the environment token secret's update
time advances. Every run verifies secret-write access before rotating the Dhan
credential. Environment secrets are read when the job starts, so jobs queued
behind a previous renewal receive its newly saved token.

The newly renewed token reaches the current download step through `GITHUB_ENV`.
The download step prefers that exported token and uses the bootstrap secret only
if no renewed token was exported. Later jobs read it from the environment secret.
The token is masked before export, and no
plaintext token is put in a commit, status report, issue or uploaded artifact.

## Failures and recovery

Token or persistence failures fail the workflow and open one bot-owned repository
issue. Repeated failures do not create duplicate issues. The next successful
renewal closes that alert. Notification delivery follows GitHub notification
settings. Status diagnostics are in the workflow artifact.

Price recovery still runs after a renewal failure. It tries Dhan with the available
token and uses yfinance for unresolved series if Dhan is unavailable. Validated
fallback prices may be published and committed while the token failure remains
visible in the workflow and its separate alert.

Renewal invalidates the previous token. The publisher retries transient GitHub
save failures using the same replacement token. It sends one renewal request
per invocation. After a transport, JSON parsing, or token extraction failure,
check the stored credential with Dhan’s read-only User Profile endpoint before
manually dispatching another run. Coordinate with the scheduled workflow so
local renewal cannot invalidate the GitHub copy.

The status report distinguishes HTTP rejection, invalid JSON, failed token
extraction, and failed persistence. Extraction diagnostics list only allowlisted
field names and JSON types. The parser accepts a JWT-shaped replacement in
`accessToken`, `access_token`, or `token` at the top level or one level under
`data`. The aliases and envelope are compatibility cases. Dhan’s public renewal
docs do not specify the response schema.

An extraction failure leaves no replacement recovery artifact. If persistence
fails after the encrypted replacement was staged, the workflow preserves a
one-day artifact named `dhan-token-recovery-<run ID>`. It contains only the token encrypted with
GitHub's environment public key and that key's identifier. Download and extract
`dhan_token_recovery.json`, then restore it while the token remains valid.

```sh
gh api --method PUT repos/OWNER/REPO/environments/market-data/secrets/DHAN_ACCESS_TOKEN --input dhan_token_recovery.json
```

Use the actual repository owner and name. The authenticated GitHub CLI account
must be allowed to update environment secrets. The encrypted JSON contains the
exact REST API body. Rerun the workflow after restoring it. If the token has
expired or GitHub's encryption key has changed, seed a fresh Dhan Web token.

An abrupt runner loss between Dhan issuing a token and GitHub saving or uploading
its encrypted recovery file can still require a fresh login. The two services do
not provide a shared transaction.

## Local use

```sh
python scripts/renew_dhan_token.py
```

This atomically updates `DHAN_ACCESS_TOKEN` in the ignored project `.env` while
preserving its other settings. Local renewal shares the price-publisher lock.
If replacement fails, `.env.dhan-recovery` retains the private replacement file.
Restore it with `python scripts/renew_dhan_token.py --restore` before renewing
again. `.env.*` is ignored by Git. Never upload the local recovery file.

Cloud renewal makes any local copy of the same token stale. Pause the cloud
workflow before managing that token locally, and seed the environment secret
with its current value before resuming. An independently configured environment
variable overrides `.env`, so remove stale shell overrides before using it.
