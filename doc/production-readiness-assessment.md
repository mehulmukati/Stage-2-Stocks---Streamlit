# Production-Readiness Engineering Assessment
## Stage-2-Stocks Streamlit — NSE Screener & Backtester

**Assessment Date:** 2026-05-08  
**Assessor Role:** Principal Engineer / Staff Software Architect  
**Codebase Location:** `D:\Content\GoogleDrive\Github\Stage-2-Stocks---Streamlit`  
**Total Python LOC:** ~8,500 across 14 modules  
**Assessment Scope:** Full codebase — all .py, CI/CD, data, config, docs

---

## A. EXECUTIVE SUMMARY

| Dimension | Score | Rationale |
|---|---|---|
| **Engineering Quality** | **7 / 10** | Clean architecture, good patterns, incomplete type hints, oversized engine files |
| **Production Readiness** | **6 / 10** | No auth, no external monitoring, no Docker, yfinance fragility |
| **Maintainability** | **7 / 10** | Good separation of concerns, but 1,466-line engine, sparse docstrings |
| **Scalability** | **5 / 10** | Single-region, no API, 4-thread pool, no horizontal-scale path |
| **Security** | **5 / 10** | No authentication, public exposure, minimal rate-limiting |

### Biggest Strengths
1. **Thread-safe data layer** — RLock + single-flight latch + atomic Parquet writes are genuinely production-grade patterns, not cargo-culted.
2. **Financial modeling fidelity** — India CGT (LTCG/STCG with 8-year carry-forward), FIFO lot tracking, survivorship-bias mitigation via `compositions.parquet` — sophisticated and largely correct.
3. **Clean engine/UI separation** — Pure-Python engines have zero Streamlit dependencies, enabling unit testing and future API exposure.
4. **6-variant parallel backtest** — Simultaneous Classic/Displacement × Full/Marginal/Prop-Fill is architecturally elegant.
5. **CI discipline** — pre-commit hooks (black, isort, flake8) enforced on every push.

### Biggest Weaknesses
1. **No authentication or access control** — app is public, unlimited backtests, no rate limiting.
2. **yfinance as sole live data source** — community-maintained, no retry/circuit-breaker, single point of failure.
3. **Zero operational observability** — no external error reporting, no data-freshness alerts, no performance metrics.
4. **Test coverage gap on critical financial logic** — tax engine, FIFO lot matching, and weight-capping are under-tested relative to their complexity.
5. **No deployment automation** — manual Parquet refresh scripts, no Dockerfile, no reproducible environment.

### Is This Genuinely Production-Grade?
**Partially.** The core computation engines are production-grade. The infrastructure, security posture, and operational maturity are not. This is a strong internal research tool that has not yet been hardened for external user exposure.

---

## B. ENGINEERING MATURITY CLASSIFICATION

### Classification: **Startup Production System**

**Justification:**

This is not a prototype — the financial modeling depth, thread-safety discipline, and CI integration are real. But it also is not a *mid-level professional system* because:

- No containerization or reproducible deployment
- No external monitoring or alerting
- No authentication layer
- Test coverage is thin on the most complex and risk-bearing code (tax engine, FIFO lot tracking)
- Configuration is split between UI state and hardcoded constants with no versioning

The codebase reads like the work of **one senior-to-staff individual contributor** building for personal production use, not a team-maintained service. The architecture decisions are largely sound, the financial domain knowledge is deep, but the operational envelope is that of an internal tool, not a customer-facing system.

---

## C. CRITICAL ISSUES (ranked by severity and urgency)

### C1. No Authentication or Rate Limiting
**Severity: HIGH | Urgency: IMMEDIATE if externally accessible**

The app is public on Streamlit Cloud. Any visitor can trigger expensive backtests (N=750 symbols, 10-year range) that consume CPU, memory, and yfinance API quota. There is no user identity, no per-user quota, and no circuit breaker. A single malicious actor (or scraper) can exhaust the compute budget indefinitely.

**Files:** `app_backtest.py`, `app.py`, `jobs.py`  
**Remediation:** Enable Streamlit Cloud authentication (email or GitHub SSO). Add per-user backtest quota (max N concurrent, max M per hour) via the existing `jobs.py` user_token mechanism.

---

### C2. yfinance Single Point of Failure — No Retry, No Fallback
**Severity: HIGH | Urgency: SHORT-TERM**

All live data acquisition — screener OHLCV and backtest delta — routes through `yfinance.download()`. yfinance is a community reverse-engineering of Yahoo Finance's undocumented API. It breaks silently when Yahoo changes its internal schema (has happened 4+ times in 3 years). There is:
- No HTTP retry with backoff (`data.py:445`, `data_backtest.py:207`)
- No timeout guard beyond the single-flight 300s latch
- No fallback data source (no NSE direct API, no Quandl/Tiingo failover)
- No alerting when yfinance returns an empty DataFrame for a known symbol

**Files:** `data.py:445–451`, `data_backtest.py:207–214`  
**Remediation:** Wrap all yfinance calls with tenacity retry (3 attempts, exponential backoff). Validate returned DataFrame schema before writing to Parquet. Alert (log + Streamlit warning) if > 5% of requested symbols return empty data.

---

### C3. Tax Engine Lacks Unit Tests — Critical Financial Logic Untested
**Severity: HIGH | Urgency: SHORT-TERM**

`_compute_fy_tax()` (backtest_engine.py:122–199) is 78 lines implementing India's complex CGT rules: current-year ST/LT netting, cross-type loss offsetting, 8-year carry-forward with expiry. This function directly drives the reported post-tax CAGR shown to users. Errors here cause *silent incorrect results* — there is no runtime signal that the tax is wrong.

The explore agents found 80+ tests total, but the test files are weighted toward stage2_engine and momentum_engine scoring. Critical test gaps:
- Loss carry-forward expiry (year 9 loss should NOT offset year 10 gains)
- ST loss offsetting LT gains (allowed) vs LT loss offsetting ST gains (NOT allowed in India)
- Cross-FY boundary: sale in March vs April of same calendar year
- Zero-tax year: all unrealized, no disposals
- Full portfolio liquidation in year 1 with large ST losses

**Files:** `backtest_engine.py:122–199`, `tests/`  
**Remediation:** Write parametrized unit tests for `_compute_fy_tax()` using known portfolios with computed-by-hand expected outputs. Add golden-data regression test pinning a full 10-year backtest NAV to a CSV baseline.

---

### C4. Unbounded Delta Cache Growth
**Severity: MEDIUM | Urgency: SHORT-TERM**

`backtest_delta.parquet` and `benchmarks_delta.parquet` accumulate every yfinance sync indefinitely. There is no rotation, cleanup, or size cap. Over months of daily use, this file grows without bound and is never purged.

**Files:** `data_backtest.py` (delta write path)  
**Remediation:** On each refresh, drop rows older than configurable horizon (default 90 days). Log cache size after writes.

---

### C5. No External Observability
**Severity: MEDIUM | Urgency: MEDIUM-TERM**

All error signals live inside the Streamlit UI. If the app crashes silently (Streamlit Cloud worker restart, OOM, yfinance timeout), there is no external alert. The only monitoring is a daily keep-alive ping (`wake-streamlit.yml`) that does not verify data freshness or app correctness.

**Files:** `.github/workflows/wake-streamlit.yml`  
**Remediation:** Integrate Sentry (free tier). Add a data-freshness health check endpoint that CI or a monitoring cron can probe. Log rebalance run duration, error counts, and cache hit rate.

---

### C6. Compositions Parquet Staleness is a Warning, Not an Error
**Severity: MEDIUM | Urgency: SHORT-TERM**

`compositions.parquet` drives the survivorship-bias filter — arguably the most important data correctness property of the backtester. If it is stale, backtests silently include de-listed stocks in historical universes. The current code (`data.py:152–160`) issues a `st.warning()` but continues. This is the wrong default for financial correctness.

**Files:** `data.py:152–160`  
**Remediation:** Escalate to `st.error()` with a hard block on backtest execution if compositions are > 30 days stale. Provide a one-click refresh button.

---

### C7. Large Binary Files Committed Directly to Git
**Severity: MEDIUM | Urgency: MEDIUM-TERM**

`backtest_history.parquet` and `benchmarks.parquet` are committed directly to the repository. As the OHLCV baseline grows over time, these files will cause `git clone` to become slow and the `.git` object store to bloat. No Git LFS is configured.

**Files:** `data/backtest_history.parquet`, `data/benchmarks.parquet`  
**Remediation:** Migrate to Git LFS (`git lfs track "*.parquet"`). Or move to S3/GCS with a startup download script. Set `.gitattributes` accordingly.

---

## D. DETAILED FINDINGS

---

### D1. OVERALL ENGINEERING QUALITY

**Assessment: Competent mid-level to senior-individual engineering**

#### D1.1 Code Professionalism
The code is consistently formatted (black, isort), uses snake_case throughout, avoids magic strings, and provides meaningful variable names. The backtest engine (`backtest_engine.py`) is the longest file at 1,466 lines and is well-internally-organized into logical sections with blank-line separators. However, several functions are too long:

- `rank_universe_at_date()` (backtest_engine.py:346–482): 135 lines, three responsibilities — input validation, ranking, exclusion reason capture. Should be split.
- `_get_or_load_ohlcv_for_backtest()` (data_backtest.py): complex loading with 6 execution branches in one function.
- `render_backtest_tabs()` (app_backtest.py): enormous UI function rendering the entire backtest result; no sub-render functions.

**Evidence:**
```python
# backtest_engine.py:346 — rank_universe_at_date does too much:
def rank_universe_at_date(df_all, date, m, n, top_m, use_stage2,
                          stage2_scores, momentum_scores, compositions,
                          return_exclusions=False):
    # 135 lines: validates inputs, applies quality filters, applies
    # stage2/momentum scoring, ranks, captures exclusion reasons.
    # Should be: validate() + apply_filters() + rank() + explain()
```

#### D1.2 Architecture Quality
**Strong.** The three-layer architecture (UI → Workers → Engines + Data) is clean and intentional:
- UI layers (`app.py`, `app_backtest.py`, `app_live_signal.py`) call workers or engines
- `workers.py` bridges Streamlit session state to background threads
- Engines (`stage2_engine.py`, `momentum_engine.py`, `backtest_engine.py`) are pure Python
- Data modules (`data.py`, `data_backtest.py`) encapsulate all I/O

Zero circular imports observed. Dependency graph is a DAG. This is an intentional design, not an accident.

#### D1.3 Modularity & Cohesion
The engines are highly cohesive. `stage2_engine.py` does exactly one thing (score Stage 2 criteria). `momentum_engine.py` does exactly one thing (compute Sharpe-based momentum scores). These are well-bounded.

The data modules (`data.py` at 811 lines, `data_backtest.py` at 399 lines) are doing more — I/O, caching, delta fetching, validation, and schema evolution. This is acceptable given their role as adapters, but `data.py` could be split: `data_cache.py` (in-memory/Parquet tier) + `data_fetch.py` (yfinance tier).

#### D1.4 Technical Debt
Moderate. The debt that exists is localized:
- Large functions (see above) that resist unit testing
- Hardcoded defaults in Streamlit sidebar widgets (no config file)
- Duplicate logic: Stage 2 score application exists in both `screener` path and `backtest` path — verify they use the same thresholds
- `RETEST_TOLERANCE`, `CIRCUIT_LEVELS`, `MIN_VOLUME` defined in `config.py` but other constants (e.g., `_VOL_WINDOW = 252` in momentum_engine, `_JOB_TTL_SECONDS = 3600` in jobs.py) are module-private — inconsistent

---

### D2. PRODUCTION READINESS

**Assessment: Internal tool readiness, not external service readiness**

#### D2.1 Error Handling
**Adequate for interactive use, insufficient for autonomous operation.**

- yfinance failures: caught, logged, returns empty DataFrame. No retry. Silently returns stale data.
- Parquet corruption: not detected. If `os.replace()` fails mid-write (power loss), the temp file is orphaned, not cleaned up.
- Session state corruption: if Streamlit session state loses the job_id mid-backtest, the result is lost silently.
- Backtest engine validation: schema validation exists (lines 99–109) but validates types, not ranges (e.g., negative prices allowed).

#### D2.2 Idempotency & Concurrency Safety
**Good for the data tier, weak for the job tier.**

- Atomic Parquet writes (`data.py:183–202`): idempotent, crash-safe. ✓
- yfinance single-flight latch (`data.py:388–401`): prevents stampede. ✓
- Job registry (`jobs.py`): per-user-per-kind isolation, TTL eviction. ✓
- Concurrent backtests by the same user: the existing job registry cancels old jobs when a new one starts for the same (user, kind). But this means a user accidentally double-clicking "Run Backtest" cancels their first run silently.

#### D2.3 Resiliency Gaps
- **No circuit breaker** for yfinance (if Yahoo is down, every screener load blocks for 300s)
- **No backpressure** — if 10 users run simultaneous 10-year backtests, the 4-thread pool queues them all, UI shows "Loading..." with no progress
- **No graceful degradation** — if `compositions.parquet` is corrupted, the backtest fails with an exception rather than falling back to an unfiltered universe with a warning

#### D2.4 Configuration Management
**Fragmented.** Parameters come from four sources:
1. `config.py` (global constants)
2. Module-level private constants (`_VOL_WINDOW`, `_JOB_TTL_SECONDS`)
3. Streamlit sidebar widget defaults (hardcoded in `app_backtest.py`)
4. `BacktestConfig` dataclass (runtime, good)

There is no environment-variable-based config override. Changing `MIN_VOLUME` for testing requires editing `config.py`. Streamlit sidebar defaults (e.g., `bt_m=20`, `bt_n=5`) cannot be tuned per-deployment without code changes.

---

### D3. ARCHITECTURE REVIEW

**Assessment: Intentionally designed, appropriate abstractions, one scaling ceiling**

#### D3.1 Architectural Patterns
- **Repository pattern** (partial): `data.py` and `data_backtest.py` act as repositories, encapsulating storage details from engines. Well done.
- **Background job pattern**: `workers.py` + `jobs.py` decouple computation from UI rendering. Standard and appropriate.
- **Event streaming**: Job progress fed to UI via event queue (up to 50 messages). Correct pattern for Streamlit's single-threaded rendering model.
- **Precomputation**: `_precompute_all_metrics()` converts O(n²) naive per-date scoring into O(n) batch + O(log n) lookup. Demonstrates algorithmic awareness.

#### D3.2 Abstraction Appropriateness
Abstractions match the problem size. No over-engineering observed. The `BacktestConfig` dataclass is the right size — it captures all runtime parameters without becoming an anemic data holder.

The one over-abstraction risk: `_precompute_stage2_scores()` and `_precompute_momentum_scores()` in `backtest_engine.py` duplicate similar logic that also exists in the screener path. If the scoring criteria diverge between screener and backtester (e.g., different MA windows), results will be inconsistent. Should share a single scoring function.

#### D3.3 State Management
Streamlit session state is used correctly — jobs are keyed by `user_token + job_kind`, results cached per-session. The `user_token` UUID is generated on first session access and persists for the session lifetime.

**Gap:** No persistent state per user across sessions. A user who refreshes the page loses their backtest results. For an interactive research tool this is acceptable; for a production tool, results should be persisted to disk and reloadable.

#### D3.4 API Design
**None exists.** The system has no programmatic interface. All interactions are through Streamlit widgets. This is the largest architectural limitation:
- Cannot be integrated into external portfolio systems
- Cannot be run headlessly (e.g., nightly batch)
- Cannot be tested end-to-end without Streamlit
- The existing `workers.py` / `jobs.py` pattern is close to being a proper async API — extracting it would be low-effort

#### D3.5 Data Flow
```
yfinance (external) → data.py/data_backtest.py (I/O layer)
                        ↓
                    Parquet files (persistence)
                        ↓
                    In-memory dict cache (hot tier)
                        ↓
              stage2_engine / momentum_engine (scoring)
                        ↓
                  backtest_engine (portfolio simulation)
                        ↓
                    charts.py (visualization)
                        ↓
               app.py / app_backtest.py (Streamlit UI)
```

The data flow is unidirectional and clearly defined. No backwards dependencies observed.

---

### D4. SECURITY REVIEW

**Assessment: Acceptable for private use; inadequate for public deployment**

#### D4.1 Authentication & Authorization
**Critical Gap.** There is no authentication. The Streamlit Cloud deployment is public. Any user can:
- Trigger compute-intensive backtests with adversarial parameters (N=750, 10-year range, daily rebalance)
- Consume the full yfinance rate quota for legitimate users
- Access live signals that reveal the owner's trading intent

**Severity: HIGH** (if publicly accessible)  
**Files:** `app.py`, `app_backtest.py`

#### D4.2 Input Validation
**Partially implemented.**
- Ticker regex: `^[A-Z0-9&\-]{1,20}$` in `data.py:654` — prevents injection ✓
- Date parsing: explicit `strptime` format — safe ✓  
- Numeric sliders: Streamlit enforces min/max — safe ✓
- **Gap:** End date before start date — `BacktestConfig` validates `start_date < end_date` but the UI allows it to be submitted and shows a confusing error
- **Gap:** M > N (hold more than screened) — caught at runtime but error message is technical
- **Gap:** Position cap sum > 100% is not validated — if user sets 6 positions × 25% cap = 150%, the water-fill algorithm silently clamps

**Severity: LOW–MEDIUM**

#### D4.3 Injection Risks
**Low risk.** No SQL, no shell execution, no eval(). The only external execution is `yfinance.download()` which accepts ticker strings — the regex validation (`^[A-Z0-9&\-]{1,20}$`) is sufficient to prevent injection. No dynamic import or subprocess execution observed.

#### D4.4 Secrets Exposure
**Low risk currently.** No API keys in code. `.env.example` is present but the app uses no environment secrets. If yfinance credentials or NSE API keys are added in future, they must go into environment variables, not hardcoded.

**Note:** The `.env.example` references `DATABASE_URL` which does not exist in the current implementation. This dead reference should be removed to avoid confusion.

#### D4.5 Data Privacy
**Low risk.** No user PII stored. Session state is in-memory only. Live signals tab exposes the owner's trading intent (quantities, symbols) — if publicly accessible, this is a trading-security concern.

#### D4.6 Dependency Supply Chain
**Medium risk.** yfinance (`>=0.2.36`) is the only community-maintained dependency without a formal security model. All other dependencies (streamlit, pandas, numpy, plotly) are enterprise-supported. Pinning exact versions in `requirements.txt` mitigates transitive dependency risks.

---

### D5. PERFORMANCE & SCALABILITY

**Assessment: Optimized for single-user interactive use; will not scale horizontally**

#### D5.1 Algorithmic Efficiency

| Operation | Complexity | Status |
|---|---|---|
| Stage 2 scoring per symbol | O(n) vectorized | Excellent |
| Momentum Sharpe computation | O(n × w) vectorized | Excellent |
| Precompute all metrics | O(symbols × days) batch | Good |
| Rebalance loop per date | O(holdings × log(metrics)) | Good (after precompute) |
| Tax FIFO lot matching | O(sales × lots) per FY | Acceptable |
| Weight water-fill | O(holdings²) worst case | Low holdings count |

Algorithmic choices are appropriate. The precomputation pattern (`_precompute_stage2_scores`, `_precompute_all_metrics`) demonstrates understanding of the loop structure — score once, look up many times. No O(n²) hotspots under normal usage.

#### D5.2 Memory Model
- **Screener OHLCV:** ~750 symbols × 2 years × 5 columns × float32 ≈ 27 MB in-memory
- **Backtest OHLCV:** ~750 symbols × 10 years × 5 columns × float32 ≈ 135 MB in-memory
- **In-memory dict cache** (`_ohlcv_cache`): bounded by TTL (600s) but has no max-size cap

**Risk:** If cache TTL expires during a long backtest, the cache refills while the backtest runs, potentially doubling memory. On Streamlit Cloud (1 GB RAM limit), this could OOM.

**Severity: MEDIUM**  
**Remediation:** Add `maxsize` to the in-memory cache; use `functools.lru_cache` or a bounded dict.

#### D5.3 I/O Efficiency
- Parquet reads: per-symbol dict of DataFrames loaded once at backtest start ✓
- Parquet writes: atomic writes with snappy compression ✓
- yfinance: single-flight latch prevents redundant calls ✓
- **Gap:** `data_backtest.py` reads all 750 symbol Parquets sequentially at backtest start; no parallel I/O

**Remediation:** Use `concurrent.futures.ThreadPoolExecutor` to parallelize Parquet reads (I/O-bound, GIL not an issue). Expected speedup: 2–4×.

#### D5.4 Scaling Ceilings

| Ceiling | Current Limit | Trigger |
|---|---|---|
| Concurrent users | ~4 (ThreadPoolExecutor) | Each backtest occupies 1 thread |
| Symbol universe | ~1,500 before OOM | Memory-bound |
| History length | ~15 years before slowdown | Loop bound |
| Rebalance frequency | Daily (250/year) at linear cost | Acceptable |

The app will not horizontally scale. Each Streamlit Cloud instance is stateful (in-memory cache) and the job registry is process-local. Multi-instance deployment would require shared state (Redis cache, shared Parquet on S3).

---

### D6. TESTING & RELIABILITY

**Assessment: Adequate unit testing of scoring engines; critical financial logic under-covered**

#### D6.1 Test Coverage Summary

| Module | Estimated Coverage | Quality |
|---|---|---|
| `stage2_engine.py` | ~85% | Good — vectorized scoring well-tested |
| `momentum_engine.py` | ~80% | Good — Sharpe variants, edge cases |
| `backtest_engine.py` | ~45% | Weak — tax engine, FIFO, walk-forward untested |
| `data.py` | ~5% | Poor — all I/O, hard to test without yfinance mock |
| `data_backtest.py` | ~5% | Poor — same |
| UI layers (`app*.py`) | ~0% | Acceptable — Streamlit untestable without browser |

The overall picture: the parts that are tested are well-tested with good parametrization. The parts that are *not* tested include the most financially consequential logic.

#### D6.2 Test Quality
**Strengths:**
- `@pytest.mark.parametrize` used correctly for multiple input scenarios
- `make_ohlcv()` fixture provides clean synthetic data
- `math.isclose(result, expected, rel_tol=1e-9)` — appropriate float tolerance
- Edge cases covered for scoring: empty data, below-minimum history, zero volatility

**Weaknesses:**
- Test constants are magic numbers without explanation (`close = [80.0 + i * 0.25 for i in range(79)]` — why 79? why 0.25?)
- No property-based tests (hypothesis) for financial invariants (tax ≥ 0, NAV non-negative, weight sum = 1.0)
- No integration tests — screener and backtest run in isolation; their shared code paths (Stage 2 scoring criteria, quality filters) are not tested together
- No performance regression tests — a code change that makes the 10-year backtest 10× slower would not be caught by CI

#### D6.3 CI Quality
**File:** `.github/workflows/ci.yml`

```yaml
# Current CI:
- pre-commit (black, isort, flake8)
- pytest tests/

# Missing:
- mypy type checking
- pytest-cov with coverage threshold
- integration test suite
- performance benchmark
```

The CI is a floor, not a ceiling. Passing CI means "no obvious style errors and unit tests pass." It does not mean "financial logic is correct" or "performance is acceptable."

#### D6.4 Release Confidence
**Low-to-moderate.** A developer can confidently refactor `stage2_engine.py` because it is well-tested. A developer cannot confidently refactor `_compute_fy_tax()` because the test coverage is insufficient to catch a subtle tax-rule change.

---

### D7. DEVOPS & OPERATIONAL MATURITY

**Assessment: Minimal viable CI/CD; no operational monitoring**

#### D7.1 CI/CD
- **GitHub Actions CI:** pre-commit + pytest on every push ✓
- **Keep-alive workflow:** daily ping to prevent Streamlit Cloud idle shutdown ✓
- **No staging environment:** changes go directly from CI to production
- **No deployment workflow:** Streamlit Cloud auto-deploys from `main` branch (assumed)

**Risk:** A broken commit to `main` immediately appears in production. No canary, no blue/green, no rollback automation.

#### D7.2 Containerization
**None.** No Dockerfile, no `docker-compose.yml`, no `.devcontainer/devcontainer.json` build steps. The repository has a `devcontainer.json` reference but it appears to be a stub. Onboarding a new developer requires manual `pip install -r requirements.txt` and manual Parquet data setup.

#### D7.3 Environment Management
**Partially managed.** `requirements.txt` pins exact versions (good). `requirements-dev.txt` exists (good). No `pyproject.toml` — the project is not installable as a package, which limits testing flexibility.

#### D7.4 Secrets Management
**Adequate for current needs.** No API secrets currently required. `.env.example` is present as a template. If API credentials are added (NSE API, Sentry DSN), Streamlit Cloud secrets store should be used — no evidence of planned secret handling.

#### D7.5 Observability Stack
**None.** There is no:
- Error reporting (Sentry, Rollbar)
- Performance monitoring (Datadog, New Relic)
- Log aggregation (Papertrail, Logtail)
- Data freshness dashboards
- Uptime monitoring (UptimeRobot, Pingdom)

The only operational signal is the daily keep-alive ping, which tests availability but not correctness.

#### D7.6 Health Checks
**None automated.** There is no `/health` endpoint, no startup validation script, and no CI probe for data freshness. The app self-reports staleness in the UI (`data.py:69–164`) but this requires a human to notice it.

#### D7.7 Operational Maturity Level
**Level 1 of 4:** "It runs on a server." No runbook, no incident response plan, no on-call rotation, no SLO.

---

### D8. CODE SMELLS & ANTI-PATTERNS

#### D8.1 Oversized Functions
**`rank_universe_at_date()` — backtest_engine.py:346–482 (135 lines)**
Three responsibilities: validate inputs, rank symbols by quality filters + scoring, optionally capture exclusion reasons. The `return_exclusions=False` flag is a code smell — a function that does fundamentally different things based on a flag should be two functions.

**`render_backtest_tabs()` — app_backtest.py (entire function)**
Likely hundreds of lines rendering the full backtest UI in one function. No sub-render functions. Impossible to unit-test, difficult to modify safely.

#### D8.2 Duplicated Scoring Paths
Stage 2 scoring is applied in:
1. Screener path (`app.py` → `stage2_engine.score_stage2()`)
2. Backtest path (`backtest_engine._precompute_stage2_scores()`)

If the screening criteria thresholds (RSI floor, MA window, volume multiplier) differ between these two paths, a stock could appear in screener results but not in backtest universe, or vice versa. This is a logic correctness risk, not just a style issue.

**Evidence needed:** Compare the scoring parameters passed in both paths to verify they are identical.

#### D8.3 Magic Constants
```python
# momentum_engine.py
_VOL_WINDOW = 252           # not exported to config.py
_SHARPE_MIN_PERIODS = 20    # not documented why 20

# stage2_engine.py  
RETEST_TOLERANCE = 0.02     # in config.py — good, but why 2%?
CONSOL_RANGE = 0.15         # consolidation band — should be configurable

# jobs.py
_JOB_TTL_SECONDS = 3600     # 1 hour — not user-configurable

# data.py
STALE_THRESHOLD_CONSTITUENTS = 30   # days
STALE_THRESHOLD_HOLIDAYS = 180      # days
```

Some constants are in `config.py` (good), others are module-private without documentation (mixed). All financial thresholds should be in `config.py` with comments explaining their derivation.

#### D8.4 `return_exclusions` Flag Pattern
```python
def rank_universe_at_date(..., return_exclusions=False):
    ...
    if return_exclusions:
        return ranked, exclusion_reasons
    return ranked
```

This is a well-known anti-pattern. The caller cannot know from the type signature what type is returned. Use two functions or always return both values as a named tuple.

#### D8.5 Stale `.env.example`
`.env.example` references `DATABASE_URL` which is not used anywhere in the codebase. This is misleading for new contributors and suggests a prior architecture that no longer exists.

#### D8.6 Compositions Staleness Downgraded to Warning
As noted in Critical Issues (C6), this is a semantic anti-pattern: treating a data-correctness failure as a warning when the data drives the core accuracy claim (anti-survivorship-bias). The severity classification of the alert should match the severity of the impact.

#### D8.7 Silent Empty Universe Fallback
```python
# backtest_engine.py:1106–1107
if not ranked:
    ranked = top_m or current_holdings  # silent fallback
```

If all symbols fail quality filters, the backtest silently holds previous positions. This can mask a configuration error (e.g., too-strict quality filter wiping out the entire universe). At minimum, this should emit a warning to the user.

---

### D9. TEAM & MAINTAINABILITY ASSESSMENT

**Assessment: Maintainable by its author; high onboarding friction for newcomers**

#### D9.1 Onboarding Difficulty
**High.** A new engineer would face:

1. **No ARCHITECTURE.md** — must reverse-engineer the three-tier data/engine/UI structure from reading code
2. **No DEPLOYING.md** — Parquet baseline setup, Streamlit Cloud secrets, refresh script invocation all undocumented
3. **`compositions.parquet` mystery** — critical for correctness, but how it is generated and maintained is not documented for a newcomer
4. **Complex backtest engine** — 1,466 lines, no function-level docstrings on key operations (`run_backtest`, `rank_universe_at_date`, `_compute_fy_tax`)
5. **India tax rules assumed knowledge** — `_compute_fy_tax` is only understandable by someone who knows India CGT — no inline explanation

#### D9.2 Bus Factor
**1.** This is effectively a single-author codebase. All domain knowledge (India CGT rules, NSE data quirks, yfinance workarounds, composition filter logic) lives in the author's head and partially in the code. No handover documentation exists.

#### D9.3 Documentation Quality

| Document | Status |
|---|---|
| README.md | Good — feature overview, usage instructions |
| PRODUCT_EVOLUTION.md | Good — history and rationale |
| Backtest user guide | Good — explains 6 strategies |
| Code docstrings | Sparse — engine functions lack function-level docs |
| Architecture docs | None |
| Deployment docs | None |
| Runbook | None |

#### D9.4 Cognitive Load
**Moderate-to-high** for the backtest engine. Understanding `run_backtest()` requires simultaneously holding:
- 6 portfolio variant state machines
- FIFO lot tracking per variant
- Per-FY tax accumulation per variant
- Weight cap enforcement logic
- Composition filter application per rebalance date

This is inherent domain complexity, but it could be reduced by splitting the function and adding inline comments explaining *why* (not *what*).

#### D9.5 Refactor Safety
**Moderate.** Pure engine functions (`stage2_engine.py`, `momentum_engine.py`) are safe to refactor — good test coverage. `backtest_engine.py` is fragile — refactoring `_compute_fy_tax` or the lot-tracking logic without comprehensive tests risks introducing silent financial errors.

#### D9.6 Would a Professional Team Maintain This?
**Yes, with reservations.** The architecture is sound enough that a professional team could take it over. The reservations are:
- Significant documentation gap requires tribal knowledge transfer
- Tax engine tests must be written before any team member touches that code
- The `render_backtest_tabs` function needs decomposition before UI work can be parallelized across team members
- The yfinance dependency is a team-level risk — someone needs to own monitoring it for breakage

---

## E. REFACTORING ROADMAP

### Immediate (Week 1–2) — Risk Reduction

| # | Action | File(s) | Effort | Risk Reduction |
|---|---|---|---|---|
| 1 | Enable Streamlit Cloud authentication | App config | 2h | Eliminates public exposure |
| 2 | Escalate compositions staleness to error+block | `data.py:152–160` | 1h | Prevents silently incorrect backtests |
| 3 | Add retry + backoff to yfinance calls | `data.py:445`, `data_backtest.py:207` | 4h | Eliminates yfinance SPOF |
| 4 | Write tax engine unit tests | `tests/test_tax_engine.py` | 1d | Catches silent financial errors |
| 5 | Remove stale `DATABASE_URL` from `.env.example` | `.env.example` | 15m | Reduces onboarding confusion |
| 6 | Add `st.warning` on silent empty-universe fallback | `backtest_engine.py:1106` | 30m | Makes config errors visible |

### Short-Term (Month 1) — Quality Improvements

| # | Action | File(s) | Effort | Impact |
|---|---|---|---|---|
| 7 | Write ARCHITECTURE.md (data flow, module roles) | New file | 4h | Onboarding: high |
| 8 | Write DEPLOYING.md (Streamlit Cloud + local) | New file | 2h | Onboarding: high |
| 9 | Add docstrings to `run_backtest`, `_compute_fy_tax`, `rank_universe_at_date` | `backtest_engine.py` | 4h | Maintainability: medium |
| 10 | Split `rank_universe_at_date` into rank + explain | `backtest_engine.py:346` | 1d | Testability: high |
| 11 | Add delta cache rotation (keep last 90 days) | `data_backtest.py` | 2h | Ops: medium |
| 12 | Add mypy to CI | `.github/workflows/ci.yml` | 2h | Quality gates: medium |
| 13 | Add golden-data regression test (10-year NAV baseline) | `tests/` | 1d | Release confidence: high |
| 14 | Consolidate all financial thresholds into `config.py` | `config.py`, `momentum_engine.py`, `stage2_engine.py` | 3h | Maintainability: medium |

### Medium-Term (Quarter 1) — Architecture Improvements

| # | Action | File(s) | Effort | Impact |
|---|---|---|---|---|
| 15 | Migrate large Parquets to Git LFS | `.gitattributes`, data pipeline | 1d | Repo health: high |
| 16 | Add Sentry error reporting | `app.py`, `app_backtest.py` | 4h | Observability: high |
| 17 | Integrate structured logging (JSON) | All modules | 2d | Auditability: medium |
| 18 | Add data freshness health check endpoint | New `health.py` | 4h | Monitoring: high |
| 19 | Parallelize Parquet reads at backtest start | `data_backtest.py` | 1d | Performance: 2–4× speedup |
| 20 | Add integration test: screener → backtest pipeline | `tests/test_integration.py` | 1d | Confidence: high |
| 21 | Decompose `render_backtest_tabs` into sub-renderers | `app_backtest.py` | 2d | Maintainability: medium |
| 22 | Add per-user backtest quota via `jobs.py` | `jobs.py`, `app_backtest.py` | 1d | Security/Ops: medium |

### Long-Term (Quarter 2–3) — Strategic Improvements

| # | Action | Effort | Impact |
|---|---|---|---|
| 23 | Extract headless API layer (FastAPI on top of engines) | 1 week | Integration: high |
| 24 | Add Docker + docker-compose for local dev | 1d | Portability: high |
| 25 | Move data to S3 + startup download (remove from Git) | 2d | Scale: high |
| 26 | Property-based tests with `hypothesis` for tax engine | 3d | Correctness confidence |
| 27 | Walk-forward parallelization (multiprocessing) | 2d | Performance: 3× |
| 28 | Add second data source as yfinance fallback | 3d | Resilience: high |

---

## F. FINAL VERDICT

### Would you approve this for production?

**Yes — for internal / small-team use with the Tier-1 fixes applied.**

Specifically, the authentication gap (C1) and the yfinance resilience gap (C2) must be addressed before a wider deployment. The tax engine test gap (C3) is acceptable for personal use only — any production use where the tax output informs real financial decisions requires comprehensive test coverage first.

### Would you trust this under scale?

**No — not without architectural changes.** The system is designed for one user and will not scale past ~4–5 concurrent users without hitting the ThreadPoolExecutor ceiling, yfinance rate limits, and Streamlit Cloud memory limits. Horizontal scaling requires: shared state (Redis or S3), a proper API layer, and stateless computation workers. None of these exist.

### Would experienced engineers respect this codebase?

**Yes, with caveats.** The architectural decisions (pure-function engines, atomic writes, single-flight latch, anti-survivorship-bias modeling, India CGT implementation) are sophisticated and demonstrate real engineering and domain depth. Most experienced engineers reviewing this would say: "Whoever built this knows what they're doing." The caveats — no auth, thin tests on the most critical code, 1,466-line engine file — would prompt experienced engineers to flag the operational gaps immediately.

### What engineering maturity level does the author appear to have?

**Senior individual contributor.** The author demonstrates:
- Deep financial domain knowledge (India CGT rules, Weinstein Stage 2, Sharpe ratio construction, survivorship bias)
- Solid Python engineering (concurrency patterns, vectorized pandas, atomic I/O)
- Architectural discipline (separation of concerns, no circular dependencies)
- CI hygiene (pre-commit, formatting standards)

The gaps are characteristic of a senior IC building for themselves, not a staff engineer building for a team:
- Limited documentation for others
- Bus factor of 1
- Operational concerns (monitoring, alerting, deployment automation) deprioritized
- Test coverage proportional to confidence, not risk

---

*Assessment complete. No code changes were made as part of this assessment.*
