const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const notify = require('../.github/scripts/market_refresh_alert.cjs');

function setup(t, { report, status = 'failure', issues = [], schedule = '15 3 * * *' } = {}) {
  t.mock.method(fs, 'existsSync', () => Boolean(report));
  t.mock.method(fs, 'readFileSync', () => JSON.stringify(report));
  const oldStatus = process.env.REFRESH_JOB_STATUS;
  process.env.REFRESH_JOB_STATUS = status;
  t.after(() => oldStatus === undefined ? delete process.env.REFRESH_JOB_STATUS : process.env.REFRESH_JOB_STATUS = oldStatus);
  const created = [], updated = [], failed = [];
  const github = {
    paginate: async () => issues,
    rest: { issues: {
      listForRepo: async () => {},
      create: async args => { created.push(args); return { data: { html_url: 'https://github.com/owner/repo/issues/1' } }; },
      update: async args => { updated.push(args); },
    } },
  };
  const context = { repo: { owner: 'owner', repo: 'repo' }, payload: { schedule },
    serverUrl: 'https://github.com', runId: 42 };
  return { args: { github, context, core: { warning() {}, error() {}, setFailed(message) { failed.push(message); } } },
    created, updated, failed };
}

const failed = { target_date: '2026-10-19', status: 'failed', final_attempt: true,
  max_price_date: '2026-10-16', target_session_symbol_count: 0, current_symbol_count: 751,
  missing_target_benchmarks: ['Nifty 50'], missing_target_symbols: ['A'], error: 'provider unavailable' };
const alert = { number: 1, title: 'Market data refresh failed: 2026-10-19',
  body: '<!-- market-refresh-recovery -->', user: { login: 'github-actions[bot]' } };

test('intermediate failure remains quiet', async t => {
  const fixture = setup(t, { report: { ...failed, final_attempt: false }, schedule: '15 15 * * *' });
  await notify(fixture.args);
  assert.equal(fixture.created.length, 0);
  assert.equal(fixture.failed.length, 0);
});

test('final failure creates one detailed alert', async t => {
  const fixture = setup(t, { report: failed });
  await notify(fixture.args);
  assert.equal(fixture.created.length, 1);
  assert.equal(fixture.failed.length, 1);
  const body = fixture.created[0].body;
  for (const text of ['2026-10-19', '2026-10-16', '0/751', 'provider unavailable', 'actions/runs/42']) {
    assert.ok(body.includes(text));
  }
});

test('unchanged final failure does not create another notification', async t => {
  const fixture = setup(t, { report: failed, issues: [alert] });
  await notify(fixture.args);
  assert.equal(fixture.created.length, 0);
  assert.equal(fixture.updated.length, 0);
  assert.equal(fixture.failed.length, 1);
});

test('recovery closes older alerts but preserves future and unrelated issues', async t => {
  const future = { ...alert, number: 2, title: 'Market data refresh failed: 2026-10-22' };
  const unrelated = { ...alert, number: 3, user: { login: 'someone' } };
  const fixture = setup(t, { status: 'success', report: { target_date: '2026-10-21', status: 'already_current' },
    issues: [alert, future, unrelated] });
  await notify(fixture.args);
  assert.deepEqual(fixture.updated.map(issue => issue.issue_number), [1]);
  assert.equal(fixture.updated[0].state, 'closed');
});

test('failed deployment after a healthy download still alerts on final attempt', async t => {
  const fixture = setup(t, { report: { ...failed, status: 'recovered' } });
  await notify(fixture.args);
  assert.equal(fixture.created.length, 1);
});

test('setup failure without a report still alerts at the final slot', async t => {
  const fixture = setup(t);
  await notify(fixture.args);
  assert.equal(fixture.created.length, 1);
  assert.ok(fixture.created[0].body.includes('before a recovery report was produced'));
});
