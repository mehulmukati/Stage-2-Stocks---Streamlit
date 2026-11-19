const { test } = require('node:test');
const assert = require('node:assert/strict');
const notify = require('../.github/scripts/dhan_token_alert.cjs');

function setup(t, healthy, issues = []) {
  const old = process.env.DHAN_RENEWAL_STATUS;
  process.env.DHAN_RENEWAL_STATUS = healthy ? 'success' : 'failure';
  t.after(() => old === undefined ? delete process.env.DHAN_RENEWAL_STATUS : process.env.DHAN_RENEWAL_STATUS = old);
  const created = [], updated = [], failures = [];
  const github = {
    paginate: async () => issues,
    rest: { issues: {
      listForRepo: async () => {},
      create: async args => created.push(args),
      update: async args => updated.push(args),
    } },
  };
  return { created, updated, failures, args: {
    github, context: { repo: { owner: 'owner', repo: 'repo' }, serverUrl: 'https://github.com', runId: 1 },
    core: { setFailed: message => failures.push(message) },
  } };
}

const issue = { number: 1, user: { login: 'github-actions[bot]' }, body: '<!-- dhan-token-renewal -->' };

test('token failure opens one actionable issue', async t => {
  const f = setup(t, false);
  await notify(f.args);
  assert.equal(f.created.length, 1);
  assert.equal(f.failures.length, 1);
  assert.ok(f.created[0].body.includes('encrypted recovery'));
});

test('repeated token failure creates no duplicate', async t => {
  const f = setup(t, false, [issue]);
  await notify(f.args);
  assert.equal(f.created.length, 0);
});

test('success closes only bot-owned token alerts', async t => {
  const f = setup(t, true, [issue, { ...issue, number: 2, user: { login: 'person' } },
    { ...issue, number: 3, body: '<!-- market-refresh-recovery -->' }]);
  await notify(f.args);
  assert.deepEqual(f.updated.map(value => value.issue_number), [1]);
  assert.equal(f.created.length, 0);
});
