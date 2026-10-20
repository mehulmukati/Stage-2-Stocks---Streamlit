const fs = require('node:fs');

const marker = '<!-- market-refresh-recovery -->';

module.exports = async ({ github, context, core }) => {
  let report = {};
  const path = 'outputs/market_refresh_status.json';
  if (fs.existsSync(path)) report = JSON.parse(fs.readFileSync(path, 'utf8'));
  const finalAttempt = report.final_attempt || context.payload.schedule === '15 3 * * *' ||
    String(context.payload.inputs?.final_attempt) === 'true';
  const healthy = process.env.REFRESH_JOB_STATUS === 'success' &&
    ['already_current', 'recovered'].includes(report.status);
  if (!healthy && !finalAttempt) return;

  const repo = context.repo;
  const issues = await github.paginate(github.rest.issues.listForRepo, { ...repo, state: 'open', per_page: 100 });
  const alerts = issues.filter(issue => !issue.pull_request && issue.user?.login === 'github-actions[bot]' &&
    issue.body?.includes(marker));
  const runUrl = `${context.serverUrl}/${repo.owner}/${repo.repo}/actions/runs/${context.runId}`;
  if (healthy) {
    for (const issue of alerts) {
      const target = issue.title.match(/^Market data refresh failed: (\d{4}-\d{2}-\d{2})$/)?.[1];
      if ((target && target <= report.target_date) || issue.title === 'Market data refresh failed: target could not be determined') {
        await github.rest.issues.update({ ...repo, issue_number: issue.number, state: 'closed',
          state_reason: 'completed', body: `${issue.body}\n\nRecovered through ${report.target_date}. [Verified run](${runUrl}).` });
      }
    }
    return;
  }
  const target = report.target_date || 'target could not be determined';
  core.setFailed(`Final recovery failed for ${target}; previous accepted prices remain in use.`);
  const title = `Market data refresh failed: ${target}`;
  if (alerts.some(issue => issue.title === title)) {
    core.warning(`An alert already exists for ${target}; no duplicate notification created.`);
    return;
  }
  const missing = report.missing_target_symbols || [];
  const body = `${marker}\nThe final scheduled recovery attempt failed. Previous accepted prices remain in use.\n\n` +
    `- Expected session: ${target}\n` +
    `- Latest accepted price date: ${report.max_price_date || 'unavailable'}\n` +
    `- Current stocks covering target: ${report.target_session_symbol_count || 0}/${report.current_symbol_count || 'unknown'}\n` +
    `- Missing benchmarks: ${(report.missing_target_benchmarks || []).join(', ') || 'see run log'}\n` +
    `- Missing stocks (${missing.length}): ${missing.slice(0, 40).join(', ') || 'see run log'}\n` +
    `- Failure: ${report.error || 'Workflow failed before a recovery report was produced; see failed step.'}\n\n` +
    `[Run log and full recovery artifact](${runUrl}).\n\n` +
    'This issue closes automatically after a successful publication covers this session or a newer one.';
  const created = await github.rest.issues.create({ ...repo, title, body });
  core.error(`Final recovery failed: ${created.data.html_url}`);
};
