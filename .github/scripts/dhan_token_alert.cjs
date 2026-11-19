const marker = '<!-- dhan-token-renewal -->';

module.exports = async ({ github, context, core }) => {
  const healthy = process.env.DHAN_RENEWAL_STATUS === 'success';
  const repo = context.repo;
  const issues = await github.paginate(github.rest.issues.listForRepo, { ...repo, state: 'open', per_page: 100 });
  const alerts = issues.filter(issue => !issue.pull_request && issue.user?.login === 'github-actions[bot]' &&
    issue.body?.includes(marker));
  if (healthy) {
    for (const issue of alerts) {
      await github.rest.issues.update({ ...repo, issue_number: issue.number, state: 'closed', state_reason: 'completed' });
    }
    return;
  }
  core.setFailed('Dhan token renewal failed. Market downloads were skipped.');
  if (alerts.length) return;
  const runUrl = `${context.serverUrl}/${repo.owner}/${repo.repo}/actions/runs/${context.runId}`;
  await github.rest.issues.create({
    ...repo,
    title: 'Dhan token renewal needs attention',
    body: `${marker}\nAutomatic Dhan token renewal or persistence failed.\n\n` +
      'Check the active Dhan Web token, Data API subscription and DHAN_TOKEN_WRITER permissions/expiry. ' +
      'If an encrypted recovery artifact exists, restore it before attempting another renewal. ' +
      'An expired token needs a fresh Dhan Web login.\n\n' +
      `[Workflow run](${runUrl}).\n\nThis issue closes after token renewal succeeds.`,
  });
};
