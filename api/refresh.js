// Trigger the GitHub Actions scrape workflow.
// Rate-limited to once per day by checking fetched_at in the live movies.json,
// unless the request includes the correct ADMIN_TOKEN.

const GITHUB_TOKEN  = process.env.GITHUB_TOKEN;  // PAT with workflow scope
const ADMIN_TOKEN   = process.env.ADMIN_TOKEN;    // secret for bypassing daily cap

const REPO_OWNER    = 'desseabar';
const REPO_NAME     = 'What-Cool-Movies-are-in-NYC';
const WORKFLOW_FILE = 'scrape.yml';
const RAW_MOVIES    = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/main/docs/movies.json`;

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');

  const providedToken = req.query.token || req.headers['x-admin-token'];
  const isAdmin = ADMIN_TOKEN && providedToken === ADMIN_TOKEN;

  if (!isAdmin) {
    try {
      const r = await fetch(RAW_MOVIES, { headers: { 'Cache-Control': 'no-cache' } });
      if (r.ok) {
        const data = await r.json();
        if (data.fetched_at) {
          const tz = 'America/New_York';
          const lastDate = new Date(data.fetched_at).toLocaleDateString('en-US', { timeZone: tz });
          const today    = new Date().toLocaleDateString('en-US', { timeZone: tz });
          if (lastDate === today) {
            return res.json({ status: 'already_refreshed', message: 'Already refreshed today!' });
          }
        }
      }
    } catch (_) {
      // Can't read current date — proceed and let the workflow run
    }
  }

  if (!GITHUB_TOKEN) {
    return res.status(500).json({ status: 'error', message: 'Server not configured.' });
  }

  const ghRes = await fetch(
    `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/workflows/${WORKFLOW_FILE}/dispatches`,
    {
      method: 'POST',
      headers: {
        Authorization:          `Bearer ${GITHUB_TOKEN}`,
        Accept:                 'application/vnd.github+json',
        'Content-Type':         'application/json',
        'X-GitHub-Api-Version': '2022-11-28',
      },
      body: JSON.stringify({ ref: 'main' }),
    }
  );

  if (ghRes.status === 204) {
    return res.json({ status: 'triggered', message: 'Refresh started! Listings will update in a few minutes.' });
  }

  const errBody = await ghRes.text().catch(() => '');
  console.error('GitHub dispatch error', ghRes.status, errBody);
  return res.status(502).json({ status: 'error', message: `Could not start refresh (GitHub ${ghRes.status}).` });
};
