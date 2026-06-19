const express = require('express');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Import queue state ───────────────────────────────────────────────────────
let _job = {
  status: 'idle', total: 0, processed: 0,
  results: {}, phase: '', pauseRemaining: 0,
  // conversation lookup: rowIndex → { externalId, conversationId }
  convLookup: {},
  // expected inboxes: rowIndex → inboxName string
  expectedInboxes: {},
  // webhook results: rowIndex → { addedInbox, match }
  webhookResults: {},
};

// ─── Helpers ──────────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

function errMsg(data) {
  const e = data?._error;
  if (!e) return data?.message || null;
  return typeof e === 'object' ? (e.message || JSON.stringify(e)) : String(e);
}

async function frontGet(path, token) {
  const res  = await fetch(`https://api2.frontapp.com${path}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
  });
  const data = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, data };
}

async function frontPost(path, token, body) {
  const res  = await fetch(`https://api2.frontapp.com${path}`, {
    method:  'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', Accept: 'application/json' },
    body:    JSON.stringify(body),
  });
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch (_) { data = { raw: text }; }
  return { ok: res.ok, status: res.status, data };
}

// Look up conversation ID via alt:uid, retrying a few times to allow indexing
async function fetchConversationId(externalId, token) {
  for (let attempt = 0; attempt < 5; attempt++) {
    await sleep(attempt === 0 ? 3000 : 4000);
    try {
      const r = await frontGet(`/messages/alt:uid:${encodeURIComponent(externalId)}`, token);
      if (!r.ok) continue;
      const d = r.data;
      // Try every known location Front puts the conversation ID
      const convId =
        d?.conversation_id ||
        d?.conversation?.id ||
        d?._links?.related?.conversation?.split('/').pop() ||
        d?._results?.[0]?.conversation_id ||
        d?._results?.[0]?.conversation?.id;
      if (convId) return convId;
      // Store raw response for debugging
      console.log(`[uid lookup attempt ${attempt}] raw:`, JSON.stringify(d).slice(0, 300));
    } catch (e) {
      console.log(`[uid lookup attempt ${attempt}] error:`, e.message);
    }
  }
  return null;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.post('/api/validate', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ ok: false, error: 'Missing token.' });
  try {
    const r = await frontGet('/me', token);
    res.status(r.status).json({ ok: r.ok, data: r.data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/list-inboxes', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ ok: false, error: 'Missing token.' });
  try {
    let inboxes = [], url = '/inboxes';
    while (url) {
      const r = await frontGet(url, token);
      if (!r.ok) return res.status(r.status).json({ ok: false, error: errMsg(r.data) || `HTTP ${r.status}` });
      inboxes = inboxes.concat(r.data._results || []);
      const next = r.data._pagination?.next;
      url = next ? new URL(next).pathname + new URL(next).search : null;
    }
    res.json({ ok: true, inboxes: inboxes.map(i => ({ id: i.id, name: i.name })) });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Queue import — accepts expectedInboxes map alongside payloads
app.post('/api/queue-import', (req, res) => {
  if (_job.status === 'running') return res.status(409).json({ ok: false, error: 'Import already running.' });

  const { token, inbox_id, payloads, expectedInboxes } = req.body;
  if (!token || !inbox_id || !Array.isArray(payloads) || !payloads.length)
    return res.status(400).json({ ok: false, error: 'Missing token, inbox_id, or payloads.' });

  _job = {
    status: 'running', total: payloads.length, processed: 0,
    results: {}, phase: 'importing', pauseRemaining: 0,
    convLookup: {}, expectedInboxes: expectedInboxes || {}, webhookResults: {},
  };

  (async () => {
    const BATCH = 50, WIN = 60_000;
    for (let i = 0; i < payloads.length; i += BATCH) {
      const batch = payloads.slice(i, i + BATCH);
      const t0    = Date.now();
      for (const { payload, rowIndex } of batch) {
        try {
          const r = await frontPost(`/inboxes/${inbox_id}/imported_messages`, token, payload);
          if (r.ok) {
            const convId = r.data?.conv_id || r.data?.conversation_id
              || r.data?.conversation?.id
              || r.data?._links?.related?.conversation?.split('/').pop();
            const uid = r.data?.uid || r.data?.id;
            _job.results[rowIndex] = { ok: true, msg: 'Imported', uid, conv_id: convId };
            if (convId) {
              _job.convLookup[rowIndex] = { externalId: payload.external_id, conversationId: convId };
            } else {
              // Fall back to alt:uid lookup
              fetchConversationId(payload.external_id, token).then(cid => {
                if (cid) {
                  _job.convLookup[rowIndex] = { externalId: payload.external_id, conversationId: cid };
                  _job.results[rowIndex].conv_id = cid;
                }
              });
            }
          } else {
            _job.results[rowIndex] = { ok: false, msg: errMsg(r.data) || `HTTP ${r.status}` };
          }
        } catch (err) {
          _job.results[rowIndex] = { ok: false, msg: err.message };
        }
        _job.processed++;
      }
      if (i + BATCH < payloads.length) {
        const deadline = Date.now() + Math.max(0, WIN - (Date.now() - t0));
        _job.phase = 'pausing';
        while (Date.now() < deadline) { _job.pauseRemaining = Math.ceil((deadline - Date.now()) / 1000); await sleep(500); }
        _job.phase = 'importing'; _job.pauseRemaining = 0;
      }
    }
    _job.status = 'done'; _job.phase = '';
  })();

  res.json({ ok: true });
});

// Debug: test alt:uid lookup + show last import response
app.post('/api/debug-lookup', async (req, res) => {
  const { token, external_id } = req.body;
  if (!token || !external_id) return res.status(400).json({ error: 'Need token and external_id' });
  const r = await frontGet(`/messages/alt:uid:${encodeURIComponent(external_id)}`, token);
  res.json({ status: r.status, ok: r.ok, data: r.data });
});

// Poll import progress — includes convLookup and webhookResults
app.get('/api/import-status', (_req, res) => {
  res.json({
    status: _job.status, total: _job.total, processed: _job.processed,
    phase: _job.phase, pause_remaining: _job.pauseRemaining,
    results: _job.results, webhookResults: _job.webhookResults,
    convLookup: _job.convLookup,
  });
});

// ─── Webhook endpoint (configure in Front: http://your-host/webhook) ─────────
app.post('/webhook', (req, res) => {
  res.status(200).send('ok'); // respond immediately

  try {
    const body = req.body;

    // Conversation ID is at body.conversation.id
    const convId = body?.conversation?.id;
    if (!convId) return;

    // Inbox name is in target.data[0].name for "move" events
    const inboxName = body?.target?.data?.[0]?.name || null;
    if (!inboxName) return;

    // Find which row owns this conversation ID
    for (const [rowIdx, lookup] of Object.entries(_job.convLookup)) {
      if (lookup.conversationId === convId) {
        const expected = (_job.expectedInboxes[rowIdx] || '').trim().toLowerCase();
        const added    = inboxName.trim();
        const match    = expected ? added.toLowerCase() === expected : null;
        _job.webhookResults[rowIdx] = { addedInbox: added, match };
      }
    }
  } catch (_) {}
});

// Reset
app.post('/api/reset', (_req, res) => {
  _job = {
    status: 'idle', total: 0, processed: 0, results: {}, phase: '', pauseRemaining: 0,
    convLookup: {}, expectedInboxes: {}, webhookResults: {},
  };
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`\n  Front Message Importer`);
  console.log(`  ──────────────────────`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Webhook URL: http://localhost:${PORT}/webhook\n`);
});
