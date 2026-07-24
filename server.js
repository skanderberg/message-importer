const express = require('express');
const path    = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

// Log every incoming request so we can confirm Front is reaching us
app.use((req, _res, next) => {
  console.log(`[req] ${req.method} ${req.path} ct=${req.headers['content-type']}`);
  next();
});

// Parse webhook with raw text first so Front's content-type never blocks it
app.use('/webhook', express.text({ type: '*/*', limit: '10mb' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── Import queue state ───────────────────────────────────────────────────────
let _job = {
  status: 'idle', total: 0, processed: 0,
  results: {}, phase: '', pauseRemaining: 0,
  convLookup: {}, expectedInboxes: {}, webhookResults: {},
  drafts: {}, humanDrafts: {}, rowContext: {}, scores: {}, token: null,
};

// Buffer webhooks that arrive before convLookup is populated
let _pendingWebhooks = []; // [{ convId, inboxName }]
let _pendingComments = []; // [{ convId }] — comment events awaiting convLookup

function applyWebhook(convId, inboxName) {
  const rowIdx = _job.convLookup[convId];
  if (rowIdx === undefined) return false;
  const expected = (_job.expectedInboxes[rowIdx] || '').trim().toLowerCase();
  const added    = inboxName.trim();
  const match    = expected ? added.toLowerCase() === expected : null;
  _job.webhookResults[rowIdx] = { addedInbox: added, match };
  return true;
}

function flushPendingWebhooks() {
  _pendingWebhooks = _pendingWebhooks.filter(({ convId, inboxName }) => !applyWebhook(convId, inboxName));
}

// A comment on one of our conversations is the trigger to look for an Autopilot draft
function applyCommentWebhook(convId) {
  const rowIdx = _job.convLookup[convId];
  if (rowIdx === undefined) return false;
  fetchDrafts(convId, rowIdx, _job.token); // fire-and-forget
  return true;
}

function flushPendingComments() {
  _pendingComments = _pendingComments.filter(({ convId }) => !applyCommentWebhook(convId));
}

function stripHtml(html) {
  if (!html) return '';
  return String(html).replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

// List drafts on a conversation and store the most recent one for its row.
// Autopilot may create the draft moments after the comment fires, so retry a few times.
async function fetchDrafts(convId, rowIndex, token) {
  if (!token) return;
  const jobRef = _job; // guard: drop results if the job is reset/replaced mid-flight
  for (let attempt = 0; attempt < 4; attempt++) {
    if (attempt > 0) await sleep(3000);
    if (jobRef !== _job) return;
    try {
      const r = await frontGet(`/conversations/${convId}/drafts`, token);
      if (jobRef !== _job) return;
      console.log(`[drafts] attempt=${attempt} conv=${convId} status=${r.status} count=${(r.data?._results||[]).length}`);
      if (!r.ok) continue;
      const drafts = r.data?._results || [];
      if (!drafts.length) continue;
      const latest = drafts.reduce((a, b) => ((b.created_at || 0) > (a.created_at || 0) ? b : a));
      const author = latest.author
        ? [latest.author.first_name, latest.author.last_name].filter(Boolean).join(' ') || latest.author.username || null
        : null;
      _job.drafts[rowIndex] = {
        body:   latest.text || stripHtml(latest.body) || latest.blurb || '',
        author,
        count:  drafts.length,
        at:     latest.created_at || null,
      };
      scoreDraft(rowIndex); // fire-and-forget quality scoring vs. human draft
      return;
    } catch (e) {
      console.log(`[drafts] attempt=${attempt} conv=${convId} error:`, e.message);
    }
  }
}

// ─── Draft quality scoring (OpenAI) ──────────────────────────────────────────
const SCORING_MODEL = 'gpt-4o-mini';

async function scoreDraft(rowIndex) {
  const human = (_job.humanDrafts[rowIndex] || '').trim();
  const draft = (_job.drafts[rowIndex]?.body || '').trim();
  if (!human || !draft) return;                 // nothing to compare against
  if (_job.scores[rowIndex]?.status === 'pending' || _job.scores[rowIndex]?.status === 'done') return;
  if (!process.env.OPENAI_API_KEY) {
    _job.scores[rowIndex] = { status: 'error', error: 'OPENAI_API_KEY not configured' };
    return;
  }

  _job.scores[rowIndex] = { status: 'pending' };
  const ctx = _job.rowContext[rowIndex] || {};
  const jobRef = _job;

  const prompt = `You are evaluating an AI-generated customer support draft reply against the reply a human agent actually sent, for the same inbound message.

INBOUND MESSAGE:
Subject: ${ctx.subject || '(none)'}
Body: ${stripHtml(ctx.body) || '(none)'}

HUMAN AGENT'S REPLY (reference standard):
${human}

AI-GENERATED DRAFT (to evaluate):
${draft}

Grade the AI draft against the human reply in three categories, each scored 1-5 (integer):
- tone: professionalism, empathy, and voice compared to the human reply
- content: does it address the same issue, cover the key points, and stay factually consistent with the human reply; penalize missing commitments/steps or anything incorrect or risky
- formatting: structure, length, greeting/sign-off, readability compared to the human reply

Respond with ONLY a JSON object, no other text:
{"tone": <1-5>, "content": <1-5>, "formatting": <1-5>, "explanation": "<concise explanation (2-3 sentences) of the main strengths and gaps vs the human reply>"}`;

  try {
    const res = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: SCORING_MODEL,
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.2,
        max_tokens: 700,
        response_format: { type: 'json_object' },
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (jobRef !== _job) return; // job was reset while we waited
    if (!res.ok) {
      const msg = data?.error?.message || data?.error || `HTTP ${res.status}`;
      console.log(`[score] row=${rowIndex} HF error: ${JSON.stringify(msg).slice(0, 300)}`);
      _job.scores[rowIndex] = { status: 'error', error: String(typeof msg === 'string' ? msg : JSON.stringify(msg)).slice(0, 200) };
      return;
    }
    const content = data?.choices?.[0]?.message?.content || '';
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    let parsed = null;
    if (jsonMatch) { try { parsed = JSON.parse(jsonMatch[0]); } catch (_) {} }
    if (!parsed || typeof parsed.tone !== 'number' || typeof parsed.content !== 'number' || typeof parsed.formatting !== 'number') {
      console.log(`[score] row=${rowIndex} unparseable response: ${content.slice(0, 300)}`);
      _job.scores[rowIndex] = { status: 'error', error: 'Model returned an unparseable response' };
      return;
    }
    const clamp5 = v => Math.max(1, Math.min(5, Math.round(v)));
    const tone = clamp5(parsed.tone), cont = clamp5(parsed.content), fmt = clamp5(parsed.formatting);
    const overall = Math.round((tone + cont + fmt) / 3 * 10) / 10;
    _job.scores[rowIndex] = {
      status: 'done',
      tone, content: cont, formatting: fmt, score: overall,
      explanation: String(parsed.explanation || '').trim(),
      model: SCORING_MODEL,
    };
    console.log(`[score] row=${rowIndex} tone=${tone} content=${cont} formatting=${fmt} overall=${overall}`);
  } catch (e) {
    console.log(`[score] row=${rowIndex} error:`, e.message);
    if (jobRef === _job) _job.scores[rowIndex] = { status: 'error', error: e.message };
  }
}

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
  for (let attempt = 0; attempt < 12; attempt++) {
    await sleep(attempt === 0 ? 5000 : 8000);
    try {
      const r = await frontGet(`/messages/alt:uid:${encodeURIComponent(externalId)}`, token);
      const d = r.data;
      console.log(`[uid-lookup] attempt=${attempt} status=${r.status} externalId=${externalId} data=${JSON.stringify(d).slice(0, 400)}`);
      _job.debugLookup = { attempt, status: r.status, externalId, data: d };
      if (!r.ok) continue;
      const convId =
        d?.conversation_id ||
        d?.conversation?.id ||
        d?._links?.related?.conversation?.split('/').pop() ||
        d?._results?.[0]?.conversation_id ||
        d?._results?.[0]?.conversation?.id;
      if (convId) return convId;
    } catch (e) {
      console.log(`[uid-lookup] attempt=${attempt} error:`, e.message);
      _job.debugLookup = { attempt, externalId, error: e.message };
    }
  }
  return null;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get('/api/version', (_req, res) => res.json({ version: 'request-logger-v12', built: '2026-06-19' }));

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

  const { token, inbox_id, payloads, expectedInboxes, humanDrafts } = req.body;
  if (!token || !inbox_id || !Array.isArray(payloads) || !payloads.length)
    return res.status(400).json({ ok: false, error: 'Missing token, inbox_id, or payloads.' });

  const rowContext = {};
  for (const { payload, rowIndex } of payloads) {
    rowContext[rowIndex] = { subject: payload?.subject || '', body: payload?.body || '' };
  }

  _job = {
    status: 'running', total: payloads.length, processed: 0,
    results: {}, phase: 'importing', pauseRemaining: 0,
    convLookup: {}, expectedInboxes: expectedInboxes || {}, webhookResults: {},
    drafts: {}, humanDrafts: humanDrafts || {}, rowContext, scores: {}, token,
  };
  _pendingWebhooks = [];
  _pendingComments = [];

  (async () => {
    const BATCH = 50, WIN = 60_000;
    for (let i = 0; i < payloads.length; i += BATCH) {
      const batch = payloads.slice(i, i + BATCH);
      const t0    = Date.now();
      for (const { payload, rowIndex } of batch) {
        try {
          const r = await frontPost(`/inboxes/${inbox_id}/imported_messages`, token, payload);
          if (r.ok) {
            const uid = r.data?.uid || r.data?.id || r.data?.message_uid;
            console.log(`[import] rowIndex=${rowIndex} status=${r.status} uid=${uid} raw=${JSON.stringify(r.data).slice(0,300)}`);
            _job.results[rowIndex] = { ok: true, msg: 'Imported', uid };
            _job.debugImport = { rowIndex, status: r.status, uid, data: r.data };
            if (uid) {
              const jobRef = _job;
              fetchConversationId(uid, token).then(cid => {
                if (cid && jobRef === _job && _job.results[rowIndex]) {
                  _job.convLookup[cid] = rowIndex;
                  _job.results[rowIndex].conv_id = cid;
                  flushPendingWebhooks(); // apply any webhooks that arrived early
                  flushPendingComments(); // and any comment/draft triggers
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
    results: _job.results, webhookResults: _job.webhookResults, drafts: _job.drafts,
    scores: _job.scores,
    convLookup: _job.convLookup, debugLookup: _job.debugLookup || null,
    debugImport: _job.debugImport || null, debugWebhook: _job.debugWebhook || null,
  });
});

// ─── Webhook endpoint (configure in Front: http://your-host/webhook) ─────────
app.post('/webhook', (req, res) => {
  res.status(200).send('ok'); // respond immediately

  const rawBody = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
  _job.debugWebhook = { received: true, rawSnippet: rawBody.slice(0, 500) };

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const convId    = body?.conversation?.id;
    const eventType = body?.type || body?.target?._meta?.type || null;
    console.log(`[webhook] type=${eventType} convId=${convId}`);

    if (!convId) return;

    // A comment OR tag added to one of our conversations → go look for an Autopilot draft
    if (eventType === 'comment' || eventType === 'tag') {
      _job.debugWebhook = { eventType, convId, rawSnippet: rawBody.slice(0, 500) };
      if (!applyCommentWebhook(convId)) _pendingComments.push({ convId });
      return;
    }

    // Otherwise treat it as the inbox-add event and record which inbox it landed in
    const inboxName = body?.target?.data?.[0]?.name || null;
    _job.debugWebhook = { eventType, convId, inboxName, rawSnippet: rawBody.slice(0, 500) };
    if (!inboxName) return;

    if (!applyWebhook(convId, inboxName)) {
      _pendingWebhooks.push({ convId, inboxName });
    }
  } catch (e) {
    console.log('[webhook] error:', e.message);
    _job.debugWebhook = { error: e.message, rawSnippet: rawBody.slice(0, 500) };
  }
});

// Reset
app.post('/api/reset', (_req, res) => {
  _job = {
    status: 'idle', total: 0, processed: 0, results: {}, phase: '', pauseRemaining: 0,
    convLookup: {}, expectedInboxes: {}, webhookResults: {}, drafts: {},
    humanDrafts: {}, rowContext: {}, scores: {}, token: null,
  };
  _pendingWebhooks = [];
  _pendingComments = [];
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`\n  Front Message Importer`);
  console.log(`  ──────────────────────`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Webhook URL: http://localhost:${PORT}/webhook\n`);
});
