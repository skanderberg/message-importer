---
name: Multi-turn conversation simulation
description: How multi-turn Autopilot testing works — customer simulation loop, webhook fragility, grading
---

Spreadsheets with `body`, `outbound`, `body 2`, `outbound 2`, … columns are multi-turn. For those rows the server acts as a customer simulator: after each non-final Autopilot draft it generates the customer's next reply with the LLM (sticking to the scripted example, steering back if Autopilot goes off script) and imports it into the same Front conversation via the same `metadata.thread_ref`. After the final turn the whole transcript is graded (tone/content/formatting 1–5) against the scripted example instead of per-draft scoring.

**Sent-message mode:** for multi-turn rows Autopilot actually *sends* replies (no drafts) — the user's Front rule fires webhooks on message-sent. Detection reads the conversation's outbound (`is_inbound: false`) messages, not the drafts endpoint; single-turn rows still use drafts. Webhook events without an inbox payload are routed to this check.

**Why the watchdog exists:** Front's comment/tag webhook is the primary trigger to look for drafts, but a second webhook per conversation is not guaranteed and drafts can appear after the short retry window. Waiting rows are therefore re-checked every ~20s (10-min cap) or the turn loop deadlocks in `waiting`.

**Gating rule:** only advance a turn when phase is `waiting`, the last transcript message is from the customer, and the draft is strictly newer than the last consumed one — otherwise late revisions of an already-consumed draft advance the script prematurely.

**How to apply:** any change to draft detection, webhook handling, or turn state must preserve these guards; turn state must only be created for rows actually imported.
