---
name: Draft quality scoring via Hugging Face
description: How the Autopilot draft quality-scoring feature works and provider details.
---

- Scoring compares the Autopilot draft to the human reply from the spreadsheet's `outbound` column, using an LLM-as-judge (1–10 + explanation).
- Provider: Hugging Face router, OpenAI-compatible endpoint `https://router.huggingface.co/v1/chat/completions`, auth via `HUGGINGFACE_API_KEY` secret, model `meta-llama/Llama-3.3-70B-Instruct` (router serves the Turbo variant).
- **Why:** user chose an open-source model via their own HF key over Replit-managed AI.
- **How to apply:** any new AI features here should reuse this endpoint/key. Note: since drafts arrive via Front webhooks that point at the published URL, scoring only fully works in production — remind user to republish after changes.
- Async writers on the server (draft fetch, conv-id lookup, scoring) must capture `_job` and drop results if the job was reset/replaced — this raced before and corrupted new jobs.
