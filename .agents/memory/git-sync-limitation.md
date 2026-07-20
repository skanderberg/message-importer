---
name: Git sync limitation
description: Why agents cannot pull/fetch/push from GitHub in this project
---

Replit's git wrapper (gitsafe) blocks all network/object-writing git operations (fetch, pull, push) in **every** agent context — main agent AND isolated task agents both fail with "Destructive git operations are not allowed."

**Why:** Confirmed empirically in this project (main agent and a dedicated background task both hit the same block).

**How to apply:** Never propose a project task to do a git pull — it will fail the same way. The user must pull via the workspace Git pane UI, then the agent can restart the workflow and the user republishes. There is also no Replit-native auto-deploy-on-GitHub-push; docs confirm publishing is manual.
