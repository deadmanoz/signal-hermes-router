# Hermes Profile Audit Checklist

Create an activation record from this checklist in the private deployment repo
before a route can move to `active`, and before any profile `skills/`, pinned
Hermes version, or route permission allowlist changes.

Date: YYYY-MM-DD

Profile: profile-name

Routes covered:

- signal:GROUP_ID_REF

Checks:

- [ ] For MCP-only routes, `signal-hermes-router preflight-permissions` shows no `local_tool_exposed` issues.
- [ ] Route `mcp_only` flag reviewed: when true, the profile's `full_callable` surface contains no local terminal/fs tools.
- [ ] Profile skills listed from the private profile directory.
- [ ] Pinned `hermes-agent` version recorded.
- [ ] `hermes-acp` toolset contents inspected and attached or summarized.
- [ ] Route permission allowlist reviewed against expected tools and arguments.
- [ ] Version 1 `full_callable` tool-surface contract recorded.
- [ ] `signal-hermes-router preflight-permissions` report attached or summarized.
- [ ] Denial canary run for an unlisted tool call.
- [ ] Allow canary run for each intended allowed tool shape.
- [ ] Route-specific context reviewed for private data minimization.
- [ ] Profile state DB, skills, and audit checklist included in encrypted backup regime.

Notes:

- Keep prior checklist records. Do not overwrite history.
- Store this artifact only in the private deployment repo or config directory.
