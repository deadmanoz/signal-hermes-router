# Hermes Profile Audit Checklist

Create an activation record from this checklist in the private deployment repo
before a route can move to `active`, and before any profile `skills/`, pinned
Hermes version, or route permission allowlist changes.

Date: YYYY-MM-DD

Profile: profile-name

Routes covered:

- signal:GROUP_ID_REF

Checks:

- [ ] Profile skills listed from the private profile directory.
- [ ] Pinned `hermes-agent` version recorded.
- [ ] `hermes-acp` toolset contents inspected and attached or summarized.
- [ ] Route permission allowlist reviewed against expected tools and arguments.
- [ ] `signal-hermes-router preflight-permissions` report attached or summarized.
- [ ] Denial canary run for an unlisted tool call.
- [ ] Allow canary run for each intended allowed tool shape.
- [ ] Route-specific context reviewed for private data minimization.
- [ ] Profile state DB, skills, and audit checklist included in encrypted backup regime.

Notes:

- Keep prior checklist records. Do not overwrite history.
- Store this artifact only in the private deployment repo or config directory.
