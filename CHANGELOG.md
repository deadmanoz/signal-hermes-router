# Changelog

## [0.1.8](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.7...v0.1.8) (2026-06-24)


### Features

* opt-in attachment tool_path in prompt manifests ([#16](https://github.com/deadmanoz/signal-hermes-router/issues/16))

## [0.1.7](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.6...v0.1.7) (2026-06-17)


### Features

* **preflight:** validate route permission tool surfaces

## [0.1.6](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.5...v0.1.6) (2026-06-16)


### Features

* **router:** route external notifications through control socket ([#11](https://github.com/deadmanoz/signal-hermes-router/issues/11))

## [0.1.5](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.4...v0.1.5) (2026-06-15)


### Features

* **router:** support scheduled synthetic route turns

## [0.1.4](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.3...v0.1.4) (2026-06-12)


### Features

* **router:** support human-only Signal direct routes

## [0.1.3](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.2...v0.1.3) (2026-06-12)


### Bug Fixes

* **router:** surface Signal shred diagnostics

## [0.1.2](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.1...v0.1.2) (2026-06-09)


### Features

* **router:** shred unrouteable Signal events ([#3](https://github.com/deadmanoz/signal-hermes-router/issues/3))

## [Unreleased]

### Bug Fixes

- Surface unknown and exception Signal shreds in logs without private payloads.
- Preserve scheduled synthetic job dedupe and control-response compatibility while adding route notifications.

### Features

- Shred unrouteable Signal events before parsing, dedupe, media storage, or ACP delivery.
- Route explicitly allowlisted Signal direct messages to Hermes profiles.
- Trigger scheduled synthetic route turns through the router-owned control socket.
- Send configured external route notifications through the router-owned control socket.
- Preflight route permission allowlists against recorded or live structured ACP tool surfaces.
- Expose stored attachment paths as `tool_path` in prompt manifests when a route opts in via `route_context.attachment_tool_paths`, so profile tools can operate on the exact stored file.

## [0.1.1](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.0...v0.1.1) (2026-05-20)

### Miscellaneous Chores

- Updated pinned GitHub Actions workflow dependencies.

## 0.1.0 (2026-05-19)

### Features

Initial release. Transport-only router from one Signal account to per-profile Hermes ACP subprocesses.
