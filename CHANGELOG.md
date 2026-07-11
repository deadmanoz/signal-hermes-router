# Changelog

## [0.1.21](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.20...v0.1.21) (2026-07-11)


### Features

* **router:** add inbound burst policy for busy groups ([#45](https://github.com/deadmanoz/signal-hermes-router/issues/45))

## [0.1.20](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.19...v0.1.20) (2026-07-11)


### Features

* **sessions:** log and proactively detect Hermes subprocess exits ([#43](https://github.com/deadmanoz/signal-hermes-router/issues/43))

## [0.1.19](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.18...v0.1.19) (2026-07-11)


### Bug Fixes

* **router:** gracefully stop supervised Hermes children on SIGTERM ([#41](https://github.com/deadmanoz/signal-hermes-router/issues/41))

## [0.1.18](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.17...v0.1.18) (2026-07-11)


### Bug Fixes

* **acp:** add dedicated short timeout for ACP initialize ([#39](https://github.com/deadmanoz/signal-hermes-router/issues/39))

## [0.1.17](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.16...v0.1.17) (2026-07-11)


### Bug Fixes

* **signal:** skip malformed SSE frames without stream teardown ([#37](https://github.com/deadmanoz/signal-hermes-router/issues/37))

## [0.1.16](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.15...v0.1.16) (2026-07-11)


### Bug Fixes

* **dedupe:** reclaim orphaned processing claims at startup ([#35](https://github.com/deadmanoz/signal-hermes-router/issues/35))

## [0.1.15](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.14...v0.1.15) (2026-07-10)


### Bug Fixes

* **router:** guard control-socket response writes against client disconnect ([#33](https://github.com/deadmanoz/signal-hermes-router/issues/33))

## [0.1.14](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.13...v0.1.14) (2026-07-10)


### Bug Fixes

* **router:** handle Hermes model failure observability ([#29](https://github.com/deadmanoz/signal-hermes-router/issues/29))

## [0.1.13](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.12...v0.1.13) (2026-07-10)


### Bug Fixes

* **router:** ignore empty Signal turns ([#30](https://github.com/deadmanoz/signal-hermes-router/issues/30))

## [0.1.12](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.11...v0.1.12) (2026-07-04)


### Features

* **router:** add generic failure diagnostics ([#24](https://github.com/deadmanoz/signal-hermes-router/issues/24))

## [0.1.11](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.10...v0.1.11) (2026-06-30)


### Documentation

* **changelog:** make Release Please the single owner of CHANGELOG.md ([#22](https://github.com/deadmanoz/signal-hermes-router/issues/22))

## [0.1.10](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.9...v0.1.10) (2026-06-30)


### Features

* **router:** deliver notification image attachments

## [0.1.9](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.8...v0.1.9) (2026-06-24)


### Bug Fixes

* **deploy:** preserve deployment-local operator state during service-tree syncs ([#18](https://github.com/deadmanoz/signal-hermes-router/issues/18))

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

## [0.1.1](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.0...v0.1.1) (2026-05-20)

### Miscellaneous Chores

- Updated pinned GitHub Actions workflow dependencies.

## 0.1.0 (2026-05-19)

### Features

Initial release. Transport-only router from one Signal account to per-profile Hermes ACP subprocesses.
