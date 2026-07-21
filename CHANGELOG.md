# Changelog

## [0.1.36](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.35...v0.1.36) (2026-07-21)


### Features

* **router:** concurrent per-route dispatch with bounded in-flight buffer ([ed62bc1](https://github.com/deadmanoz/signal-hermes-router/commit/ed62bc153d4eb4d15a03b3e8163af56fe545bcd7))

## [0.1.35](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.34...v0.1.35) (2026-07-20)


### Features

* **tests:** test-suite support helpers + test_router.py family split ([#79](https://github.com/deadmanoz/signal-hermes-router/issues/79)) ([d7c08d5](https://github.com/deadmanoz/signal-hermes-router/commit/d7c08d56f9327f21ecd11e35cb6b0043d6a949ac))

## [0.1.34](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.33...v0.1.34) (2026-07-19)


### Documentation

* post-v0.1.33 cleanup pass for public consumption ([#76](https://github.com/deadmanoz/signal-hermes-router/issues/76)) ([56150be](https://github.com/deadmanoz/signal-hermes-router/commit/56150be3452e1e3d7f22b2ac71b59259ec815e86))

## [0.1.33](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.32...v0.1.33) (2026-07-19)


### Features

* **router:** add atomic live configuration reload over the private control socket ([#74](https://github.com/deadmanoz/signal-hermes-router/issues/74)) ([98443a9](https://github.com/deadmanoz/signal-hermes-router/commit/98443a95615e0006fc6bf49e73c699129703cdb0))

## [0.1.32](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.31...v0.1.32) (2026-07-17)


### Features

* **permissions,preflight,router:** prevent MCP-only Signal routes from invoking local terminal tools ([#72](https://github.com/deadmanoz/signal-hermes-router/issues/72)) ([15709a4](https://github.com/deadmanoz/signal-hermes-router/commit/15709a4a4669e2b560e05ca9779ac7eaddc241a8))

## [0.1.31](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.30...v0.1.31) (2026-07-17)


### Bug Fixes

* **release:** close publication trust gaps ([#70](https://github.com/deadmanoz/signal-hermes-router/issues/70)) ([95704e9](https://github.com/deadmanoz/signal-hermes-router/commit/95704e91323b7263e21b8b38c8f138aba61dcf6f))

## [0.1.30](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.29...v0.1.30) (2026-07-15)


### Bug Fixes

* **preflight:** adapt to Hermes-native _tool_surface/list response ([#68](https://github.com/deadmanoz/signal-hermes-router/issues/68)) ([ec63eef](https://github.com/deadmanoz/signal-hermes-router/commit/ec63eef99ee74f1928bf53825572e1c110d2eac0))

## [0.1.29](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.28...v0.1.29) (2026-07-15)


### Bug Fixes

* **preflight:** version callable tool-surface contracts ([#64](https://github.com/deadmanoz/signal-hermes-router/issues/64)) ([f1fdee5](https://github.com/deadmanoz/signal-hermes-router/commit/f1fdee58f11fae7239e7b67d62a639b1abb5d388))

## [0.1.28](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.27...v0.1.28) (2026-07-15)


### Bug Fixes

* **release:** prevent stale automated release reviews ([1979a24](https://github.com/deadmanoz/signal-hermes-router/commit/1979a2423a8ab1aada6d26fdbf3199f0d7ff64ab))

## [0.1.27](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.26...v0.1.27) (2026-07-15)


### Documentation

* **deployment:** harden release deployment checks ([#62](https://github.com/deadmanoz/signal-hermes-router/issues/62))

## [0.1.26](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.25...v0.1.26) (2026-07-15)


### Bug Fixes

* **router:** surface unmarked ACP empty replies

## [0.1.25](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.24...v0.1.25) (2026-07-12)


### Bug Fixes

* **release:** resolve release PR number without fromJSON in step env ([#57](https://github.com/deadmanoz/signal-hermes-router/issues/57))

## [0.1.24](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.23...v0.1.24) (2026-07-11)


### Features

* **outbound:** honor profile-emitted no-reply sentinel on transport out ([#52](https://github.com/deadmanoz/signal-hermes-router/issues/52))

## [0.1.23](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.22...v0.1.23) (2026-07-11)


### Features

* **router:** add retention sweeps for dedupe DB and media storage ([#50](https://github.com/deadmanoz/signal-hermes-router/issues/50))

## [0.1.22](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.21...v0.1.22) (2026-07-11)


### Features

* **sessions:** rotate persistent-route sessions by age and turn count ([#48](https://github.com/deadmanoz/signal-hermes-router/issues/48))

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
