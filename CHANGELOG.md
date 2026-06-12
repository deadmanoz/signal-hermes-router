# Changelog

## [0.1.3](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.2...v0.1.3) (2026-06-12)


### Bug Fixes

* **router:** surface Signal shred diagnostics ([a27e873](https://github.com/deadmanoz/signal-hermes-router/commit/a27e8730fe763b0249f1e9364dd9ad33cfd56e02))

## [0.1.2](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.1...v0.1.2) (2026-06-09)


### Features

* **router:** shred unrouteable Signal events ([#3](https://github.com/deadmanoz/signal-hermes-router/issues/3))

## [Unreleased]

### Bug Fixes

- Surface unknown and exception Signal shreds in logs without private payloads.

### Features

- Shred unrouteable Signal events before parsing, dedupe, media storage, or ACP delivery.

## [0.1.1](https://github.com/deadmanoz/signal-hermes-router/compare/v0.1.0...v0.1.1) (2026-05-20)

### Miscellaneous Chores

- Updated pinned GitHub Actions workflow dependencies.

## 0.1.0 (2026-05-19)

### Features

Initial release. Transport-only router from one Signal account to per-profile Hermes ACP subprocesses.
