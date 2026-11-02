# Changelog

Notable changes to this project. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

A release is a `v*` tag, and pushing one to the public repository builds the
multi-arch container image. This file starts at `v0.4.0`: the earlier releases
predate it, and a changelog reconstructed from git log would misreport them, so
their record is git log and the release pages.

## [Unreleased]

### Added

- `CHANGELOG.md`.
- `RetentionOverSizeTarget` in the example alert rules: fires when the census
  measures the archive over `RETENTION_KEEP_BYTES` across three consecutive
  prune intervals. One pass over target is the growth reservation having
  underestimated the archive and the next pass frees it, so the rule waits
  rather than paging on a self-healing overshoot.

## [0.4.1] - 2026-11-02

### Changed

- `retention_target_bytes` now reports the configured `RETENTION_KEEP_BYTES`
  from process startup, published by both the archiver and the backfill entry
  point. It used to be set by a prune pass, so a pass that stopped early - the
  foreign-keys brake, the headroom refusal - published `0` for an archive whose
  size target the operator had configured, and a dashboard could not tell "no
  size target" from "deletion stopped". `0` now means only what it says: the
  archive is bounded by `RETENTION_KEEP_DAYS` alone, or retention is disabled.

## [0.4.0] - 2026-11-01

### Added

- Size settings are written as human sizes: `RETENTION_KEEP_BYTES`,
  `RETENTION_GROWTH_HEADROOM_BYTES` and `S3_PART_SIZE` accept `400GB`,
  `400 GiB`, `16MiB` or a plain byte count, in either decimal or binary units.
  A bare number stays a byte count, so existing values keep working.
- `humanfriendly` as a runtime dependency, for parsing and rendering those
  sizes.

### Changed

- The multipart part floor is documented as the AWS S3 specification. Some
  S3-compatible endpoints accept smaller parts; that is no reason to diverge
  from AWS.

### Fixed

- A size value that is not one number plus one unit is refused at startup
  instead of being parsed into a threshold the operator did not write - the
  parser underneath ignores trailing text and resolves any unit beginning with
  a known prefix letter, and a too-small `RETENTION_KEEP_BYTES` deletes footage.

[Unreleased]: https://github.com/ivanboykov/frigate-motion-exporter/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/ivanboykov/frigate-motion-exporter/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/ivanboykov/frigate-motion-exporter/compare/v0.3.0...v0.4.0
