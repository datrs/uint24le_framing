# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Added

- `Uint24LELengthPrefixedFraming::split_off_errors`, which sends read errors to a separate stream.
- CI.

### Changed

### Fixed

- EOF now ends the stream, or is an `UnexpectedEof` error if it arrives part way through a frame.

### Removed



## [0.2.0] - 2026-01-15

### Added

### Changed

* In `poll_flush` ensure underlying IO is flushed.

### Removed



## [0.1.0] - 2025-12-29

### Added

* `Uint24LELengthPrefixedFraming`

### Changed

### Removed

<!-- next-url -->
[Unreleased]: https://github.com/datrs/uint24le_framing/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/datrs/uint24le_framing/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/datrs/uint24le_framing/compare/v0.1.0...v0.1.0
