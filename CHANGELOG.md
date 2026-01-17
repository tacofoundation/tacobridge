# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-01-17

### Fixed

* Export now strips `internal:gdal_vsi` column to prevent stale VSI paths in exported datasets
* `build_local_metadata()` uses per-level path tracking to avoid ID collisions between hierarchy levels

### Changed

* Removed `PARQUET_EXTENSION` import (removed from tacoreader)

### Dependencies

* Requires `tacoreader>=2.4.10`

## [0.2.0] - 2025-01-04

### Added

* Comprehensive test suite:
  * Unit tests for plan, execute, finalize, metadata, types, logging
  * Integration tests for api, concat, roundtrip
  * Test fixtures with flat, nested, and deep datasets

### Fixed

* Import error: `VSI_SUBFILE_PREFIX` now imported from `tacobridge._constants` instead of `tacoreader._constants`

## [0.1.0] - 2025-01-04

Initial release of tacobridge.

### Added

* High-level API:
  * `export()` - Export filtered/concatenated TacoDataset to FOLDER or ZIP format
  * `zip2folder()` - Convert ZIP to FOLDER format
  * `folder2zip()` - Convert FOLDER to ZIP format

* Low-level API (plan/execute/finalize pattern):
  * `plan_export()`, `plan_zip2folder()`, `plan_folder2zip()` - Compute operations without I/O
  * `execute()` - Execute single CopyTask (parallelizable by user)
  * `finalize()` - Write metadata or package ZIP

* Core features:
  * Format auto-detection from output path extension
  * Parallel execution support via `workers` parameter
  * Progress bars with tqdm
  * Remote source support (S3, HTTPS, GCS) for zip2folder
  * Fast local extraction path using zipfile

* Metadata handling:
  * Automatic reindexing of filtered datasets
  * Composite keying for concatenated datasets (`source_path`, `source_file`)
  * Provenance tracking (`taco:subset_of`, `taco:subset_date`)
  * Local `__meta__` generation for FOLDER format

* Exception hierarchy:
  * `TacoBridgeError` - Base exception
  * `TacoPlanError` - Planning failures (before I/O)
  * `TacoExecuteError` - Single task failures (retryable)
  * `TacoFinalizeError` - Finalization failures

### Dependencies

* Requires `tacoreader>=2.3.3`
* Requires `tacotoolbox>=0.26.0`