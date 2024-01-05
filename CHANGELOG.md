# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.9] - 2024-01-05

### Changed in 1.1.0

- Added tracking of the "Principle" (`ERRULE_CODE`) with replicated relationships.
- Added missing indexes on `sz_dm_relation` table for `entity_id` and `related_id`.
- Added indexes on `sz_dm_relation` for `match_key` and `principle`

## [1.0.0] - 2023-12-08

### Changes in version 1.0.0

- Initial revision
