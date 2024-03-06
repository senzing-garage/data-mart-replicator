# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2024-03-05

### Changed in 1.1.0

- Added tracking of the "Principle" (`ERRULE_CODE`) with replicated relationships
- Added tracking of the Match Key (`MATCH_KEY`) and "Principle" (`ERRULE_CODE`) 
  with replicated records
- Modified index on `sz_dm_relation` table for `related_id` + `entity_id`
- Added indexes on `sz_dm_relation` for `match_key` and `principle`
- Added indexes on `sz_dm_record` for `match_key` and `principle`
- Added tracking of Principle and Match Key variants of summary statistics
- Added configuration for `maven-compiler-plugin`
- Updated dependency versions:
    - Updated AWS dependencies to version `2.24.12`
    - Updated `maven-javadoc-plugin` to version `3.6.3`
    - Updated `senzing-listener` to version `0.5.5`
    - Updqted `senzing-commons-java` to version `3.2.0`
    - Updated Jackson dependencies to version `2.16.1`
    - Updted SLF4J dependencies to version `2.0.12`

## [1.0.0] - 2023-12-08

### Changes in version 1.0.0

- Initial revision
