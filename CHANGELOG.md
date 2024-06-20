# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.3] - 2024-06-20

### Changed in 1.1.3

- Updated `senzing-commons-java` dependency from version `3.2.0` to `3.3.0`
- Updated `senzing-listener` dependency from version `0.5.7` to `0.5.9`
- Updated  `postgresql` dependency from version `42.7.2` to `42.7.3`
- Updated `jackson-annotations` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-databind` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-core` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-module-jaxb-annotations` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-datatype-joda` dependency from version `2.16.1` to `2.17.1`
- Updated Amazon `sqs` dependney from version `2.24.12` to `2.26.4`
- Updated `amqp-client` dependency from version `5.20.0` to `5.21.0`
- Updated `slf4j-api` dependency from version `2.0.12` to `2.0.13`
- Updated `slf4j-simple` dependency from version `2.0.12` to `2.0.13`
- Updated `maven-compiler-plugin` dependency from version `3.12.1` to `3.13.0`
- Updated `maven-source-plugin` dependency from version `3.3.0` to `3.3.1`
- Updated `maven-javadoc-plugin` dependency from version `3.6.3` to `3.7.0`
- Updated `maven-shade-plugin` dependency from version `3.5.2` to `3.6.0`

## [1.1.2] - 2024-03-21

### Changed in 1.1.2

- Updated `senzing-listener` dependency to version `0.5.7` to get fix for PostgreSQL v13.x
  when using `com.senzing.listener.communication.sql.PostgreSQLClient`.

## [1.1.1] - 2024-03-20

### Changed in 1.1.1

- Added support for PostgreSQL v13.x by changing changing `CREATE OR REPLACE TRIGGER`
  to `DROP TRIGGER` and `CREATE TRIGGER`

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
