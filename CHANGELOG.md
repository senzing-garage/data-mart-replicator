# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
[markdownlint](https://dlaa.me/markdownlint/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0-beta.1.3] - 2025-12-19

### Changed in 2.0.0-beta.1.3

- Replaced parsing of `MATCH_LEVEL` (Senzing 3.x) with `MATCH_LEVEL_CODE` (Senzing 4.x)
- Introduced `SzMatchLevelCode` enum class to support parsing `MATCH_LEVEL_CODE`
- Updated report services to allow the inclusion or omission of trailing slash on many endpoints

## [2.0.0-beta.1.2] - 2025-12-10

### Changed in 2.0.0-beta.1.2

- Changed `SqsUri` to `SQSUri`
- Changed `SqliteUri` to `SQLiteUri`
- Changes to `SQLiteUri` to support multiple in-memory databases.

## [2.0.0-beta.1.1] - 2025-12-05

### Changed in 2.0.0-beta.1.1

- Modified `SQLiteUri` to handle Windows paths with drive letters and back-slashes
  as path separators in the URI even these are NOT standard URI's.  This was done 
  because such URI's are supported by the underlying Senzing engine in the
  initialization settings and therefore need to be handled when encountered.

## [2.0.0-beta.1.0] - 2025-12-03

### Changed in 2.0.0-beta.1.0

- Changed source code level to Java 17
- Changed dependency on `g2-sdk-java` to `sz-sdk-java`
- Converted for new Java SDK's from 4.x
- Modified to produce both a standard JAR and an executable server JAR
- Incorporated reporting code that was previously in `senzing-poc-server`
- Known Issues:
  - Missing unit tests on all data mart code (only some unit tests on listener code).  This issue is
    inherited from version 1.x which was manually tested, but will be rectified before release of 2.0.0.

## [1.2.4] - 2025-10-23

### Changed in 1.2.4

- Updated dependencies:
  - Updated `senzing-commons-java` dependencies from version `3.3.6` to `3.3.7`
  - Updated `jackson-xxxx` dependencies from version `2.19.2` to `2.20.0`
  - Updated `junit-jupiter` from version `5.13.4` to `5.14.0`
  - Updated `amqp-client` from version `5.26.0` to `5.27.0`
  - Updated Amazon `sqs` from version `2.32.19` to `2.35.11`

## [1.2.3] - 2025-08-12

### Changed in 1.2.3

- Fixed compile-time warnings.
- Updated dependencies:
  - Updated `senzing-commons-java` dependencies from version `3.3.4` to `3.3.6`
  - Updated `jackson-xxxx` dependencies from version `2.19.0` to `2.19.2`
  - Updated `junit-jupiter` from version `5.13.1` to `5.13.4`
  - Updated `sqlite-jdbc` from version `3.42.0.1` to `3.50.3.0`
  - Updated `amqp-client` from version `5.25.0` to `5.26.0`
  - Updated `jaxb-impl` from version `2.3.1` to `2.3.9`
  - Updated `javassist` from version `3.27.0-GA` to `3.30.2-GA`
  - Updated Amazon `sqs` from version `2.31.59` to `2.32.19`

## [1.2.2] - 2025-06-09

### Changed in 1.2.2

- Updated dependencies:
  - Updated `senzing-commons-java` dependencies from version `3.3.3` to `3.3.4`
  - Updated `postgresql` dependency from version `42.7.5` to `42.7.6`
  - Updated `jackson-xxxx` dependencies from version `2.18.3` to `2.19.0`
  - Updated `junit-jupiter` from version `5.12.2` to `5.13.1`
  - Updated Amazon `sqs` from version `2.31.22` to `2.31.59`

## [1.2.1] - 2025-04-16

### Changed in 1.2.1

- Updated dependencies:
  - Updated `senzing-commons-java` dependencies from version `3.3.2` to `3.3.3`
  - Updated `postgresql` dependency from version `42.7.4` to `42.7.5`
  - Updated `jackson-xxxx` dependencies from version `2.18.2` to `2.18.3`
  - Updated `junit-jupiter` from version `5.11.3` to `5.12.2`
  - Updated Amazon `sqs` from version `2.29.26` to `2.31.22`
  - Updated `amqp-client` dependencies from version `5.23.0` to `5.25.0`
  - Updated `slf4j-xxxxx` dependencies from version `2.0.16` to `2.0.17`
  - Updated `jaxb-core` dependencies from version `3.0.0` to `3.0.2`
  - Updated `maven-surefire-plugin` from version `3.5.2` to `3.5.3`
  - Updated `maven-compiler-plugin` from version `3.13.0` to `3.14.0`
  - Updated `maven-javadoc-plugin` from version `3.11.1` to `3.11.2`

## [1.2.0] - 2024-12-12

### Changed in 1.2.0

- Merged in `senzing-listener` code.

## [1.1.6] - 2024-12-06

### Changed in 1.1.6

- Updated `senzing-commons` dependency from version `3.3.0` to `3.3.2`
- Updated `senzing-listener` dependency from version `1.0.0` to `1.0.2`
- Updated `jackson-xxxxx` dependencies from version `2.18.0` to `2.18.2`
- Updated `amqp-client` dependency from version `5.22.0` to `5.23.0`
- Updated Amazon `sqs` dependency from version `2.28.17` to `2.29.26`
- Updated `junit-jupiter` dependency from version `5.11.2` to `5.11.3`
- Updated `maven-javadoc-plugin` dependency from version `3.10.1` to `3.11.1`

## [1.1.5] - 2024-10-08

### Changed in 1.1.5

- Updated `senzing-commons` dependency from version `3.3.0` to `3.3.1`
- Updated `senzing-listener` dependency from version `1.0.0` to `1.0.1`
- Updated `jackson-xxxxx` dependencies from version `2.17.2` to `2.18.0`
- Updated `amqp-client` dependency from version `5.21.0` to `5.22.0`
- Updated Amazon `sqs` dependency from version `2.26.27` to `2.28.17`
- Updated `slf4j-xxxxx` dependencies from version `2.0.13` to `2.0.16`
- Updated `junit-jupiter` dependency from version `5.10.3` to `5.11.2`
- Updated `postgresql` dependency from version `42.7.3` to `42.7.4`
- Updated `maven-javadoc-plugin` dependency from version `3.8.0` to `3.10.1`

## [1.1.4] - 2024-08-02

### Changed in 1.1.4

- Updated `senzing-listener` dependency from version `0.5.9` to `1.0.0`
- Updated `jackson-annotations` dependency from version `2.17.1` to `2.17.2`
- Updated `jackson-databind` dependency from version `2.17.1` to `2.17.2`
- Updated `jackson-core` dependency from version `2.17.1` to `2.17.2`
- Updated `jackson-module-jaxb-annotations` dependency from version `2.17.1` to `2.17.2`
- Updated `jackson-datatype-joda` dependency from version `2.17.1` to `2.17.2`
- Updated `junit-jupiter` dependency from version `5.10.2` to `5.10.3`
- Updated Amazon `sqs` dependency from version `2.26.4` to `2.26.27`
- Updated `maven-javadoc-plugin` dependency from version `3.7.0` to `3.8.0`

## [1.1.3] - 2024-06-20

### Changed in 1.1.3

- Updated `senzing-commons-java` dependency from version `3.2.0` to `3.3.0`
- Updated `senzing-listener` dependency from version `0.5.7` to `0.5.9`
- Updated `postgresql` dependency from version `42.7.2` to `42.7.3`
- Updated `jackson-annotations` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-databind` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-core` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-module-jaxb-annotations` dependency from version `2.16.1` to `2.17.1`
- Updated `jackson-datatype-joda` dependency from version `2.16.1` to `2.17.1`
- Updated Amazon `sqs` dependency from version `2.24.12` to `2.26.4`
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
  - Updated `senzing-commons-java` to version `3.2.0`
  - Updated Jackson dependencies to version `2.16.1`
  - Updated SLF4J dependencies to version `2.0.12`

## [1.0.0] - 2023-12-08

### Changes in version 1.0.0

- Initial revision
