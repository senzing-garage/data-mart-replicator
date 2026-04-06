# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Senzing MCP Server

When working on this project, always consult the Senzing MCP server for information about Senzing (SDK usage, data mapping, configuration, deployment, architecture, troubleshooting, best practices). Do not rely on training data for Senzing-specific details — use the MCP server tools as the authoritative source.

## FAQ MCP Server

Check the `data-mart-replicator-faq` MCP tools BEFORE making design assumptions about this project.
Key design rules, concurrency invariants, and operational knowledge are documented there.
When you encounter difficulty, search the FAQs FIRST.
After resolving any issue, ask the user if they want you to add it as a FAQ entry.

Critical topics documented in FAQs:
- INFO message parsing and task scheduling flow
- Follow-up task triggering and deduplication
- Entity delta computation and hash-based change detection
- Cross-source report updates with match key and ER code breakdowns
- Distributed locking and deadlock prevention
- Backpressure and throttling mechanisms
- Multi-process safety with lease-based coordination

## IMPORTANT: Code Modification Policy

**DO NOT modify files directly. Always make suggestions instead and wait for user approval.**

- The ONLY file you are permitted to edit directly is .claude/CLAUDE.md
- For ALL other files, provide suggestions and recommendations
- Wait for user review and approval before any changes are made
- Default to suggesting changes rather than implementing them
- This ensures the user maintains full control over all code modifications

## Project Overview

The Senzing Data Mart Replicator is a multi-threaded Java application that consumes Senzing INFO messages from message queues (Amazon SQS, RabbitMQ, or SQL-based), retrieves entity data via the Senzing SDK, and replicates statistics to a data mart database (PostgreSQL or SQLite).

**Main Entry Point:** `com.senzing.datamart.SzReplicator` (src/main/java/com/senzing/datamart/SzReplicator.java:119)

## Build and Test Commands

### Building
```bash
mvn install
```
This compiles the code, runs tests, and creates a shaded JAR with all dependencies.

### Running Tests
```bash
mvn test
```
Tests run in parallel by default (configured in pom.xml:165-169).

### Running a Single Test
```bash
mvn test -Dtest=ClassName
mvn test -Dtest=ClassName#methodName
```

### Generating Javadocs
```bash
mvn javadoc:javadoc
```
Output: `target/site/apidocs/`

### Running the Application
```bash
java -jar target/data-mart-replicator-2.0.0.jar --help
```

### Clean Build
```bash
mvn clean install
```

## Architecture

### Core Components

**Message Consumption Flow:**
1. `SzReplicator` (main thread) creates and starts a `MessageConsumer` (SQL/SQS/RabbitMQ)
2. `AbstractMessageConsumer` manages async worker pool and message batching
3. Messages are parsed and passed to `SzReplicatorService.process()`
4. Service schedules tasks via `SchedulingService` based on message content
5. Task handlers execute database operations in separate threads

**Key Packages:**
- `com.senzing.datamart` - Main application, service, options, URIs
- `com.senzing.datamart.handlers` - Task handlers (RefreshEntityHandler, report handlers)
- `com.senzing.datamart.model` - Domain models (entities, records, relationships, reports)
- `com.senzing.datamart.schema` - Database schema builders (PostgreSQL, SQLite)
- `com.senzing.listener.communication` - Message consumers and factory
- `com.senzing.listener.service` - Abstract listener service and task processing
- `com.senzing.listener.service.scheduling` - Task scheduling coordination
- `com.senzing.listener.service.locking` - Distributed locking for data consistency

### Message Consumers (Pluggable)

Three implementations via factory pattern:
- **SQLConsumer** - Reads from `sz_message_queue` table in data mart database
- **SQSConsumer** - Consumes from Amazon SQS queues
- **RabbitMQConsumer** - Consumes from RabbitMQ exchanges

Consumer selection determined by command-line options in `SzReplicatorOption`.

### Concurrency Model

From SzReplicator.java:729-732:
- Message consumption threads: `coreConcurrency * 2`
- Task scheduling threads: `coreConcurrency * 2`
- Database pool size: `coreConcurrency` (max: `poolSize * 3`)
- Default core concurrency: 8 threads

### Database Interaction

**Connection Management:**
- Uses `ConnectionPool` from senzing-commons with configurable sizing
- Transaction isolation: READ_COMMITTED for PostgreSQL
- Pattern: Get connection → Begin transaction → Lock rows → Update → Commit/Rollback → Close

**Schema Management:**
- Auto-created on first run via `SchemaBuilder` implementations
- Separate builders for PostgreSQL (with triggers) and SQLite
- Tables: entities, records, relationships, reports, locks, message queue

**SQL Patterns:**
- PostgreSQL: `INSERT ... ON CONFLICT DO UPDATE` for upserts
- Batch operations for efficient bulk updates
- Distributed locking via `sz_dm_locks` table (RefreshEntityHandler.java:1187)

### Senzing SDK Integration

**Initialization:**
- Uses `SzAutoCoreEnvironment` for automatic configuration management
- Configured with instance name, concurrency, and verbosity flags
- Environment wrapped in proxy to prevent premature destruction

**Entity Retrieval:**
- `engine.getEntity(entityId, flags)` returns JSON
- Flags control what data is included (records, relationships, matching info)
- Response parsed to `SzResolvedEntity` domain model
- Delta computation detects changes by comparing entity hashes

### Task Handlers

**Primary Handler:** `RefreshEntityHandler` (lines 81-204)
- Retrieves entity from Senzing SDK
- Computes deltas against existing data mart state
- Updates entity, record, and relationship tables
- Schedules report update tasks
- Uses database locking to prevent race conditions

**Report Handlers:** Update summary statistics
- `SourceSummaryReportHandler` - Per-datasource statistics
- `CrossSummaryReportHandler` - Cross-datasource match statistics
- `SizeBreakdownReportHandler` - Entity size distribution
- `RelationBreakdownReportHandler` - Relationship type distribution

Background thread (`ReportUpdater`) periodically schedules pending report updates.

## Key Design Patterns

- **Factory Pattern:** `MessageConsumerFactory` creates appropriate consumer
- **Strategy Pattern:** Pluggable consumers, schedulers, schema builders
- **Registry Pattern:** `ConnectionProvider.REGISTRY` for component binding
- **Template Method:** `AbstractListenerService` defines task scheduling skeleton
- **Thread Pool Pattern:** `AsyncWorkerPool` for concurrent processing
- **State Machine:** `MessageConsumer.State` and `ListenerService.State` enums
- **Command Pattern:** Task/Action for deferred execution

## Configuration

**Environment Variables:**
- All command-line options have environment variable equivalents
- Format: `SENZING_DATA_MART_*` or `SENZING_*`
- Example: `--concurrency` → `SENZING_DATA_MART_CONCURRENCY`

**Senzing Initialization:**
- Via `--core-settings {json-text}` (inline JSON)
- Via `--core-settings {file-path}` (path to JSON file)
- Environment: `SENZING_TOOLS_CORE_SETTINGS` or `SENZING_ENGINE_CONFIGURATION_JSON`

## Testing Notes

- Framework: JUnit 5 (Jupiter)
- Parallel execution enabled at class level
- Test files mirror src structure in src/test/java
- Use `mvn test -Dtest=ClassName` to run individual tests

### JUnit @Order Annotation Convention

When using JUnit's `@Order` annotation on test methods, always increment by 100 instead of 1. This allows inserting new tests between existing ones without renumbering all subsequent tests.

Example:

```java
@Test
@Order(100)
void testFirst() { }

@Test
@Order(200)
void testSecond() { }

@Test
@Order(300)
void testThird() { }
```

### Generating Coverage Report

```bash
mvn test jacoco:report -P jacoco
```
The JaCoCo profile must be activated with `-P jacoco`. The coverage report is generated at `target/site/jacoco/index.html`.

## Dependencies

**Core:**
- `sz-sdk` 4.3.0+ - Senzing Core SDK (minimum 4.3.0 for match key escaping)
- `sz-sdk-auto` 0.5.1 - Auto-configuration
- `senzing-commons` 4.0.0-beta.3.0 - Shared utilities

**Database:**
- `postgresql` 42.7.10 - PostgreSQL JDBC driver
- `sqlite-jdbc` 3.51.3.0 - SQLite JDBC driver

**Messaging:**
- `amqp-client` 5.29.0 - RabbitMQ client
- `sqs` 2.42.28 - AWS SQS SDK

**Serialization:**
- `jackson-*` 2.21.x - JSON processing

## Important Implementation Details

### Transaction Management
Always use this pattern (RefreshEntityHandler.java:89-204):
```java
Connection conn = null;
try {
    conn = provider.getConnection();
    // perform operations
    conn.commit();
} catch (Exception e) {
    if (conn != null) conn.rollback();
    throw e;
} finally {
    if (conn != null) conn.close();
}
```

### Backpressure and Throttling
`AbstractMessageConsumer` implements backpressure (lines 1015-1030):
- Pauses consumption when pending queue exceeds maximum
- Resumes when queue drains below threshold
- Prevents memory exhaustion under high message load

### Distributed Locking
Use `LockingService` to prevent concurrent modifications:
- Enroll entity IDs before modification
- Held for transaction duration
- Prevents race conditions in multi-instance deployments

### Error Handling
- Batch-level retry: if any message in batch fails, entire batch retried
- Transaction rollback on exception
- Detailed logging with context for debugging

## Current Development Status

- Main branch: `main`
- Upgraded from Senzing SDK 3.x to 4.x
- Minimum Senzing version: 4.3.0
- URI classes renamed from `*Url` to `*Uri` (e.g., `PostgreSqlUrlTest.java` → `PostgreSqlUriTest.java`)
