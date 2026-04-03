## Component Overview

### What are the major components and how do they interact?

**SzReplicator** (main entry point)
- Parses command-line options, initializes the Senzing SDK environment, creates the connection pool, and wires everything together.
- Creates and starts the appropriate `MessageConsumer` based on configuration.
- Concurrency sizing: `coreConcurrency * 2` for consumption threads, `coreConcurrency * 2` for scheduling threads, `coreConcurrency` for base pool size (max `poolSize * 3`).

**MessageConsumer** (message ingestion layer)
- Three implementations: `SQSConsumer`, `RabbitMQConsumer`, `SQLConsumer`
- All extend `AbstractMessageConsumer` which provides batching, backpressure, and worker pool management
- Consumer selection is driven by which command-line options are provided

**SzReplicatorService** (orchestration)
- Extends `AbstractListenerService` — receives parsed messages and schedules tasks
- Maintains a handler map: each `TaskAction` maps to a `TaskHandler`
- Contains the `ReportUpdater` background thread that periodically flushes pending report updates

**SchedulingService** (task management)
- Manages task lifecycle: PENDING → execution → SUCCESSFUL/FAILED
- Provides task deduplication (identical tasks collapse into one with increased multiplicity)
- Handles follow-up task deferral (waits for a quiet period before executing)
- Uses `LockingService` to check resource availability before executing tasks

**TaskHandlers** (business logic)
- `RefreshEntityHandler`: The primary handler — retrieves entity from SDK, computes deltas, updates database
- `SourceSummaryReportHandler`: Per-datasource entity/record counts
- `CrossSummaryReportHandler`: Cross-datasource match/relation statistics
- `SizeBreakdownReportHandler`: Entity size distribution
- `RelationBreakdownReportHandler`: Entity relationship count distribution

**ConnectionPool** (database access)
- From `senzing-commons` library
- Manages PostgreSQL or SQLite connections
- Auto-creates schema on first use via `SchemaBuilder` implementations

### Component lifecycle

All major components follow a state machine pattern:
- `UNINITIALIZED → INITIALIZING → {READY/AVAILABLE/INITIALIZED} → DESTROYING → DESTROYED`
- State changes are synchronized and call `notifyAll()` to wake waiting threads
- `destroy()` waits for in-progress work to complete before transitioning to DESTROYED
- `waitUntilDestroyed()` blocks until the component reaches DESTROYED state
