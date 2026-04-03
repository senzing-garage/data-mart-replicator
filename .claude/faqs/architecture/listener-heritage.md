## Listener Heritage and Generalization

### Why does this project have generalized abstractions like AbstractMessageConsumer and SchedulingService?

The message consumption, task scheduling, and service lifecycle layers in this project are inherited from what was originally the "Senzing Listener" — a separate, generalized project for consuming Senzing INFO messages for any purpose, not just data mart replication.

This means:
- `AbstractMessageConsumer`, `AsyncWorkerPool`, message batching — all generalized for any INFO message consumer
- `AbstractListenerService`, `SchedulingService`, task lifecycle — generalized scheduling framework
- `LockingService`, `ResourceKey` — generalized resource locking abstraction
- `MessageConsumerFactory` — pluggable consumer selection

These abstractions are more general than strictly needed if the system were purpose-built exclusively for the data mart replicator. Some aspects (like the message part map, task action registry, and pluggable scheduling) could potentially be simplified for a data-mart-only use case.

### What this means when making changes

- Don't assume that simplifying an abstraction is safe — it may exist because of the generalized listener design
- The generalized nature means the scheduling and consumer layers are decoupled from the data mart business logic
- Data mart-specific logic lives in `SzReplicatorService`, `SzReplicationProvider`, and the task handlers
- The listener layers (under `com.senzing.listener`) should be treated as infrastructure — changes there affect the fundamental processing pipeline
- If considering simplification, be aware that the generalized design provides separation of concerns that makes the system easier to test and reason about, even if it's more abstraction than strictly necessary

### The key data-mart-specific extensions

- `SzReplicatorService` extends `AbstractListenerService` to configure the handler map and report updater
- `SzReplicationProvider` defines the data-mart-specific task actions (REFRESH_ENTITY, report updates)
- `RefreshEntityHandler` contains the core data mart business logic
- Report handlers implement data-mart-specific incremental statistics
