## INFO Message Parsing

### What is a Senzing INFO message and how is it parsed?

A Senzing INFO message is a JSON notification emitted by the Senzing engine when entity resolution state changes. The replicator consumes these messages to keep the data mart synchronized.

**INFO Message JSON Structure:**

```json
{
  "DATA_SOURCE": "CUSTOMERS",
  "RECORD_ID": "1001",
  "AFFECTED_ENTITIES": [
    {"ENTITY_ID": 42},
    {"ENTITY_ID": 99}
  ]
}
```

Key fields:
- `DATA_SOURCE` + `RECORD_ID`: Identifies the source record that was added, modified, or deleted
- `AFFECTED_ENTITIES`: Array of entity IDs whose resolution changed as a result. An entity may have gained or lost records, or relationships may have changed.

Optional fields:
- `INTERESTING_ENTITIES`: Entities flagged as interesting based on configured rules
- `NOTICES`: Alert notices triggered by the change

**Parsing Flow (AbstractListenerService.scheduleTasks)**

1. Raw JSON is parsed via `SzInfoMessage.fromRawJson()`
2. The `messagePartMap` (configured in `SzReplicatorService`) maps message parts to task actions:
   - `AFFECTED_ENTITY` → `REFRESH_ENTITY` action
3. For each affected entity ID, a task is scheduled:
   ```
   scheduler.createTaskBuilder(REFRESH_ENTITY)
     .resource(ENTITY_RESOURCE_KEY, entityId)
     .parameter(ENTITY_ID_KEY, entityId)
     .schedule()
   ```
4. `scheduler.commit()` finalizes the batch of scheduled tasks

### What triggers an INFO message?

Any mutation to the Senzing entity resolution store:
- Adding a new record
- Modifying an existing record
- Deleting a record
- Re-evaluation that causes records to merge into a different entity or split apart

A single record change can affect multiple entities (e.g., a record moving from Entity A to Entity B produces an INFO message listing both entities as affected).

### Important: message ordering is NOT guaranteed

INFO messages may arrive out of order. The replicator handles this safely because:
1. Each `REFRESH_ENTITY` task fetches the **current** entity state from the SDK (not the state at message time)
2. Entity hash comparison detects no-op changes (if a later message already updated the entity)
3. Duplicate messages are harmless — they just produce no-delta refreshes
