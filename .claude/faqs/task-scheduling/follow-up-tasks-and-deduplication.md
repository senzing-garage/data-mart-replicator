## Follow-Up Tasks and Deduplication

### How do tasks trigger follow-up tasks?

When `RefreshEntityHandler` processes an entity, it may discover that related entities also need refreshing. This happens when:

- A relationship was **added**: the related entity's data mart state may be stale
- A relationship was **removed**: the related entity needs to know it lost a relationship
- A relationship's data sources or match type **changed**: both entities need updating

Follow-up `REFRESH_ENTITY` tasks are scheduled via the `followUpScheduler` parameter passed to `handleTask()`:

```java
followUpScheduler.createTaskBuilder(REFRESH_ENTITY.toString())
    .resource(ENTITY_RESOURCE_KEY, followUpEntityId)
    .parameter(ENTITY_ID_KEY, followUpEntityId)
    .schedule(true)  // true = this is a follow-up task
```

**Critical**: Follow-up tasks are scheduled with `schedule(true)` which marks them as follow-ups. Follow-up tasks do NOT themselves generate further follow-ups (no recursive cascading). This prevents infinite task chains.

### How does task deduplication work?

The `SchedulingService` collapses identical tasks to avoid redundant work:

**Standard task deduplication:**
- Tasks are identified by their signature (action + parameters)
- If an identical task is already pending, the new one is not created — instead, the existing task's `multiplicity` counter is incremented
- When the task executes, `handleTask()` receives the multiplicity count

**Follow-up task deduplication with deferral:**
- Follow-up tasks have a deferral window (default 10 seconds)
- When a duplicate follow-up arrives during the deferral window, the timer resets
- The task won't execute until the deferral window expires with no new duplicates
- Maximum deferral is capped at `FOLLOW_UP_TIMEOUT` (60 seconds)
- This collapses bursts of related changes into a single task execution

**Why deferral matters:**
When a large batch of INFO messages arrives, many may affect the same entity. Without deferral, the entity would be refreshed N times. With deferral, the system waits for the burst to settle, then refreshes once with the latest state.

### Task states

- **PENDING**: Scheduled, never attempted
- **POSTPONED**: Attempted but couldn't acquire resource locks; will retry
- **FOLLOW_UP**: Scheduled as a result of another task's completion; subject to deferral

### Multiplicity

The `multiplicity` parameter tells the handler how many identical tasks were collapsed. Most handlers ignore it, but report handlers use it to understand the magnitude of pending changes. A multiplicity of 5 means "5 separate events triggered this same report update."
