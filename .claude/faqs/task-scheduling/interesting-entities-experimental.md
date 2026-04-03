## Interesting Entities (Experimental)

### What are "Interesting Entities" in Senzing INFO messages?

Senzing INFO messages can optionally contain an `INTERESTING_ENTITIES` section alongside `AFFECTED_ENTITIES`. This is an experimental Senzing feature that flags entities matching user-configured interest criteria (e.g., watchlist hits, high-risk patterns).

### Current status in the data mart replicator

The generalized Senzing Listener layer has **stubbed-out** support for interesting entities:
- `AbstractListenerService.scheduleTasks()` parses the `INTERESTING_ENTITIES` section
- `handleInteresting()` is called for each interesting entity
- Tasks can be scheduled with flags and degrees from the interesting entity data

However, the **data mart replicator itself has NO handler implementations** for interesting entities. There are no task actions, no handlers, and no database tables for interesting entity data.

### What this means for code changes

- Do NOT implement interesting entity handlers without explicit direction — the feature is experimental in Senzing itself
- If you see `INTERESTING_ENTITIES` handling in the listener code, it's infrastructure scaffolding, not active functionality
- The stubbed code in the listener should be left as-is — it doesn't cause problems and will be needed if/when the feature matures
- Do NOT confuse `AFFECTED_ENTITIES` (actively processed) with `INTERESTING_ENTITIES` (ignored by the data mart)
