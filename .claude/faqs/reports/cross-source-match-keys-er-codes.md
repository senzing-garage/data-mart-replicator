## Cross-Source Reports, Match Keys, and ER Codes

### How do cross-source reports track matches between data sources?

Cross-source summary reports answer: "How do records from Data Source A relate to records from Data Source B?" They track this at three levels of detail:

**Level 1: Overall cross-source statistics**
- Report key: `CSS:{STATISTIC}:{SOURCE1}:{SOURCE2}`
- Example: `CSS:MATCHED_COUNT:CUSTOMERS:VENDORS` = 500
- Meaning: 500 entities contain records from both CUSTOMERS and VENDORS that resolved together

**Level 2: Broken down by ER Code (principle)**
- Report key: `CSS:{STATISTIC}:{SOURCE1}:{SOURCE2}:{PRINCIPLE}`
- Example: `CSS:ENTITY_COUNT:CUSTOMERS:VENDORS:MATCH_NAME_DOB` = 300
- Meaning: 300 of those matches were due to the MATCH_NAME_DOB resolution rule

**Level 3: Broken down by ER Code AND Match Key**
- Report key: `CSS:{STATISTIC}:{SOURCE1}:{SOURCE2}:{PRINCIPLE}:{MATCH_KEY}`
- Example: `CSS:ENTITY_COUNT:CUSTOMERS:VENDORS:MATCH_NAME_DOB:+NAME+DOB` = 280
- Meaning: 280 matches via MATCH_NAME_DOB specifically on the NAME+DOB attribute combination

### What are Match Keys?

Match keys describe **which attributes** caused two records or entities to match. They use a `+ATTR` prefix format:
- `+NAME` — matched on name
- `+NAME+ADDRESS` — matched on name and address
- `+SSN` — matched on SSN
- `+NAME+DOB+EMAIL` — matched on name, date of birth, and email

Match keys appear on:
- **Records**: How this record matched into its entity (null for the first/seed record)
- **Relationships**: How two entities are related (bilateral match key between entities)

### What are ER Codes (Principles)?

ER Codes (also called "principles" or "errule_code" in the schema) identify **which Senzing resolution rule** fired to create the match. Examples:
- `MATCH_NAME_DOB` — name + date of birth rule
- `MATCH_NAME_ADDRESS` — name + address rule
- `MATCH_SSN` — SSN-only rule

The ER Code tells you the rule; the Match Key tells you the specific attributes that satisfied the rule.

### Directional match keys for relationships

Relationships can have different match keys depending on direction. For example, disclosed relationships using `REL_POINTER`:
- Forward: `+REL_POINTER(SPOUSE:)` (entity A points to B as spouse)
- Reverse: `+REL_POINTER(:SPOUSE)` (entity B is pointed to by A)

The `sz_dm_relation` table stores both `match_key` (forward) and `rev_match_key` (reverse). Both are indexed for report queries.

### Data source pair ordering

Cross-source reports always use ordered data source pairs. When comparing CUSTOMERS vs VENDORS, the pair is consistently `(CUSTOMERS, VENDORS)` alphabetically. Same-source pairs (CUSTOMERS vs CUSTOMERS) represent entities where multiple records from the same source resolved together.

### Why this matters for correctness

When computing cross-source deltas, every match key and ER code combination must be tracked separately. If an entity previously matched CUSTOMERS↔VENDORS on `+NAME` and now matches on `+NAME+ADDRESS`, the old `+NAME` count must be decremented and the new `+NAME+ADDRESS` count incremented. Missing either half corrupts the report.
