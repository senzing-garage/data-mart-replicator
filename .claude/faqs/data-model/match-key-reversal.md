## Match Key Reversal for Disclosed Relationships

### Why do we need reverse match keys?

The `sz_dm_relation` table always stores the relationship with the lower entity ID as `entity_id` and the higher as `related_id`. The `match_key` column holds the match key from the perspective of the lower-ID entity, and `rev_match_key` holds it from the higher-ID entity's perspective.

For **discovered relationships** (POSSIBLE_MATCH, AMBIGUOUS_MATCH, POSSIBLE_RELATION), match keys are symmetrical — `+NAME+ADDRESS` is the same from both sides. The reverse match key equals the forward match key.

For **disclosed relationships** (DISCLOSED_RELATION), match keys are **directional** because `REL_POINTER` roles have a from/to direction. For example:
- Entity A → Entity B: `+REL_POINTER(HUSBAND:WIFE)`
- Entity B → Entity A: `+REL_POINTER(WIFE:HUSBAND)`

Both the forward and reverse match keys are stored so that reports can be queried efficiently from either entity's perspective.

### How the engine formats match keys (Senzing 4.3.0+)

Match keys are strings composed of `+` prefixed (confirmation) and `-` prefixed (denial) components:
- Discovered components: `+NAME`, `+ADDRESS`, `+PHONE`, `-DOB`
- Disclosed components: `+REL_POINTER(min_roles:max_roles)` or `+DOMAIN(min_roles:max_roles)`

**Special character escaping** (GDEV-4124, minimum Senzing 4.3.0): The characters `\ + - , ( ) :` are escaped with backslash in role values and feature names. The structural colon separating min/max roles is NOT escaped. This makes the match key unambiguously parseable.

Examples:
- Simple one-way: `+REL_POINTER(:SUBSIDIARY_OF)` — empty min, "SUBSIDIARY_OF" max
- Two-way: `+REL_POINTER(HUSBAND:WIFE)` — "HUSBAND" min, "WIFE" max
- Escaped colon in role: `+REL_POINTER(GLOBAL\:PARENT:SUBSIDIARY)` — "Global:Parent" min (colon escaped)
- Combined discovered+disclosed: `+SURNAME+ADDRESS+PHONE+REL_POINTER(DEFENDANT,HUSBAND:PLAINTIFF,WIFE)-DOB`
- Multiple comma-separated roles: `+REL_POINTER(DEFENDANT,HUSBAND:PLAINTIFF,WIFE)` — two min roles, two max roles (from marriage + divorce disclosures merging)

### How `SzRelationship.getReverseMatchKey()` works

The escape-aware parser in `SzRelationship.java` reverses a match key by:

1. **Tokenize** the match key on unescaped `+` and `-` boundaries into components
2. For each component, check if it's **role-bearing** — has an unescaped `(` containing exactly one unescaped `:` followed by an unescaped `)`
3. For role-bearing components: swap the text before and after the unescaped colon inside the parentheses
4. Non-role-bearing components (`+NAME`, `-DOB`) pass through unchanged
5. Reassemble all components

Helper methods:
- `isEscaped(char[], int)` — counts preceding backslashes (odd count = escaped)
- `findNextUnescaped(char[], char, int, int)` — finds next unescaped occurrence of a target character
- `reverseRoleBearingComponent(String)` — reverses a single parenthesized component

Malformed components (missing closing parenthesis, no unescaped colon, multiple unescaped colons) log a warning and pass through unchanged.

### How we verify correctness

The integration tests (`SzMatchKeyReversalIntegrationTest` and `SzComplexDisclosedRelTest`) verify the parser by comparing against the engine's own output:

1. Load test data with disclosed relationships into a real Senzing repository
2. For each disclosed relationship between Entity A and Entity B:
   - Get Entity A → find its `MATCH_KEY` to Entity B (the forward key)
   - Get Entity B → find its `MATCH_KEY` to Entity A (the reverse key from the engine)
3. Assert: `getReverseMatchKey(forward) == engine's reverse`
4. Assert: `getReverseMatchKey(reverse) == engine's forward`

This tests 56 reversals from `truth-set-disclosed-rel.jsonl` and 4 reversals from `complex-disclosed-rel-test.jsonl` — all with 0 mismatches.

### Test data for complex cases

`complex-disclosed-rel-test.jsonl` contains COUNTY_MARRIAGES and COUNTY_DIVORCE_FILINGS records that produce match keys combining both discovered and disclosed components:
- `+SURNAME+ADDRESS+PHONE+REL_POINTER(DEFENDANT,HUSBAND:PLAINTIFF,WIFE)-DOB`
- This confirms the parser handles the real-world case of merged disclosure roles from multiple data sources alongside discovered features.

### Important: Minimum Senzing version 4.3.0 required

The escape-aware parser depends on the engine escaping special characters in match keys (GDEV-4124). The `pom.xml` enforces `sz-sdk >= 4.3.0`. Earlier engine versions do NOT escape colons in role values, making the match key ambiguous and unparseable for roles like "Global:Parent" or "Owns 50:50".

### The `SZ_INCLUDE_MATCH_KEY_DETAILS` flag is NOT used

We previously included `SZ_INCLUDE_MATCH_KEY_DETAILS` in the entity retrieval flags. This flag returns a structured JSON breakdown of match key components (CONFIRMATIONS and DENIALS arrays). Since we now parse the `MATCH_KEY` string directly with the escape-aware parser, this flag was removed to improve performance. This avoids coupling to the engine's internal match key construction algorithm (e.g., component ordering).
