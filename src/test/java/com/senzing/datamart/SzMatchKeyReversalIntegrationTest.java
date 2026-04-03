package com.senzing.datamart;

import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.model.SzRelationship;
import com.senzing.sdk.*;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.util.SzUtilities;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.*;
import java.sql.*;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static com.senzing.sdk.SzFlag.*;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that loads truth-set-disclosed-rel.jsonl into a real
 * Senzing repository and verifies that the escape-aware
 * {@link SzRelationship#getReverseMatchKey(String)} produces correct
 * results by comparing against the engine's own match keys from both
 * entity perspectives.
 *
 * <p>For each disclosed relationship between Entity A and Entity B:
 * <ul>
 *   <li>Get Entity A → find match key to B (forward)</li>
 *   <li>Get Entity B → find match key to A (reverse)</li>
 *   <li>Assert: {@code getReverseMatchKey(forward) == reverse}</li>
 *   <li>Assert: {@code getReverseMatchKey(reverse) == forward}</li>
 * </ul>
 *
 * <p>Run with: {@code mvn test -Dtest=SzMatchKeyReversalIntegrationTest}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Execution(ExecutionMode.SAME_THREAD)
public class SzMatchKeyReversalIntegrationTest {

    private static final String TRUTH_SET_RESOURCE
            = "/com/senzing/datamart/truth-set-disclosed-rel.jsonl";

    private static final Set<SzFlag> ENTITY_FLAGS;
    static {
        EnumSet<SzFlag> enumSet = EnumSet.of(
                SZ_ENTITY_INCLUDE_ENTITY_NAME,
                SZ_ENTITY_INCLUDE_RECORD_DATA,
                SZ_ENTITY_INCLUDE_RECORD_MATCHING_INFO,
                SZ_ENTITY_INCLUDE_RELATED_MATCHING_INFO,
                SZ_ENTITY_INCLUDE_RELATED_RECORD_DATA);
        enumSet.addAll(SZ_ENTITY_INCLUDE_ALL_RELATIONS);
        ENTITY_FLAGS = Collections.unmodifiableSet(enumSet);
    }

    private EmbeddedPostgres embeddedPg;
    private String coreSettings;

    /**
     * Map of entity ID to its related entity match keys.
     * Key: entity ID, Value: map of related entity ID to MATCH_KEY.
     */
    private Map<Long, Map<Long, String>> entityMatchKeys;

    @BeforeAll
    void setup() throws Exception {
        embeddedPg = EmbeddedPostgres.builder().start();

        int pgPort = embeddedPg.getPort();
        String pgHost = "localhost";
        String pgDatabase = "postgres";
        String pgUser = "postgres";
        String pgPassword = "postgres";

        PostgreSqlUri connectionUri = new PostgreSqlUri(
                pgUser, pgPassword, pgHost, pgPort, pgDatabase);

        String jdbcUrl = "jdbc:postgresql://" + pgHost + ":" + pgPort
                + "/" + pgDatabase + "?user=" + pgUser + "&password=" + pgPassword;

        // Create Senzing schema
        File schemaFile = RepositoryType.POSTGRESQL.getSchemaFile();
        try (FileReader rdr = new FileReader(schemaFile, UTF_8);
             BufferedReader br = new BufferedReader(rdr);
             Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {
            for (String sql = br.readLine(); sql != null; sql = br.readLine()) {
                sql = sql.trim();
                if (sql.length() == 0) continue;
                stmt.execute(sql);
            }
        }

        coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

        // Register data sources
        SzCoreEnvironment env = SzCoreEnvironment.newBuilder()
                .instanceName("MatchKeyIntSetup")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzConfigManager configManager = env.getConfigManager();
            SzConfig config = configManager.createConfig();
            config.registerDataSource("CUSTOMERS");
            config.registerDataSource("REFERENCE");
            config.registerDataSource("WATCHLIST");
            configManager.setDefaultConfig(config.export());
        } finally {
            env.destroy();
        }

        // Load records
        Set<SzRecordKey> recordKeys = new LinkedHashSet<>();
        env = SzCoreEnvironment.newBuilder()
                .instanceName("MatchKeyIntLoad")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzEngine engine = env.getEngine();
            List<JsonObject> records = readRecords();
            for (JsonObject jsonObj : records) {
                String dataSource = getString(jsonObj, "DATA_SOURCE");
                String recordId = getString(jsonObj, "RECORD_ID");
                SzRecordKey recordKey = SzRecordKey.of(dataSource, recordId);
                recordKeys.add(recordKey);
                engine.addRecord(recordKey, toJsonText(jsonObj), SZ_NO_FLAGS);
            }
            while (engine.countRedoRecords() > 0) {
                String redo = engine.getRedoRecord();
                if (redo != null && redo.trim().length() > 0) {
                    engine.processRedoRecord(redo);
                }
            }
        } finally {
            env.destroy();
        }

        // Retrieve all entities and their disclosed relationship match keys
        entityMatchKeys = new LinkedHashMap<>();
        env = SzCoreEnvironment.newBuilder()
                .instanceName("MatchKeyIntQuery")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzEngine engine = env.getEngine();
            Set<SzRecordKey> found = new HashSet<>();

            for (SzRecordKey recordKey : recordKeys) {
                if (found.contains(recordKey)) continue;

                String entityJson;
                try {
                    entityJson = engine.getEntity(recordKey, ENTITY_FLAGS);
                } catch (SzNotFoundException e) {
                    continue;
                }

                JsonObject jsonObj = parseJsonObject(entityJson);
                JsonObject resolved = getJsonObject(jsonObj, "RESOLVED_ENTITY");
                long entityId = getLong(resolved, "ENTITY_ID");

                JsonArray records = getJsonArray(resolved, "RECORDS");
                if (records != null) {
                    for (JsonObject rec : records.getValuesAs(JsonObject.class)) {
                        found.add(SzRecordKey.of(
                                getString(rec, "DATA_SOURCE"),
                                getString(rec, "RECORD_ID")));
                    }
                }

                JsonArray related = getJsonArray(jsonObj, "RELATED_ENTITIES");
                if (related == null) continue;

                Map<Long, String> relMatchKeys = new LinkedHashMap<>();
                for (JsonObject rel : related.getValuesAs(JsonObject.class)) {
                    long relatedId = getLong(rel, "ENTITY_ID");
                    String matchKey = getString(rel, "MATCH_KEY");
                    int isDisclosed = getInteger(rel, "IS_DISCLOSED");
                    if (isDisclosed == 1) {
                        relMatchKeys.put(relatedId, matchKey);
                    }
                }
                if (!relMatchKeys.isEmpty()) {
                    entityMatchKeys.put(entityId, relMatchKeys);
                }
            }
        } finally {
            env.destroy();
        }
    }

    @AfterAll
    void teardown() throws Exception {
        if (embeddedPg != null) {
            embeddedPg.close();
        }
    }

    @Test
    @Order(100)
    void testDisclosedRelationshipsExist() {
        assertFalse(entityMatchKeys.isEmpty(),
                "Expected disclosed relationships in truth-set-disclosed-rel.jsonl");

        int totalRelationships = 0;
        for (Map<Long, String> relMap : entityMatchKeys.values()) {
            totalRelationships += relMap.size();
        }
        logInfo("Found " + totalRelationships
                + " disclosed relationships across "
                + entityMatchKeys.size() + " entities");
    }

    @Test
    @Order(200)
    void testReverseMatchKeyMatchesEnginePerspective() {
        int verified = 0;
        int mismatches = 0;

        for (Map.Entry<Long, Map<Long, String>> entityEntry
                : entityMatchKeys.entrySet()) {
            long entityA = entityEntry.getKey();

            for (Map.Entry<Long, String> relEntry
                    : entityEntry.getValue().entrySet()) {
                long entityB = relEntry.getKey();
                String forwardKey = relEntry.getValue();

                // Find the reverse key: Entity B's match key looking at Entity A
                Map<Long, String> bRelations = entityMatchKeys.get(entityB);
                if (bRelations == null || !bRelations.containsKey(entityA)) {
                    // Entity B may not have a disclosed relation back to A
                    // (one-way disclosures), skip
                    continue;
                }

                String reverseKey = bRelations.get(entityA);

                // Verify: our parser's reverse of the forward key should
                // match the engine's reverse key
                String computedReverse
                        = SzRelationship.getReverseMatchKey(forwardKey);

                if (!computedReverse.equals(reverseKey)) {
                    logWarning("MISMATCH for Entity " + entityA
                            + " -> " + entityB,
                            "  Forward key      : " + forwardKey,
                            "  Engine reverse   : " + reverseKey,
                            "  Computed reverse : " + computedReverse);
                    mismatches++;
                } else {
                    verified++;
                }

                // Also verify the other direction
                String computedForward
                        = SzRelationship.getReverseMatchKey(reverseKey);

                if (!computedForward.equals(forwardKey)) {
                    logWarning("MISMATCH (reverse direction) for Entity "
                            + entityB + " -> " + entityA,
                            "  Reverse key       : " + reverseKey,
                            "  Engine forward    : " + forwardKey,
                            "  Computed forward  : " + computedForward);
                    mismatches++;
                } else {
                    verified++;
                }
            }
        }

        logInfo("Verified " + verified
                + " match key reversals, " + mismatches + " mismatches");

        assertEquals(0, mismatches,
                "Match key reversal mismatches detected — "
                + "see stderr for details");
        assertTrue(verified > 0,
                "Expected at least one bidirectional disclosed "
                + "relationship to verify");
    }

    private List<JsonObject> readRecords() throws IOException {
        List<JsonObject> result = new ArrayList<>(200);
        try (InputStream is = getClass().getResourceAsStream(TRUTH_SET_RESOURCE);
             InputStreamReader isr = new InputStreamReader(is, UTF_8);
             BufferedReader br = new BufferedReader(isr)) {
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                line = line.trim();
                if (line.length() == 0 || line.startsWith("#")) continue;
                result.add(parseJsonObject(line));
            }
        }
        return result;
    }
}
