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
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests complex disclosed relationships with discovered components using
 * COUNTY_MARRIAGES and COUNTY_DIVORCE_FILINGS data sources. Verifies that
 * match key reversal works correctly for match keys that combine discovered
 * features (shared address, phone) with disclosed features (REL_POINTER
 * roles like HUSBAND/WIFE, DEFENDANT/PLAINTIFF).
 *
 * <p>Run with: {@code mvn test -Dtest=SzComplexDisclosedRelTest}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Execution(ExecutionMode.SAME_THREAD)
public class SzComplexDisclosedRelTest {

    private static final String TRUTH_SET_RESOURCE
            = "/com/senzing/datamart/complex-disclosed-rel-test.jsonl";

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

    /** Entity ID → (related entity ID → match key) for disclosed relationships */
    private Map<Long, Map<Long, String>> entityMatchKeys;

    @BeforeAll
    void setup() throws Exception {
        embeddedPg = EmbeddedPostgres.builder().start();

        int pgPort = embeddedPg.getPort();
        PostgreSqlUri connectionUri = new PostgreSqlUri(
                "postgres", "postgres", "localhost", pgPort, "postgres");
        String jdbcUrl = "jdbc:postgresql://localhost:" + pgPort
                + "/postgres?user=postgres&password=postgres";

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
                .instanceName("ComplexRelSetup")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzConfigManager configManager = env.getConfigManager();
            SzConfig config = configManager.createConfig();
            config.registerDataSource("COUNTY_MARRIAGES");
            config.registerDataSource("COUNTY_DIVORCE_FILINGS");
            configManager.setDefaultConfig(config.export());
        } finally {
            env.destroy();
        }

        // Load records
        Set<SzRecordKey> recordKeys = new LinkedHashSet<>();
        env = SzCoreEnvironment.newBuilder()
                .instanceName("ComplexRelLoad")
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

        // Retrieve entities and collect match keys
        entityMatchKeys = new LinkedHashMap<>();
        env = SzCoreEnvironment.newBuilder()
                .instanceName("ComplexRelQuery")
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

                    // Print ALL relationships for diagnostic purposes
                    System.out.println("Entity " + entityId + " -> " + relatedId
                            + " [disclosed=" + isDisclosed + "] MATCH_KEY: "
                            + matchKey);

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
                "Expected disclosed relationships in complex-disclosed-rel-test.jsonl");
    }

    @Test
    @Order(200)
    void testReverseMatchKeyConsistency() {
        int verified = 0;
        int mismatches = 0;

        for (Map.Entry<Long, Map<Long, String>> entityEntry
                : entityMatchKeys.entrySet()) {
            long entityA = entityEntry.getKey();

            for (Map.Entry<Long, String> relEntry
                    : entityEntry.getValue().entrySet()) {
                long entityB = relEntry.getKey();
                String forwardKey = relEntry.getValue();

                Map<Long, String> bRelations = entityMatchKeys.get(entityB);
                if (bRelations == null || !bRelations.containsKey(entityA)) {
                    continue; // one-way disclosure
                }

                String reverseKey = bRelations.get(entityA);
                String computedReverse
                        = SzRelationship.getReverseMatchKey(forwardKey);

                if (!computedReverse.equals(reverseKey)) {
                    System.err.println("MISMATCH: Entity " + entityA
                            + " -> " + entityB);
                    System.err.println("  Forward      : " + forwardKey);
                    System.err.println("  Engine reverse: " + reverseKey);
                    System.err.println("  Our reverse   : " + computedReverse);
                    mismatches++;
                } else {
                    verified++;
                }
            }
        }

        System.out.println("Complex disclosed rel test: verified="
                + verified + " mismatches=" + mismatches);
        assertEquals(0, mismatches, "Match key reversal mismatches detected");
        assertTrue(verified > 0,
                "Expected bidirectional disclosed relationships to verify");
    }

    private List<JsonObject> readRecords() throws IOException {
        List<JsonObject> result = new ArrayList<>(20);
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
