package com.senzing.datamart;

import com.senzing.datamart.DataMartTestExtension.RepositoryType;
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

/**
 * Diagnostic test to confirm that Senzing 4.3.0+ properly escapes special
 * characters (particularly colons) in MATCH_KEY strings for disclosed
 * relationships. This test loads truth-set-disclosed-rel.jsonl and dumps
 * the MATCH_KEY values for all disclosed relationships.
 *
 * <p>Run with: {@code mvn test -Dtest=SzMatchKeyEscapingDiagnosticTest}</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Execution(ExecutionMode.SAME_THREAD)
public class SzMatchKeyEscapingDiagnosticTest {

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
    private Set<SzRecordKey> recordKeys;

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
                .instanceName("MatchKeyDiagSetup")
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
        env = SzCoreEnvironment.newBuilder()
                .instanceName("MatchKeyDiagLoad")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzEngine engine = env.getEngine();
            recordKeys = new LinkedHashSet<>();

            List<JsonObject> records = readRecords();
            for (JsonObject jsonObj : records) {
                String dataSource = getString(jsonObj, "DATA_SOURCE");
                String recordId = getString(jsonObj, "RECORD_ID");
                SzRecordKey recordKey = SzRecordKey.of(dataSource, recordId);
                recordKeys.add(recordKey);
                engine.addRecord(recordKey, toJsonText(jsonObj), SZ_NO_FLAGS);
            }

            // Process redos
            while (engine.countRedoRecords() > 0) {
                String redo = engine.getRedoRecord();
                if (redo != null && redo.trim().length() > 0) {
                    engine.processRedoRecord(redo);
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
    void testDumpDisclosedRelationshipMatchKeys() throws Exception {
        SzCoreEnvironment env = SzCoreEnvironment.newBuilder()
                .instanceName("MatchKeyDiagQuery")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();
        try {
            SzEngine engine = env.getEngine();
            Set<SzRecordKey> found = new HashSet<>();
            int disclosedCount = 0;

            logInfo("=== DISCLOSED RELATIONSHIP MATCH KEYS ===");

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
                String entityName = getString(resolved, "ENTITY_NAME");

                // Track found records
                JsonArray records = getJsonArray(resolved, "RECORDS");
                if (records != null) {
                    for (JsonObject rec : records.getValuesAs(JsonObject.class)) {
                        String ds = getString(rec, "DATA_SOURCE");
                        String rid = getString(rec, "RECORD_ID");
                        found.add(SzRecordKey.of(ds, rid));
                    }
                }

                // Check related entities
                JsonArray related = getJsonArray(jsonObj, "RELATED_ENTITIES");
                if (related == null) continue;

                for (JsonObject rel : related.getValuesAs(JsonObject.class)) {
                    long relatedId = getLong(rel, "ENTITY_ID");
                    String matchKey = getString(rel, "MATCH_KEY");
                    String matchLevelCode = getString(rel, "MATCH_LEVEL_CODE");
                    int isDisclosed = getInteger(rel, "IS_DISCLOSED");
                    String erruleCode = getString(rel, "ERRULE_CODE");

                    if (isDisclosed == 1) {
                        disclosedCount++;
                        logInfo("Entity " + entityId + " (" + entityName
                                + ") -> Entity " + relatedId,
                                "  MATCH_KEY       : " + matchKey,
                                "  MATCH_LEVEL_CODE: " + matchLevelCode,
                                "  ERRULE_CODE     : " + erruleCode);
                    }
                }
            }

            logInfo("Total disclosed relationships found: " + disclosedCount);

            Assertions.assertTrue(disclosedCount > 0,
                    "Expected at least one disclosed relationship in "
                    + "truth-set-disclosed-rel.jsonl");

        } finally {
            env.destroy();
        }
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
