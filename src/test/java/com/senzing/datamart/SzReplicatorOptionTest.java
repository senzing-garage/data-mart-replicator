package com.senzing.datamart;

import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.ParameterProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import javax.json.JsonObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.datamart.SzReplicatorOption.*;

/**
 * Comprehensive unit tests for {@link SzReplicatorOption}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzReplicatorOptionTest {

    @TempDir
    Path tempDir;

    private static final ParameterProcessor PROCESSOR = SzReplicatorOption.PARAMETER_PROCESSOR;

    // ========================================================================
    // Enum Constant Tests
    // ========================================================================

    /**
     * Test that all expected enum constants exist.
     */
    @Test
    public void testEnumConstants() {
        SzReplicatorOption[] values = SzReplicatorOption.values();

        assertEquals(15, values.length, "Should have 15 enum constants");

        // Verify specific constants exist
        assertNotNull(HELP);
        assertNotNull(VERSION);
        assertNotNull(IGNORE_ENVIRONMENT);
        assertNotNull(CORE_INSTANCE_NAME);
        assertNotNull(CORE_SETTINGS);
        assertNotNull(CORE_CONFIG_ID);
        assertNotNull(CORE_LOG_LEVEL);
        assertNotNull(CORE_CONCURRENCY);
        assertNotNull(REFRESH_CONFIG_SECONDS);
        assertNotNull(PROCESSING_RATE);
        assertNotNull(SQS_INFO_URI);
        assertNotNull(RABBITMQ_URI);
        assertNotNull(RABBITMQ_INFO_QUEUE);
        assertNotNull(DATABASE_INFO_QUEUE);
        assertNotNull(DATABASE_URI);
    }

    /**
     * Test valueOf() works for all enum constants.
     */
    @Test
    public void testValueOf() {
        assertEquals(HELP, SzReplicatorOption.valueOf("HELP"));
        assertEquals(VERSION, SzReplicatorOption.valueOf("VERSION"));
        assertEquals(CORE_SETTINGS, SzReplicatorOption.valueOf("CORE_SETTINGS"));
        assertEquals(DATABASE_URI, SzReplicatorOption.valueOf("DATABASE_URI"));
    }

    // ========================================================================
    // Instance Method Tests
    // ========================================================================

    /**
     * Test getCommandLineFlag() returns correct values.
     */
    @Test
    public void testGetCommandLineFlag() {
        assertEquals("--help", HELP.getCommandLineFlag());
        assertEquals("--version", VERSION.getCommandLineFlag());
        assertEquals("--core-settings", CORE_SETTINGS.getCommandLineFlag());
        assertEquals("--core-concurrency", CORE_CONCURRENCY.getCommandLineFlag());
        assertEquals("--database-uri", DATABASE_URI.getCommandLineFlag());
    }

    /**
     * Test isPrimary() returns correct values.
     */
    @Test
    public void testIsPrimary() {
        assertTrue(HELP.isPrimary(), "HELP should be primary");
        assertTrue(VERSION.isPrimary(), "VERSION should be primary");
        assertTrue(CORE_SETTINGS.isPrimary(), "CORE_SETTINGS should be primary");

        assertFalse(IGNORE_ENVIRONMENT.isPrimary());
        assertFalse(CORE_INSTANCE_NAME.isPrimary());
        assertFalse(CORE_CONCURRENCY.isPrimary());
    }

    /**
     * Test isSensitive() returns correct values.
     */
    @Test
    public void testIsSensitive() {
        assertTrue(RABBITMQ_URI.isSensitive(), "RABBITMQ_URI should be sensitive");
        assertTrue(DATABASE_URI.isSensitive(), "DATABASE_URI should be sensitive");
        assertTrue(CORE_SETTINGS.isSensitive(), "CORE_SETTINGS should be sensitive");

        assertFalse(HELP.isSensitive());
        assertFalse(CORE_CONCURRENCY.isSensitive());
        assertFalse(SQS_INFO_URI.isSensitive());
    }

    // ========================================================================
    // Static Method Tests - parseProcessingRate()
    // ========================================================================

    /**
     * Test parseProcessingRate with valid values.
     */
    @Test
    public void testParseProcessingRateValid() {
        assertEquals(ProcessingRate.LEISURELY,
            SzReplicatorOption.parseProcessingRate("leisurely"));
        assertEquals(ProcessingRate.LEISURELY,
            SzReplicatorOption.parseProcessingRate("LEISURELY"));

        assertEquals(ProcessingRate.STANDARD,
            SzReplicatorOption.parseProcessingRate("standard"));

        assertEquals(ProcessingRate.AGGRESSIVE,
            SzReplicatorOption.parseProcessingRate("aggressive"));
    }

    /**
     * Test parseProcessingRate with whitespace.
     */
    @Test
    public void testParseProcessingRateWithWhitespace() {
        assertEquals(ProcessingRate.STANDARD,
            SzReplicatorOption.parseProcessingRate("  standard  "));
    }

    /**
     * Test parseProcessingRate with null throws exception.
     */
    @Test
    public void testParseProcessingRateNull() {
        assertThrows(NullPointerException.class, () -> {
            SzReplicatorOption.parseProcessingRate(null);
        });
    }

    /**
     * Test parseProcessingRate with invalid value throws exception.
     */
    @Test
    public void testParseProcessingRateInvalid() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            SzReplicatorOption.parseProcessingRate("invalid");
        });

        assertTrue(exception.getMessage().contains("processing rate"));
    }

    // ========================================================================
    // Static Method Tests - parseDatabaseUri()
    // ========================================================================

    /**
     * Test parseDatabaseUri with PostgreSQL URI.
     */
    @Test
    public void testParseDatabaseUriPostgreSQL() {
        ConnectionUri uri = SzReplicatorOption.parseDatabaseUri(
            "postgresql://user:pass@localhost:5432/G2");

        assertNotNull(uri);
        assertTrue(uri instanceof PostgreSqlUri);
    }

    /**
     * Test parseDatabaseUri with SQLite URI.
     */
    @Test
    public void testParseDatabaseUriSQLite() {
        ConnectionUri uri = SzReplicatorOption.parseDatabaseUri(
            "sqlite3://na:na@/tmp/test.db");

        assertNotNull(uri);
        assertTrue(uri instanceof SQLiteUri);
    }

    /**
     * Test parseDatabaseUri with SzCoreSettingsUri.
     */
    @Test
    public void testParseDatabaseUriCoreSettings() {
        ConnectionUri uri = SzReplicatorOption.parseDatabaseUri(
            "sz://core-settings/SQL/CONNECTION");

        assertNotNull(uri);
        assertTrue(uri instanceof SzCoreSettingsUri,
            "Should parse to SzCoreSettingsUri");
    }

    /**
     * Test parseDatabaseUri with null throws exception.
     */
    @Test
    public void testParseDatabaseUriNull() {
        assertThrows(NullPointerException.class, () -> {
            SzReplicatorOption.parseDatabaseUri(null);
        });
    }

    /**
     * Test parseDatabaseUri with unsupported database type.
     */
    @Test
    public void testParseDatabaseUriUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> {
            SzReplicatorOption.parseDatabaseUri("mysql://localhost:3306/db");
        });
    }

    // ========================================================================
    // ParamProcessor Tests - Boolean Options
    // ========================================================================

    /**
     * Test IGNORE_ENVIRONMENT parameter processing.
     */
    @Test
    public void testIgnoreEnvironmentProcessing() throws Exception {
        // No parameters → true
        Object result = PROCESSOR.process(IGNORE_ENVIRONMENT, Collections.emptyList());
        assertEquals(Boolean.TRUE, result);

        // "true" → true
        result = PROCESSOR.process(IGNORE_ENVIRONMENT, List.of("true"));
        assertEquals(Boolean.TRUE, result);

        // "TRUE" → true
        result = PROCESSOR.process(IGNORE_ENVIRONMENT, List.of("TRUE"));
        assertEquals(Boolean.TRUE, result);

        // "false" → false
        result = PROCESSOR.process(IGNORE_ENVIRONMENT, List.of("false"));
        assertEquals(Boolean.FALSE, result);

        // "FALSE" → false
        result = PROCESSOR.process(IGNORE_ENVIRONMENT, List.of("FALSE"));
        assertEquals(Boolean.FALSE, result);
    }

    /**
     * Test IGNORE_ENVIRONMENT with invalid parameter.
     */
    @Test
    public void testIgnoreEnvironmentInvalid() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(IGNORE_ENVIRONMENT, List.of("yes"));
        });

        assertTrue(exception.getMessage().contains("true") ||
                   exception.getMessage().contains("false"));
    }

    /**
     * Test DATABASE_INFO_QUEUE parameter processing.
     */
    @Test
    public void testDatabaseInfoQueueProcessing() throws Exception {
        // No parameters → true
        Object result = PROCESSOR.process(DATABASE_INFO_QUEUE, Collections.emptyList());
        assertEquals(Boolean.TRUE, result);

        // "true" → true
        result = PROCESSOR.process(DATABASE_INFO_QUEUE, List.of("true"));
        assertEquals(Boolean.TRUE, result);

        // "false" → false
        result = PROCESSOR.process(DATABASE_INFO_QUEUE, List.of("false"));
        assertEquals(Boolean.FALSE, result);
    }

    // ========================================================================
    // ParamProcessor Tests - CORE_SETTINGS
    // ========================================================================

    /**
     * Test CORE_SETTINGS with JSON text parameter.
     */
    @Test
    public void testCoreSettingsProcessingJsonText() throws Exception {
        String jsonText = "{\"PIPELINE\":{\"CONFIGPATH\":\"/etc/opt/senzing\"}}";

        Object result = PROCESSOR.process(CORE_SETTINGS, List.of(jsonText));

        assertNotNull(result);
        assertTrue(result instanceof JsonObject);

        JsonObject jsonObj = (JsonObject) result;
        assertTrue(jsonObj.containsKey("PIPELINE"));
    }

    /**
     * Test CORE_SETTINGS with file path parameter.
     */
    @Test
    public void testCoreSettingsProcessingFilePath() throws Exception {
        // Create a temporary JSON file
        Path jsonFile = tempDir.resolve("test-config.json");
        String jsonContent = "{\"TEST\":\"value\",\"SQL\":{\"CONNECTION\":\"test\"}}";
        Files.writeString(jsonFile, jsonContent);

        Object result = PROCESSOR.process(CORE_SETTINGS, List.of(jsonFile.toString()));

        assertNotNull(result);
        assertTrue(result instanceof JsonObject);

        JsonObject jsonObj = (JsonObject) result;
        assertTrue(jsonObj.containsKey("TEST"));
        assertEquals("value", jsonObj.getString("TEST"));
    }

    /**
     * Test CORE_SETTINGS with non-existent file.
     */
    @Test
    public void testCoreSettingsProcessingFileNotFound() {
        String nonExistentFile = "/path/to/nonexistent/file.json";

        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of(nonExistentFile));
        });
    }

    /**
     * Test CORE_SETTINGS with invalid JSON.
     */
    @Test
    public void testCoreSettingsProcessingInvalidJson() {
        assertThrows(Exception.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of("{invalid json"));
        });
    }

    /**
     * Test CORE_SETTINGS with empty string.
     */
    @Test
    public void testCoreSettingsProcessingEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of(""));
        }, "Should throw IllegalArgumentException for empty string");
    }

    /**
     * Test CORE_SETTINGS with whitespace-only string.
     */
    @Test
    public void testCoreSettingsProcessingWhitespaceOnly() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of("   "));
        }, "Should throw for whitespace-only string");
    }

    /**
     * Test CORE_SETTINGS with deleted file throws exception.
     */
    @Test
    public void testCoreSettingsProcessingDeletedFile() throws Exception {
        // Create and then delete a temp file
        Path tempFile = tempDir.resolve("deleted-config.json");
        Files.writeString(tempFile, "{\"TEST\":\"value\"}");
        Files.delete(tempFile);

        // Processing should throw exception for non-existent file
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of(tempFile.toString()));
        }, "Should throw IllegalArgumentException for deleted/non-existent file");

        String message = exception.getMessage().toLowerCase();
        assertTrue(message.contains("file") ||
                   message.contains("not found") ||
                   message.contains("does not exist") ||
                   message.contains("error reading"),
            "Exception message should indicate file issue");
    }

    /**
     * Test CORE_SETTINGS with file containing invalid JSON.
     */
    @Test
    public void testCoreSettingsProcessingFileWithInvalidJson() throws Exception {
        // Create temp file with invalid JSON content
        Path tempFile = tempDir.resolve("invalid-json-config.json");
        Files.writeString(tempFile, "{ this is not valid JSON at all }");

        // Processing should throw exception for invalid JSON
        assertThrows(Exception.class, () -> {
            PROCESSOR.process(CORE_SETTINGS, List.of(tempFile.toString()));
        }, "Should throw exception when file contains invalid JSON");
    }

    // ========================================================================
    // ParamProcessor Tests - Integer/Long Parsing
    // ========================================================================

    /**
     * Test CORE_CONCURRENCY parameter processing.
     */
    @Test
    public void testCoreConcurrencyProcessing() throws Exception {
        Object result = PROCESSOR.process(CORE_CONCURRENCY, List.of("16"));
        assertEquals(Integer.valueOf(16), result);
    }

    /**
     * Test CORE_CONCURRENCY with zero throws exception.
     */
    @Test
    public void testCoreConcurrencyZero() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_CONCURRENCY, List.of("0"));
        }, "Should throw IllegalArgumentException for zero concurrency");
    }

    /**
     * Test CORE_CONCURRENCY with negative value.
     */
    @Test
    public void testCoreConcurrencyNegative() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_CONCURRENCY, List.of("-5"));
        });
    }

    /**
     * Test CORE_CONCURRENCY with non-numeric value.
     */
    @Test
    public void testCoreConcurrencyNonNumeric() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_CONCURRENCY, List.of("abc"));
        });
    }

    /**
     * Test CORE_CONFIG_ID parameter processing.
     */
    @Test
    public void testCoreConfigIdProcessing() throws Exception {
        Object result = PROCESSOR.process(CORE_CONFIG_ID, List.of("12345"));
        assertEquals(Long.valueOf(12345L), result);
    }

    /**
     * Test CORE_CONFIG_ID with non-numeric value.
     */
    @Test
    public void testCoreConfigIdNonNumeric() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_CONFIG_ID, List.of("not-a-number"));
        });
    }

    /**
     * Test REFRESH_CONFIG_SECONDS parameter processing.
     */
    @Test
    public void testRefreshConfigSecondsProcessing() throws Exception {
        Object result = PROCESSOR.process(REFRESH_CONFIG_SECONDS, List.of("600"));
        assertEquals(Long.valueOf(600L), result);
    }

    /**
     * Test REFRESH_CONFIG_SECONDS allows negative values.
     */
    @Test
    public void testRefreshConfigSecondsNegative() throws Exception {
        Object result = PROCESSOR.process(REFRESH_CONFIG_SECONDS, List.of("-1"));
        assertEquals(Long.valueOf(-1L), result);
    }

    // ========================================================================
    // ParamProcessor Tests - CORE_LOG_LEVEL
    // ========================================================================

    /**
     * Test CORE_LOG_LEVEL parameter processing.
     */
    @Test
    public void testCoreLogLevelProcessing() throws Exception {
        // "verbose" → 1
        assertEquals(Integer.valueOf(1), PROCESSOR.process(CORE_LOG_LEVEL, List.of("verbose")));
        assertEquals(Integer.valueOf(1), PROCESSOR.process(CORE_LOG_LEVEL, List.of("VERBOSE")));
        assertEquals(Integer.valueOf(1), PROCESSOR.process(CORE_LOG_LEVEL, List.of("1")));

        // "muted" → 0
        assertEquals(Integer.valueOf(0), PROCESSOR.process(CORE_LOG_LEVEL, List.of("muted")));
        assertEquals(Integer.valueOf(0), PROCESSOR.process(CORE_LOG_LEVEL, List.of("MUTED")));
        assertEquals(Integer.valueOf(0), PROCESSOR.process(CORE_LOG_LEVEL, List.of("0")));

        // Default parameters → default (muted/0)
        assertEquals(Integer.valueOf(0), PROCESSOR.process(CORE_LOG_LEVEL, CORE_LOG_LEVEL.getDefaultParameters()));
    }

    /**
     * Test CORE_LOG_LEVEL with invalid value.
     */
    @Test
    public void testCoreLogLevelInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(CORE_LOG_LEVEL, List.of("debug"));
        }, "Should throw IllegalArgumentException for invalid log level");
    }

    // ========================================================================
    // ParamProcessor Tests - String Parameters
    // ========================================================================

    /**
     * Test CORE_INSTANCE_NAME parameter processing.
     */
    @Test
    public void testCoreInstanceNameProcessing() throws Exception {
        Object result = PROCESSOR.process(CORE_INSTANCE_NAME, List.of("my-instance"));
        assertEquals("my-instance", result);
    }

    /**
     * Test CORE_INSTANCE_NAME trims whitespace.
     */
    @Test
    public void testCoreInstanceNameTrimsWhitespace() throws Exception {
        Object result = PROCESSOR.process(CORE_INSTANCE_NAME, List.of("  my-instance  "));
        assertEquals("my-instance", result);
    }

    /**
     * Test RABBITMQ_INFO_QUEUE parameter processing.
     */
    @Test
    public void testRabbitMqInfoQueueProcessing() throws Exception {
        Object result = PROCESSOR.process(RABBITMQ_INFO_QUEUE, List.of("info-queue"));
        assertEquals("info-queue", result);
    }

    // ========================================================================
    // ParamProcessor Tests - PROCESSING_RATE
    // ========================================================================

    /**
     * Test PROCESSING_RATE parameter processing.
     */
    @Test
    public void testProcessingRateProcessing() throws Exception {
        Object result = PROCESSOR.process(PROCESSING_RATE, List.of("leisurely"));
        assertEquals(ProcessingRate.LEISURELY, result);

        result = PROCESSOR.process(PROCESSING_RATE, List.of("standard"));
        assertEquals(ProcessingRate.STANDARD, result);

        // Default parameters → default (standard)
        result = PROCESSOR.process(PROCESSING_RATE, PROCESSING_RATE.getDefaultParameters());
        assertEquals(ProcessingRate.STANDARD, result);
    }

    // ========================================================================
    // ParamProcessor Tests - URI Parameters
    // ========================================================================

    /**
     * Test SQS_INFO_URI parameter processing.
     */
    @Test
    public void testSqsInfoUriProcessing() throws Exception {
        String sqsUrl = "https://sqs.us-east-1.amazonaws.com/123456/MyQueue";

        Object result = PROCESSOR.process(SQS_INFO_URI, List.of(sqsUrl));

        assertNotNull(result);
        assertTrue(result instanceof SQSUri);
    }

    /**
     * Test RABBITMQ_URI parameter processing.
     */
    @Test
    public void testRabbitMqUriProcessing() throws Exception {
        String rabbitUrl = "amqp://user:pass@localhost:5672/senzing";

        Object result = PROCESSOR.process(RABBITMQ_URI, List.of(rabbitUrl));

        assertNotNull(result);
        assertTrue(result instanceof RabbitMqUri);
    }

    /**
     * Test DATABASE_URI parameter processing with PostgreSQL.
     */
    @Test
    public void testDatabaseUriProcessingPostgreSQL() throws Exception {
        String dbUrl = "postgresql://user:pass@localhost:5432/G2";

        Object result = PROCESSOR.process(DATABASE_URI, List.of(dbUrl));

        assertNotNull(result);
        assertTrue(result instanceof PostgreSqlUri);
    }

    /**
     * Test DATABASE_URI parameter processing with SQLite.
     */
    @Test
    public void testDatabaseUriProcessingSQLite() throws Exception {
        String dbUrl = "sqlite3://na:na@/tmp/test.db";

        Object result = PROCESSOR.process(DATABASE_URI, List.of(dbUrl));

        assertNotNull(result);
        assertTrue(result instanceof SQLiteUri);
    }

    /**
     * Test DATABASE_URI with SzCoreSettingsUri format.
     */
    @Test
    public void testDatabaseUriProcessingCoreSettings() throws Exception {
        String coreSettingsUrl = "sz://core-settings/SQL/CONNECTION";

        Object result = PROCESSOR.process(DATABASE_URI, List.of(coreSettingsUrl));

        assertNotNull(result);
        assertTrue(result instanceof SzCoreSettingsUri,
            "Should parse to SzCoreSettingsUri");
    }

    /**
     * Test DATABASE_URI with default parameters.
     */
    @Test
    public void testDatabaseUriProcessingDefault() throws Exception {
        Object result = PROCESSOR.process(DATABASE_URI, DATABASE_URI.getDefaultParameters());

        assertNotNull(result, "Default DATABASE_URI should not be null");
        assertTrue(result instanceof SzCoreSettingsUri,
            "Default DATABASE_URI should be SzCoreSettingsUri instance");
    }

    /**
     * Test DATABASE_URI with unsupported database type.
     */
    @Test
    public void testDatabaseUriProcessingUnsupported() {
        assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(DATABASE_URI, List.of("mysql://localhost:3306/db"));
        }, "Should throw for unsupported database type like MySQL");
    }

    // ========================================================================
    // ParamProcessor Tests - HELP and VERSION
    // ========================================================================

    /**
     * Test HELP parameter processing.
     */
    @Test
    public void testHelpProcessing() throws Exception {
        Object result = PROCESSOR.process(HELP, Collections.emptyList());
        assertEquals(Boolean.TRUE, result);
    }

    /**
     * Test VERSION parameter processing.
     */
    @Test
    public void testVersionProcessing() throws Exception {
        Object result = PROCESSOR.process(VERSION, Collections.emptyList());
        assertEquals(Boolean.TRUE, result);
    }

    // ========================================================================
    // Conflict Tests
    // ========================================================================

    /**
     * Test HELP conflicts with other options.
     */
    @Test
    public void testHelpConflicts() {
        Set<CommandLineOption> conflicts = HELP.getConflicts();

        assertFalse(conflicts.contains(HELP),
            "HELP should not conflict with itself");
        assertTrue(conflicts.contains(VERSION),
            "HELP should conflict with VERSION");
        assertTrue(conflicts.contains(CORE_SETTINGS),
            "HELP should conflict with CORE_SETTINGS");
    }

    /**
     * Test messaging option conflicts.
     */
    @Test
    public void testMessagingConflicts() {
        Set<CommandLineOption> sqsConflicts = SQS_INFO_URI.getConflicts();

        assertTrue(sqsConflicts.contains(RABBITMQ_URI));
        assertTrue(sqsConflicts.contains(DATABASE_INFO_QUEUE));
    }

    // ========================================================================
    // Additional Tests
    // ========================================================================

    /**
     * Test toString() returns enum name.
     */
    @Test
    public void testToString() {
        assertEquals("HELP", HELP.toString());
        assertEquals("CORE_SETTINGS", CORE_SETTINGS.toString());
        assertEquals("DATABASE_URI", DATABASE_URI.toString());
    }

    /**
     * Test PARAMETER_PROCESSOR with invalid option type.
     */
    @Test
    public void testParameterProcessorInvalidOption() {
        CommandLineOption mockOption = new CommandLineOption() {
            public String getCommandLineFlag() { return "--mock"; }
            public int getMinimumParameterCount() { return 0; }
            public int getMaximumParameterCount() { return 0; }
            public List getDefaultParameters() { return Collections.emptyList(); }
            public boolean isPrimary() { return false; }
            public String getEnvironmentVariable() { return null; }
            public List getEnvironmentFallbacks() { return Collections.emptyList(); }
            public Set getConflicts() { return Collections.emptySet(); }
            public Set getDependencies() { return Collections.emptySet(); }
        };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            PROCESSOR.process(mockOption, Collections.emptyList());
        });

        assertTrue(exception.getMessage().contains("Unhandled"));
    }

    // ========================================================================
    // Additional Getter Tests
    // ========================================================================

    /**
     * Test getEnvironmentVariable() for all enum values.
     */
    @Test
    public void testGetEnvironmentVariableAllValues() {
        String envPrefix = "SENZING_TOOLS_";

        assertNull(HELP.getEnvironmentVariable());
        assertNull(VERSION.getEnvironmentVariable());
        assertNull(IGNORE_ENVIRONMENT.getEnvironmentVariable());
        assertEquals(envPrefix + "CORE_INSTANCE_NAME", CORE_INSTANCE_NAME.getEnvironmentVariable());
        assertEquals(envPrefix + "CORE_SETTINGS", CORE_SETTINGS.getEnvironmentVariable());
        assertEquals(envPrefix + "CORE_CONFIG_ID", CORE_CONFIG_ID.getEnvironmentVariable());
        assertEquals(envPrefix + "CORE_LOG_LEVEL", CORE_LOG_LEVEL.getEnvironmentVariable());
        assertEquals(envPrefix + "CORE_CONCURRENCY", CORE_CONCURRENCY.getEnvironmentVariable());
        assertEquals(envPrefix + "REFRESH_CONFIG_SECONDS", REFRESH_CONFIG_SECONDS.getEnvironmentVariable());
        assertEquals(envPrefix + "PROCESSING_RATE", PROCESSING_RATE.getEnvironmentVariable());
        assertEquals(envPrefix + "SQS_INFO_QUEUE_URL", SQS_INFO_URI.getEnvironmentVariable());
        assertEquals(envPrefix + "RABBITMQ_URI", RABBITMQ_URI.getEnvironmentVariable());
        assertEquals(envPrefix + "RABBITMQ_INFO_QUEUE", RABBITMQ_INFO_QUEUE.getEnvironmentVariable());
        assertEquals(envPrefix + "DATABASE_INFO_QUEUE", DATABASE_INFO_QUEUE.getEnvironmentVariable());
        assertEquals(envPrefix + "DATA_MART_DATABASE_URI", DATABASE_URI.getEnvironmentVariable());
    }

    /**
     * Test getEnvironmentFallbacks() for all enum values.
     */
    @Test
    public void testGetEnvironmentFallbacksAllValues() {
        // Helper to check if null or empty
        assertTrue(HELP.getEnvironmentFallbacks() == null ||
                   HELP.getEnvironmentFallbacks().isEmpty());
        assertTrue(VERSION.getEnvironmentFallbacks() == null ||
                   VERSION.getEnvironmentFallbacks().isEmpty());
        assertTrue(IGNORE_ENVIRONMENT.getEnvironmentFallbacks() == null ||
                   IGNORE_ENVIRONMENT.getEnvironmentFallbacks().isEmpty());

        // CORE_SETTINGS has fallback
        assertNotNull(CORE_SETTINGS.getEnvironmentFallbacks());
        assertEquals(1, CORE_SETTINGS.getEnvironmentFallbacks().size());
        assertEquals("SENZING_ENGINE_CONFIGURATION_JSON",
            CORE_SETTINGS.getEnvironmentFallbacks().get(0));

        // RABBITMQ_URI has fallback
        assertNotNull(RABBITMQ_URI.getEnvironmentFallbacks());
        assertEquals(1, RABBITMQ_URI.getEnvironmentFallbacks().size());
        assertEquals("SENZING_TOOLS_RABBITMQ_URI",
            RABBITMQ_URI.getEnvironmentFallbacks().get(0));

        // Others have null or empty fallbacks
        assertTrue(CORE_CONFIG_ID.getEnvironmentFallbacks() == null ||
                   CORE_CONFIG_ID.getEnvironmentFallbacks().isEmpty());
        assertTrue(DATABASE_URI.getEnvironmentFallbacks() == null ||
                   DATABASE_URI.getEnvironmentFallbacks().isEmpty());
    }

    /**
     * Test getConflicts() returns non-null for all enum values.
     */
    @Test
    public void testGetConflictsAllValues() {
        for (SzReplicatorOption option : SzReplicatorOption.values()) {
            assertNotNull(option.getConflicts(),
                option + " should return non-null conflicts set");
        }
    }

    /**
     * Test getDependencies() returns non-null for all enum values.
     */
    @Test
    public void testGetDependenciesAllValues() {
        for (SzReplicatorOption option : SzReplicatorOption.values()) {
            assertNotNull(option.getDependencies(),
                option + " should return non-null dependencies set");
        }
    }

    // ========================================================================
    // Additional Error Handling Tests
    // ========================================================================

}
