package com.senzing.datamart;

import com.senzing.listener.communication.rabbitmq.RabbitMQConsumer;
import com.senzing.listener.communication.rabbitmq.TestableRabbitMQConsumer;
import com.senzing.listener.communication.sqs.SQSConsumer;
import com.senzing.listener.communication.sqs.TestableSQSConsumer;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.util.JsonUtilities;
import com.senzing.util.Quantified.Statistic;
import com.senzing.util.SzInstallLocations;
import com.senzing.util.SzUtilities;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import uk.org.webcompere.systemstubs.stream.SystemOut;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SzReplicator}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class SzReplicatorTest {

    /**
     * The {@link SzInstallLocations} for accessing Senzing installation files.
     */
    private static final SzInstallLocations INSTALL_LOCATIONS = SzInstallLocations.findLocations();

    /**
     * The embedded PostgreSQL database for Senzing repository.
     */
    private EmbeddedPostgres embeddedPostgres;

    /**
     * The data mart SQLite database file.
     */
    private File dataMartDbFile;

    /**
     * The Senzing core settings JSON string.
     */
    private String coreSettings;

    /**
     * The PostgreSQL JDBC URL for the Senzing repository.
     */
    private String postgresJdbcUrl;

    /**
     * Set up the Senzing repository with schema and default configuration.
     */
    @BeforeAll
    public void setUp() throws Exception {
        // Start embedded PostgreSQL for Senzing repository
        embeddedPostgres = EmbeddedPostgres.start();
        postgresJdbcUrl = embeddedPostgres.getJdbcUrl("postgres", "postgres");

        // Create temporary SQLite file for data mart
        dataMartDbFile = File.createTempFile("datamart_test_", ".db");
        dataMartDbFile.deleteOnExit();

        // Create Senzing schema in PostgreSQL
        createSenzingSchema(postgresJdbcUrl);

        // Build PostgreSqlUri from embedded postgres connection details
        int port = embeddedPostgres.getPort();
        PostgreSqlUri connectionUri = new PostgreSqlUri("postgres", "postgres", "localhost", port, "postgres");
        coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

        // Initialize default configuration with data sources
        initializeDefaultConfig(coreSettings);
    }

    /**
     * Clean up resources.
     */
    @AfterAll
    public void tearDown() throws Exception {
        // Close embedded PostgreSQL
        if (embeddedPostgres != null) {
            embeddedPostgres.close();
        }

        // Delete temporary data mart file
        if (dataMartDbFile != null && dataMartDbFile.exists()) {
            dataMartDbFile.delete();
        }
    }

    /**
     * Creates the Senzing schema in the PostgreSQL database.
     *
     * @param jdbcUrl The JDBC URL for the PostgreSQL database.
     * @throws SQLException If a database error occurs.
     * @throws IOException If an I/O error occurs reading the schema file.
     */
    private void createSenzingSchema(String jdbcUrl) throws SQLException, IOException {
        // Get the PostgreSQL schema file
        File resourceDir = INSTALL_LOCATIONS.getResourceDirectory();
        File schemaDir = new File(resourceDir, "schema");
        File schemaFile = new File(schemaDir, "szcore-schema-postgresql-create.sql");

        if (!schemaFile.exists()) {
            throw new IOException("Schema file not found: " + schemaFile.getAbsolutePath());
        }

        // Execute schema SQL
        try (FileReader rdr = new FileReader(schemaFile, UTF_8);
             BufferedReader br = new BufferedReader(rdr);
             Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement())
        {
            for (String sql = br.readLine(); sql != null; sql = br.readLine()) {
                sql = sql.trim();
                if (sql.length() == 0) {
                    continue;
                }
                stmt.execute(sql);
            }
        }
    }

    /**
     * Initializes the default Senzing configuration with required data sources.
     *
     * @param settings The core settings JSON string.
     * @throws SzException If a Senzing error occurs.
     */
    private void initializeDefaultConfig(String settings) throws SzException {
        SzCoreEnvironment env = null;
        try {
            env = SzCoreEnvironment.newBuilder()
                .instanceName("SzReplicatorTestSetup")
                .settings(settings)
                .verboseLogging(false)
                .build();

            SzConfigManager configManager = env.getConfigManager();
            SzConfig config = configManager.createConfig();

            // Add required data sources
            config.registerDataSource("CUSTOMERS");
            config.registerDataSource("REFERENCE");
            config.registerDataSource("WATCHLIST");

            // Set as default configuration
            configManager.setDefaultConfig(config.export());

        } finally {
            if (env != null) {
                env.destroy();
            }
        }
    }

    // ========================================================================
    // Tests
    // ========================================================================

    /**
     * Test printJarVersion() outputs JAR version information.
     */
    @Test
    public void testPrintJarVersion() {
        // Capture output using StringWriter
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        // Call printJarVersion
        SzReplicator.printJarVersion(pw);
        pw.flush();

        // Get the output
        String output = sw.toString();

        // Verify output is not null or empty
        assertNotNull(output, "Output should not be null");
        assertFalse(output.trim().isEmpty(), "Output should not be empty");

        // Verify output contains the Maven version from BuildInfo
        assertTrue(output.contains(BuildInfo.MAVEN_VERSION),
            "Output should contain MAVEN_VERSION: " + BuildInfo.MAVEN_VERSION);

        // Verify output contains typical JAR version header elements
        // (might contain "Version", "Senzing", "Replicator", etc.)
        assertTrue(output.toLowerCase().contains("version") ||
                   output.toLowerCase().contains("replicator"),
            "Output should contain version-related text");
    }

    /**
     * Test printSenzingVersions() outputs version information.
     */
    @Test
    public void testPrintSenzingVersions() throws Exception {
        SzEnvironment env = null;
        try {
            // Initialize environment with core settings
            env = SzCoreEnvironment.newBuilder()
                .settings(this.coreSettings)
                .build();

            // Get version information from Senzing directly
            String versionJson = env.getProduct().getVersion();
            JsonObject versionObj = JsonUtilities.parseJsonObject(versionJson);

            // Extract expected values
            String expectedNativeApiVersion = JsonUtilities.getString(versionObj, "VERSION");
            String expectedBuildVersion = JsonUtilities.getString(versionObj, "BUILD_VERSION");
            String expectedBuildNumber = JsonUtilities.getString(versionObj, "BUILD_NUMBER");

            JsonObject compatVersion = JsonUtilities.getJsonObject(versionObj, "COMPATIBILITY_VERSION");
            String expectedConfigCompatVersion = JsonUtilities.getString(compatVersion, "CONFIG_VERSION");

            // Capture output using StringWriter
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            // Call printSenzingVersions
            SzReplicator.printSenzingVersions(pw);
            pw.flush();

            // Get the output
            String output = sw.toString();

            // Verify output contains expected version information
            assertNotNull(output, "Output should not be null");
            assertFalse(output.trim().isEmpty(), "Output should not be empty");

            // Verify key version fields are present
            assertTrue(output.contains("Senzing Replicator Version"),
                "Output should contain Replicator version label");
            assertTrue(output.contains("Senzing Native API Version"),
                "Output should contain Native API version label");
            assertTrue(output.contains("Senzing Native Build Version"),
                "Output should contain Build version label");
            assertTrue(output.contains("Config Compatibility Version"),
                "Output should contain Config compatibility version label");

            // Verify actual version values from Senzing appear in output
            assertTrue(output.contains(expectedNativeApiVersion),
                "Output should contain actual Native API version: " + expectedNativeApiVersion);
            assertTrue(output.contains(expectedBuildVersion),
                "Output should contain actual Build version: " + expectedBuildVersion);
            assertTrue(output.contains(expectedBuildNumber),
                "Output should contain actual Build number: " + expectedBuildNumber);
            assertTrue(output.contains(expectedConfigCompatVersion),
                "Output should contain actual Config compatibility version: " + expectedConfigCompatVersion);

        } finally {
            if (env != null) {
                env.destroy();
            }
        }
    }

    /**
     * Provides ConnectionUri parameters for data mart database testing.
     */
    public List<Arguments> getDataMartUriParameters() throws Exception {
        List<Arguments> result = new LinkedList<>();

        // Test 1: SzCoreSettingsUri - extracts database from core settings
        ConnectionUri coreSettingsUri = SzReplicatorOption.parseDatabaseUri(
            "sz://core-settings/SQL/CONNECTION");
        result.add(Arguments.of(coreSettingsUri));

        // Test 2: PostgreSqlUri - direct connection to embedded PostgreSQL
        int port = embeddedPostgres.getPort();
        PostgreSqlUri postgresConnUri = new PostgreSqlUri("postgres", "postgres", "localhost", port, "postgres");
        result.add(Arguments.of(postgresConnUri));

        // Test 3: SQLiteUri - SQLite data mart file
        SQLiteUri sqliteUri = new SQLiteUri(this.dataMartDbFile);
        result.add(Arguments.of(sqliteUri));

        return result;
    }

    /**
     * Test SzReplicator constructor and getStatistics() with different data mart URIs.
     */
    @ParameterizedTest
    @MethodSource("getDataMartUriParameters")
    public void testSzReplicatorConstructorAndGetStatistics(ConnectionUri dataMartUri) throws Exception {
        SzReplicator replicator = null;
        try {
            // Create options with core settings and database URI
            SzReplicatorOptions options = new SzReplicatorOptions();
            options.setCoreSettings(JsonUtilities.parseJsonObject(this.coreSettings));

            // Set data mart database URI (from parameter)
            options.setDatabaseUri(dataMartUri);

            // Use database queue (since we're not setting up SQS or RabbitMQ)
            options.setUsingDatabaseQueue(true);

            // Construct SzReplicator (without starting processing)
            replicator = new SzReplicator(options);

            // Get statistics
            Map<Statistic, Number> stats = replicator.getStatistics();

            // Verify statistics map
            assertNotNull(stats, "Statistics map should not be null");
            assertFalse(stats.isEmpty(), "Statistics map should not be empty");

            // Verify map contains expected statistic types
            // (exact statistics depend on implementation, but should have some)
            assertTrue(stats.size() > 0,
                "Should have at least one statistic");

            // Capture console output for printStatisticsMap
            SystemOut systemOut = new SystemOut();
            systemOut.execute(() -> {
                SzReplicator.printStatisticsMap(stats);
            });
            String output = systemOut.getText();

            // Verify output
            assertNotNull(output, "Console output should not be null");
            assertFalse(output.trim().isEmpty(),
                "Console output should not be empty");

            // Verify output contains statistic names and values
            // Each line should have format: "  name: value [units]"
            String[] lines = output.split("\n");
            assertTrue(lines.length > 0,
                "Output should have at least one line");

            // Verify at least one line contains a colon (name: value format)
            boolean hasColonFormat = false;
            for (String line : lines) {
                if (line.contains(":")) {
                    hasColonFormat = true;
                    break;
                }
            }
            assertTrue(hasColonFormat,
                "Output should contain lines in 'name: value' format");

        } finally {
            if (replicator != null) {
                replicator.shutdown();
            }
        }
    }

    // ========================================================================
    // Usage/Help Text Tests
    // ========================================================================

    /**
     * Test printUsageIntro() outputs expected content.
     */
    @Test
    public void testPrintUsageIntro() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        SzReplicator.printUsageIntro(pw);
        pw.flush();

        String output = sw.toString();

        assertNotNull(output);
        assertFalse(output.trim().isEmpty());
        assertTrue(output.contains("java -jar"),
            "Should contain 'java -jar' command");
        assertTrue(output.contains("<options>"),
            "Should mention options placeholder");
    }

    /**
     * Test printStandardOptionsUsage() documents all standard options.
     */
    @Test
    public void testPrintStandardOptionsUsage() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        SzReplicator.printStandardOptionsUsage(pw);
        pw.flush();

        String output = sw.toString();

        assertNotNull(output);
        assertFalse(output.trim().isEmpty());

        // Verify all 10 standard option flags are documented
        assertTrue(output.contains("--help"),
            "Should document --help");
        assertTrue(output.contains("--version"),
            "Should document --version");
        assertTrue(output.contains("--ignore-environment"),
            "Should document --ignore-environment");
        assertTrue(output.contains("--core-instance-name"),
            "Should document --core-instance-name");
        assertTrue(output.contains("--core-settings"),
            "Should document --core-settings");
        assertTrue(output.contains("--core-config-id"),
            "Should document --core-config-id");
        assertTrue(output.contains("--core-log-level"),
            "Should document --core-log-level");
        assertTrue(output.contains("--core-concurrency"),
            "Should document --core-concurrency");
        assertTrue(output.contains("--refresh-config-seconds"),
            "Should document --refresh-config-seconds");
        assertTrue(output.contains("--processing-rate"),
            "Should document --processing-rate");

        // Verify section header
        assertTrue(output.contains("Standard Options"),
            "Should have Standard Options section header");

        // Verify environment variables are mentioned
        assertTrue(output.contains("VIA ENVIRONMENT"),
            "Should mention environment variables");
    }

    /**
     * Test printInfoQueueOptionsUsage() documents all info queue options.
     */
    @Test
    public void testPrintInfoQueueOptionsUsage() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        SzReplicator.printInfoQueueOptionsUsage(pw);
        pw.flush();

        String output = sw.toString();

        assertNotNull(output);
        assertFalse(output.trim().isEmpty());

        // Verify all 4 info queue option flags are documented
        assertTrue(output.contains("--database-info-queue"),
            "Should document --database-info-queue");
        assertTrue(output.contains("--sqs-info-uri"),
            "Should document --sqs-info-uri");
        assertTrue(output.contains("--rabbit-info-uri"),
            "Should document --rabbit-info-uri");
        assertTrue(output.contains("--rabbit-info-queue"),
            "Should document --rabbit-info-queue");

        // Verify section header
        assertTrue(output.contains("Info Queue"),
            "Should have Info Queue section header");

        // Verify examples are included
        assertTrue(output.contains("EXAMPLE:") || output.contains("example"),
            "Should include examples");

        // Verify environment variables are mentioned
        assertTrue(output.contains("VIA ENVIRONMENT"),
            "Should mention environment variables");
    }

    /**
     * Test printDatabaseOptionsUsage() documents DATABASE_URI option.
     */
    @Test
    public void testPrintDatabaseOptionsUsage() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        SzReplicator.printDatabaseOptionsUsage(pw);
        pw.flush();

        String output = sw.toString();

        assertNotNull(output);
        assertFalse(output.trim().isEmpty());

        // Verify DATABASE_URI option is documented
        assertTrue(output.contains("--database-uri"),
            "Should document --database-uri");

        // Verify section header
        assertTrue(output.contains("Database") && output.contains("Connectivity"),
            "Should have Database Connectivity section header");

        // Verify all database types are mentioned
        assertTrue(output.contains("PostgreSQL") || output.contains("postgresql"),
            "Should mention PostgreSQL");
        assertTrue(output.contains("SQLite") || output.contains("sqlite"),
            "Should mention SQLite");
        assertTrue(output.contains("core-settings") || output.contains("SzCoreSettings"),
            "Should mention SzCoreSettings format");

        // Verify examples are included
        assertTrue(output.contains("EXAMPLE:"),
            "Should include examples");

        // Verify environment variable is mentioned
        assertTrue(output.contains("VIA ENVIRONMENT"),
            "Should mention environment variable");

        // Verify default value is mentioned
        assertTrue(output.contains("Default:") || output.contains("default"),
            "Should mention default value");

        // Verify SQLite warning about concurrent writes is present
        assertTrue(output.contains("simultaneously") || output.contains("concurrent"),
            "Should warn about simultaneous/concurrent access issues");
    }

    /**
     * Test getUsageString() includes all usage sections.
     */
    @Test
    public void testGetUsageString() {
        String usage = SzReplicator.getUsageString();

        assertNotNull(usage);
        assertFalse(usage.trim().isEmpty());

        // Verify contains intro
        assertTrue(usage.contains("java -jar"),
            "Should contain intro with java -jar");

        // Verify contains standard options section
        assertTrue(usage.contains("Standard Options"),
            "Should contain Standard Options section");
        assertTrue(usage.contains("--help"),
            "Should contain --help from standard options");
        assertTrue(usage.contains("--core-concurrency"),
            "Should contain --core-concurrency from standard options");

        // Verify contains info queue section
        assertTrue(usage.contains("Info Queue"),
            "Should contain Info Queue section");
        assertTrue(usage.contains("--sqs-info-uri"),
            "Should contain --sqs-info-uri from info queue options");
        assertTrue(usage.contains("--rabbit-info-uri"),
            "Should contain --rabbit-info-uri from info queue options");

        // Verify contains database section
        assertTrue(usage.contains("Database") && usage.contains("Connectivity"),
            "Should contain Database Connectivity section");
        assertTrue(usage.contains("--database-uri"),
            "Should contain --database-uri from database options");

        // Verify all major option flags are present (spot check)
        assertTrue(usage.contains("--version"));
        assertTrue(usage.contains("--core-settings"));
        assertTrue(usage.contains("--processing-rate"));
        assertTrue(usage.contains("--database-info-queue"));

        // Verify environment variables are mentioned
        assertTrue(usage.contains("VIA ENVIRONMENT"),
            "Should mention environment variables");
    }

    // ========================================================================
    // main() Method Tests
    // ========================================================================

    /**
     * Test main() with --help argument.
     */
    @Test
    public void testMainWithHelp() throws Exception {
        // Capture console output
        SystemOut systemOut = new SystemOut();
        systemOut.execute(() -> {
            SzReplicator.main(new String[]{"--help"});
        });
        String output = systemOut.getText();

        // Verify output contains usage information
        assertNotNull(output, "Output should not be null");
        assertFalse(output.trim().isEmpty(), "Output should not be empty");

        // Verify it contains help/usage content
        assertTrue(output.contains("java -jar"),
            "Should contain usage command");
        assertTrue(output.contains("--help"),
            "Should document --help option");
        assertTrue(output.contains("Standard Options"),
            "Should contain Standard Options section");
        assertTrue(output.contains("Info Queue"),
            "Should contain Info Queue section");
        assertTrue(output.contains("Database"),
            "Should contain Database section");

        // Verify multiple options are documented
        assertTrue(output.contains("--core-concurrency"),
            "Should document --core-concurrency");
        assertTrue(output.contains("--database-uri"),
            "Should document --database-uri");
        assertTrue(output.contains("--sqs-info-uri"),
            "Should document --sqs-info-uri");
    }

    /**
     * Test main() with --version argument.
     */
    @Test
    public void testMainWithVersion() throws Exception {
        // Capture console output
        SystemOut systemOut = new SystemOut();
        systemOut.execute(() -> {
            SzReplicator.main(new String[]{"--version"});
        });
        String output = systemOut.getText();

        // Verify output contains version information
        assertNotNull(output, "Output should not be null");
        assertFalse(output.trim().isEmpty(), "Output should not be empty");

        // Verify it contains version labels
        assertTrue(output.contains("Senzing Replicator Version") ||
                   output.contains("Version"),
            "Should contain version information");
        assertTrue(output.contains("Senzing Native API Version") ||
                   output.contains("Native API"),
            "Should contain Native API version");

        // Verify it contains MAVEN_VERSION
        assertTrue(output.contains(BuildInfo.MAVEN_VERSION),
            "Should contain MAVEN_VERSION: " + BuildInfo.MAVEN_VERSION);

        // Verify it contains a version number pattern
        assertTrue(output.matches("(?s).*\\d+\\.\\d+\\.\\d+.*"),
            "Should contain version numbers in x.y.z format");
    }

    // ========================================================================
    // RabbitMQ and SQS Consumer Tests
    // ========================================================================

    /**
     * Test SzReplicator with RabbitMQ consumer configuration.
     */
    @Test
    public void testSzReplicatorWithRabbitMQConsumer() throws Exception {
        TestableReplicator replicator = null;
        try {
            // Create options with RabbitMQ configuration
            SzReplicatorOptions options = new SzReplicatorOptions();
            options.setCoreSettings(JsonUtilities.parseJsonObject(this.coreSettings));

            // Set RabbitMQ URI and queue
            RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "pass", "localhost", "vhost");
            options.setRabbitMqUri(rabbitUri);
            options.setRabbitMqInfoQueue("test-queue");

            // Set data mart database
            SQLiteUri dataMartUri = new SQLiteUri(this.dataMartDbFile);
            options.setDatabaseUri(dataMartUri);

            // Construct TestableReplicator (tests RabbitMQ code path)
            replicator = new TestableReplicator(options);

            // If we get here, RabbitMQ consumer was created successfully
            assertNotNull(replicator, "Replicator should be created");

        } finally {
            if (replicator != null) {
                replicator.shutdown();
            }
        }
    }

    /**
     * Test SzReplicator with SQS consumer configuration.
     */
    @Test
    public void testSzReplicatorWithSQSConsumer() throws Exception {
        TestableReplicator replicator = null;
        try {
            // Create options with SQS configuration
            SzReplicatorOptions options = new SzReplicatorOptions();
            options.setCoreSettings(JsonUtilities.parseJsonObject(this.coreSettings));

            // Set SQS URI
            SQSUri sqsUri = new SQSUri(
                new java.net.URI("https://sqs.us-east-1.amazonaws.com/123456/TestQueue"));
            options.setSQSInfoUri(sqsUri);

            // Set data mart database
            SQLiteUri dataMartUri = new SQLiteUri(this.dataMartDbFile);
            options.setDatabaseUri(dataMartUri);

            // Construct TestableReplicator (tests SQS code path)
            replicator = new TestableReplicator(options);

            // If we get here, SQS consumer was created successfully
            assertNotNull(replicator, "Replicator should be created");

        } finally {
            if (replicator != null) {
                replicator.shutdown();
            }
        }
    }

    /**
     * Test main() with database queue - starts replicator and triggers shutdown via SQLException.
     */
    @Test
    public void testMainWithDatabaseQueue() throws Exception {
        // Create a separate EmbeddedPostgres for data mart (will be closed to trigger shutdown)
        EmbeddedPostgres dataMartPostgres = EmbeddedPostgres.start();

        try {
            // Build PostgreSqlUri for data mart
            int port = dataMartPostgres.getPort();
            PostgreSqlUri dataMartUri = new PostgreSqlUri("postgres", "postgres", "localhost", port, "postgres");

            // Prepare command-line arguments
            String[] args = new String[] {
                "--core-settings", this.coreSettings,
                "--database-uri", dataMartUri.toString(),
                "--database-info-queue"
            };

            // Launch background thread to close database after delay
            Thread shutdownThread = new Thread(() -> {
                try {
                    // Wait for replicator to start
                    Thread.sleep(4000); // 4 seconds

                    // Close the database to trigger SQLException
                    dataMartPostgres.close();

                } catch (Exception e) {
                    // Ignore - test will handle verification
                }
            });
            shutdownThread.setDaemon(true);
            shutdownThread.start();

            // Capture System.out and System.err
            SystemOut systemOut = new SystemOut();
            SystemErr systemErr = new SystemErr();

            systemOut.execute(() -> {
                systemErr.execute(() -> {
                    // Call main() - should start, run briefly, then shutdown due to SQLException
                    SzReplicator.main(args);
                });
            });

            String stdoutOutput = systemOut.getText();
            String stderrOutput = systemErr.getText();

            // Combine outputs for verification
            String combinedOutput = stdoutOutput + stderrOutput;

            // Verify commandLineStart() options output (lines 234-238)
            assertTrue(combinedOutput.contains("Core Settings") ||
                       combinedOutput.contains("CORE_SETTINGS") ||
                       combinedOutput.contains("OPTIONS"),
                "Should contain options output from commandLineStart");

            // Verify options printout (line 243)
            assertTrue(combinedOutput.contains("database") ||
                       combinedOutput.contains("DATABASE"),
                "Should contain database configuration in options output");

            // Verify replicator lifecycle messages (lines 258-263)
            assertTrue(combinedOutput.contains("STARTING") &&
                       combinedOutput.contains("REPLICATOR"),
                "Should contain 'STARTING REPLICATOR' message");
            assertTrue(combinedOutput.contains("STARTED") &&
                       combinedOutput.contains("REPLICATOR"),
                "Should contain 'STARTED REPLICATOR' message");
            assertTrue(combinedOutput.contains("JOINING") &&
                       combinedOutput.contains("REPLICATOR"),
                "Should contain 'JOINING REPLICATOR' message");
            assertTrue(combinedOutput.contains("JOINED") &&
                       combinedOutput.contains("REPLICATOR"),
                "Should contain 'JOINED REPLICATOR' message");

            // Verify SQLException-related error messages
            assertTrue(combinedOutput.toLowerCase().contains("error") ||
                       combinedOutput.toLowerCase().contains("exception") ||
                       combinedOutput.toLowerCase().contains("sql"),
                "Should contain error messages from SQLException");

        } finally {
            // Ensure database is closed
            if (dataMartPostgres != null) {
                try {
                    dataMartPostgres.close();
                } catch (Exception ignore) {
                    // Already closed
                }
            }
        }
    }

    // ========================================================================
    // Helper Classes
    // ========================================================================

    /**
     * Testable subclass of SzReplicator that uses TestableRabbitMQConsumer
     * and TestableSQSConsumer instead of real consumers.
     */
    private static class TestableReplicator extends SzReplicator {
        /**
         * Constructs with {@link SzReplicatorOptions}.
         */
        public TestableReplicator(SzReplicatorOptions options) throws Exception {
            super(options);
        }

        /**
         * Constructs with {@link SzReplicatorOptions} and startProcessing flag.
         */
        public TestableReplicator(SzReplicatorOptions options, boolean startProcessing) throws Exception {
            super(options, startProcessing);
        }

        /**
         * Constructs with {@link SzEnvironment} and {@link SzReplicatorOptions}.
         */
        public TestableReplicator(SzEnvironment environment, SzReplicatorOptions options, boolean startProcessing)
            throws Exception
        {
            super(environment, options, startProcessing);
        }

        /**
         * Returns TestableRabbitMQConsumer instead of real RabbitMQConsumer.
         */
        @Override
        protected RabbitMQConsumer createRabbitMQConsumer() {
            return new TestableRabbitMQConsumer();
        }

        /**
         * Returns TestableSQSConsumer instead of real SQSConsumer.
         */
        @Override
        protected SQSConsumer createSQSConsumer() {
            return new TestableSQSConsumer();
        }
    }

}

