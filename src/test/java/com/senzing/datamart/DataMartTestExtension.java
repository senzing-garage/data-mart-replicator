package com.senzing.datamart;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sql.DatabaseType;
import com.senzing.util.SzInstallLocations;
import com.senzing.util.SzUtilities;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * JUnit Jupiter extension that provides shared test resources for data mart tests.
 * Sets up PostgreSQL and SQLite repositories with Senzing schema that persist
 * across all test classes in a test run.
 *
 * <p>
 * Each repository type provides two databases: one for Senzing (with schema and
 * default configuration) and one for the data mart (schema created at runtime
 * by the data mart components).
 * </p>
 */
public class DataMartTestExtension implements BeforeAllCallback {

    /**
     * Flag to track whether global initialization has occurred.
     */
    private static boolean initialized = false;
    
    /**
     * The internal monitor to use for synchronization.
     */
    private static final Object MONITOR = new Object();

    /**
     * Thread-safe map of repository type to repository instances.
     */
    private static final Map<RepositoryType, Repository> REPOSITORIES
        = Collections.synchronizedMap(new EnumMap<>(RepositoryType.class));

    /**
     * The Senzing install locations for finding schema files.
     * Throws IllegalStateException if Senzing is not installed.
     */
    private static final SzInstallLocations INSTALL_LOCATIONS = SzInstallLocations.findLocations();

    /**
     * Enumeration of supported repository types for testing.
     */
    public enum RepositoryType {
        /**
         * PostgreSQL database repository.
         */
        POSTGRESQL {
            @Override
            public DatabaseType getDatabaseType() {
                return DatabaseType.POSTGRESQL;
            }

            @Override
            public File getSchemaFile() {
                File resourceDir = INSTALL_LOCATIONS.getResourceDirectory();
                File schemaDir = new File(resourceDir, "schema");
                return new File(schemaDir, "szcore-schema-postgresql-create.sql");
            }
        },

        /**
         * SQLite database repository.
         */
        SQLITE {
            @Override
            public DatabaseType getDatabaseType() {
                return DatabaseType.SQLITE;
            }

            @Override
            public File getSchemaFile() {
                File resourceDir = INSTALL_LOCATIONS.getResourceDirectory();
                File schemaDir = new File(resourceDir, "schema");
                return new File(schemaDir, "szcore-schema-sqlite-create.sql");
            }
        };

        /**
         * Gets the corresponding {@link DatabaseType} for this repository type.
         *
         * @return The {@link DatabaseType} for this repository type.
         */
        public abstract DatabaseType getDatabaseType();

        /**
         * Gets the schema file for the Senzing core database.
         *
         * @return The {@link File} representing the schema SQL file.
         */
        public abstract File getSchemaFile();
    }

    /**
     * Immutable class representing a test repository containing a Senzing
     * database and a data mart database of the same type.
     */
    public static final class Repository {
        /**
         * The repository type.
         */
        private final RepositoryType repositoryType;

        /**
         * The connection URI for the Senzing database.
         */
        private final ConnectionUri connectionUri;

        /**
         * The JDBC URL for the data mart database.
         */
        private final String dataMartJdbcUrl;

        /**
         * The Senzing core settings JSON.
         */
        private final String coreSettings;

        /**
         * The list of database resources (Files for SQLite, EmbeddedPostgres for PostgreSQL).
         */
        private final List<?> databases;

        /**
         * Constructs a new immutable Repository instance.
         *
         * @param repositoryType  The repository type.
         * @param connectionUri   The connection URI for the Senzing database.
         * @param dataMartJdbcUrl The JDBC URL for the data mart database.
         * @param coreSettings    The Senzing core settings JSON.
         * @param databases       The list of database resources to manage.
         */
        private Repository(RepositoryType  repositoryType,
                           ConnectionUri   connectionUri,
                           String          dataMartJdbcUrl,
                           String          coreSettings,
                           List<?>         databases)
        {
            this.repositoryType  = repositoryType;
            this.connectionUri   = connectionUri;
            this.dataMartJdbcUrl = dataMartJdbcUrl;
            this.coreSettings    = coreSettings;
            this.databases       = Collections.unmodifiableList(new ArrayList<>(databases));
        }

        /**
         * Gets the repository type.
         *
         * @return The {@link RepositoryType} for this repository.
         */
        public RepositoryType getRepositoryType() {
            return this.repositoryType;
        }

        /**
         * Gets the connection URI for the Senzing database.
         *
         * @return The {@link ConnectionUri} (either {@link PostgreSqlUri} or
         *         {@link SQLiteUri}) for use in SENZING_ENGINE_CONFIGURATION_JSON.
         */
        public ConnectionUri getConnectionUri() {
            return this.connectionUri;
        }

        /**
         * Gets the JDBC URL for the data mart database.
         *
         * @return The JDBC URL string for connecting to the data mart database.
         */
        public String getDataMartJdbcUrl() {
            return this.dataMartJdbcUrl;
        }

        /**
         * Gets the Senzing Core SDK settings JSON for initializing
         * {@link SzCoreEnvironment}.
         *
         * @return The settings JSON string (SENZING_ENGINE_CONFIGURATION_JSON).
         */
        public String getCoreSettings() {
            return this.coreSettings;
        }

        /**
         * Gets the list of database resources for cleanup.
         * Returns {@link File} objects for SQLite or {@link EmbeddedPostgres}
         * instances for PostgreSQL.
         *
         * @return The list of database resources.
         */
        private List<?> getDatabases() {
            return this.databases;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Initializes shared test repositories on first invocation.
     * Subsequent invocations are no-ops.
     * </p>
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        synchronized (MONITOR) {
            if (!initialized) {
                Thread shutdownHook = null;

                try {
                    // first register the shutdown hook
                    shutdownHook = registerShutdownHook();

                    // attempt to initialize the repositories (this may fail)
                    initializeRepositories();

                   // if successful, mark everything as initialized
                   initialized = true;

                } catch (Exception e) {
                    if (shutdownHook != null) {
                        // unregister the shutdown hook (we will run it now in this thread)
                        Runtime.getRuntime().removeShutdownHook(shutdownHook);

                        // call the run() method from the shutdown hook in this thread
                        // to cleanup anything that might have been initialized
                        shutdownHook.run();
                    }

                    // rethrow the exception
                    throw e;
                }
            }
        }
    }

    /**
     * Gets the repository for the specified type.
     *
     * @param repoType The {@link RepositoryType} to retrieve.
     *
     * @return The {@link Repository} for the specified type, or {@code null}
     *         if not yet initialized.
     */
    public static Repository getRepository(RepositoryType repoType) {
        return REPOSITORIES.get(repoType);
    }

    /**
     * Initializes all supported repository types.
     *
     * @throws Exception If initialization fails.
     */
    private static void initializeRepositories() throws Exception {
        for (RepositoryType type : RepositoryType.values()) {
            Repository repo = setupRepository(type);
            REPOSITORIES.put(type, repo);
        }
    }

    /**
     * Registers the shutdown hook to clean up resources and returns the 
     * {@link Thread} representing the shutdown hook.
     * 
     * @return The {@link Thread} representing the shutdown hook that was registered.
     */
    private static Thread registerShutdownHook() {
        Thread shutdownHook = new Thread(() -> {
            for (Map.Entry<RepositoryType, Repository> entry : REPOSITORIES.entrySet()) {
                RepositoryType type = entry.getKey();
                Repository repo = entry.getValue();
                if (repo == null) {
                    continue;
                }
                List<?> databases = repo.getDatabases();
                for (Object db : databases) {
                    try {
                        switch (type) {
                            case SQLITE:
                                File file = (File) db;
                                if (file.exists()) {
                                    file.delete();
                                }
                                break;
                            case POSTGRESQL:
                                EmbeddedPostgres pg = (EmbeddedPostgres) db;
                                pg.close();
                                break;
                        }
                    } catch (Exception e) {
                        System.err.println("Error cleaning up " + type + " database: " + e.getMessage());
                    }
                }
            }
        }, "DataMartTestExtension-Shutdown");

        Runtime.getRuntime().addShutdownHook(shutdownHook);

        return shutdownHook;
    }

    /**
     * Sets up a repository of the specified type.
     *
     * @param type The {@link RepositoryType} to set up.
     *
     * @return The configured {@link Repository} instance.
     *
     * @throws Exception If setup fails.
     */
    private static Repository setupRepository(RepositoryType type) throws Exception {
        switch (type) {
            case POSTGRESQL:
                return setupPostgreSqlRepository();
            case SQLITE:
                return setupSqliteRepository();
            default:
                throw new IllegalArgumentException("Unsupported repository type: " + type);
        }
    }

    /**
     * Sets up a PostgreSQL repository using Zonky Embedded PostgreSQL.
     *
     * @return The configured {@link Repository} instance.
     *
     * @throws Exception If setup fails.
     */
    private static Repository setupPostgreSqlRepository() throws Exception {
        // Create one embedded PostgreSQL instance since both Senzing and data mart
        // can share the same PostgreSQL database (not possible for SQLite due to
        // concurrent write operations by native Senzing code and Java data mart code)
        EmbeddedPostgres embeddedPg = EmbeddedPostgres.builder().start();

        try {
            List<EmbeddedPostgres> databases = List.of(embeddedPg);

            int pgPort = embeddedPg.getPort();

            // Create the connection URI for Senzing
            PostgreSqlUri connectionUri = new PostgreSqlUri(
                "postgres", "postgres", "localhost", pgPort, "postgres");

            // Create the JDBC URL for the data mart
            String jdbcUrl = "jdbc:postgresql://localhost:" + pgPort + "/postgres";

            // Create Senzing schema
            createSenzingSchema(RepositoryType.POSTGRESQL, jdbcUrl);

            // Build the core settings JSON using SzUtilities
            String coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

            // Initialize the default configuration
            initializeDefaultConfig(coreSettings);

            return new Repository(
                RepositoryType.POSTGRESQL,
                connectionUri,
                jdbcUrl,
                coreSettings,
                databases
            );

        } catch (Exception e) {
            // cleanup upon failure
            if (embeddedPg != null) {
                embeddedPg.close();
            }

            // rethrow
            throw e;
        }
    }

    /**
     * Sets up a SQLite repository using temporary database files.
     *
     * @return The configured {@link Repository} instance.
     *
     * @throws Exception If setup fails.
     */
    private static Repository setupSqliteRepository() throws Exception {
        // Create temporary files for SQLite databases
        File senzingDbFile = File.createTempFile("senzing_test_", ".db");
        senzingDbFile.deleteOnExit();

        File dataMartDbFile = File.createTempFile("datamart_test_", ".db");
        dataMartDbFile.deleteOnExit();

        try {
            List<File> databases = List.of(senzingDbFile, dataMartDbFile);

            // Create the connection URI for Senzing
            SQLiteUri connectionUri = new SQLiteUri(senzingDbFile);

            // Create the JDBC URL for the data mart
            String dataMartJdbcUrl = "jdbc:sqlite:" + dataMartDbFile.getAbsolutePath();

            // Build the Senzing JDBC URL for schema creation
            String senzingJdbcUrl = "jdbc:sqlite:" + senzingDbFile.getAbsolutePath();

            // Create Senzing schema
            createSenzingSchema(RepositoryType.SQLITE, senzingJdbcUrl);

            // Build the core settings JSON using SzUtilities
            String coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

            // Initialize the default configuration
            initializeDefaultConfig(coreSettings);

            return new Repository(
                RepositoryType.SQLITE,
                connectionUri,
                dataMartJdbcUrl,
                coreSettings,
                databases
            );

        } catch (Exception e) {
            // delete the senzing DB file if created
            if (senzingDbFile != null) {
                senzingDbFile.delete();
            }

            // delete the data mart DB file if created
            if (dataMartDbFile != null) {
                dataMartDbFile.delete();
            }

            // rethrow
            throw e;
        }
    }

    /**
     * Creates the Senzing database schema from the schema file.
     *
     * @param type    The repository type.
     * @param jdbcUrl The JDBC URL for the database.
     *
     * @throws SQLException If a database error occurs.
     * @throws IOException  If reading the schema file fails.
     */
    private static void createSenzingSchema(RepositoryType type, String jdbcUrl)
        throws SQLException, IOException
    {
        File schemaFile = type.getSchemaFile();
        if (!schemaFile.exists()) {
            throw new IOException("Schema file not found: " + schemaFile.getAbsolutePath());
        }

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
     * @param coreSettings The core settings JSON.
     *
     * @throws SzException If a Senzing error occurs.
     */
    private static void initializeDefaultConfig(String coreSettings) throws SzException {
        SzCoreEnvironment env = null;
        try {
            env = SzCoreEnvironment.newBuilder()
                .instanceName("DataMartTestSetup")
                .settings(coreSettings)
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
}
