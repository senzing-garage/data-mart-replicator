package com.senzing.datamart;

import com.senzing.datamart.model.SzRecord;
import com.senzing.datamart.model.SzRelatedEntity;
import com.senzing.datamart.model.SzResolvedEntity;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.core.auto.SzAutoCoreEnvironment;
import com.senzing.sql.Connector;
import com.senzing.sql.DatabaseType;
import com.senzing.sql.PostgreSqlConnector;
import com.senzing.sql.SQLiteConnector;
import com.senzing.util.SzInstallLocations;
import com.senzing.util.SzUtilities;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.json.JsonArray;
import javax.json.JsonObject;

import static java.nio.charset.StandardCharsets.UTF_8;
import static com.senzing.sdk.SzFlag.*;
import static com.senzing.util.JsonUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
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
@Execution(ExecutionMode.SAME_THREAD)
public class DataMartTestExtension implements BeforeAllCallback {
    /**
     * System property to preserve the SQLite databases for analysis
     * rather than clean them up from disk.  Its value is <code>{@value}</code>.
     */
    public static final String TEST_PRESERVE_SYSTEM_PROPERTY 
        = "com.senzing.datamart.test.preserve";

    /**
     * The resource file name for the truth set.
     */
    private static final String TRUTH_SET_RESOURCE_NAME = "truth-set.jsonl";

    /**
     * The flags to use when retrieving the entity from the G2 repository.
     */
    private static final Set<SzFlag> ENTITY_FLAGS;
    static {
        EnumSet<SzFlag> enumSet = EnumSet.of(SZ_ENTITY_INCLUDE_ENTITY_NAME,
                SZ_ENTITY_INCLUDE_RECORD_DATA,
                SZ_ENTITY_INCLUDE_RECORD_MATCHING_INFO,
                SZ_ENTITY_INCLUDE_RELATED_MATCHING_INFO,
                SZ_ENTITY_INCLUDE_RELATED_RECORD_DATA);
        enumSet.addAll(SZ_ENTITY_INCLUDE_ALL_RELATIONS);
        ENTITY_FLAGS = Collections.unmodifiableSet(enumSet);
    }

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
    private static final SzInstallLocations INSTALL_LOCATIONS 
        = SzInstallLocations.findLocations();

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
        private final ConnectionUri senzingConnUri;

        /**
         * The connection URI for the data mart database.
         */
        private final ConnectionUri dataMartConnUri;

        /**
         * The JDBC URL for the data mart database.
         */
        private final String dataMartJdbcUrl;

        /**
         * The {@link Connector} for connecting to the data mart. 
         */
        private final Connector dataMartConnector;

        /**
         * The Senzing core settings JSON.
         */
        private final String coreSettings;

        /**
         * The list of database resources (Files for SQLite, EmbeddedPostgres for PostgreSQL).
         */
        private final List<?> databases;

        /**
         * The {@link Set} of {@link String} data source codes that are configured.
         */
        private final Set<String> dataSources;

        /**
         * The {@link Set} of {@link String} data source codes identifying the 
         * configured data sources with loaded records.
         */
        private final Set<String> loadedSources;

        /** 
         * The <b>unmodifiable</b> {@link Set} of {@link SzRecordKey} instances
         * identifying all records loaded into the repository.
         */
        private final Set<SzRecordKey> recordKeys;

        /** 
         * The <b>unmodifiable</b> {@link Map} of {@link Long} entity ID keys
         * to {@link SzResolvedEntity} values.
         */
        private final SortedMap<Long, SzResolvedEntity> entities;

        /**
         * The {@link Map} of {@link String} keys that are match keys
         * mapped to {@link Set} values containing the associated
         * {@link String} principle values.
         */
        private final Map<String, Set<String>> relatedMatchKeyMap;

        /**
         * The {@link Map} of {@link String} principle keys to {@link Set}
         * values containing the associated {@link String} match key values.
         */
        private final Map<String, Set<String>> relatedPrincipleMap;

        /**
         * Constructs a new immutable Repository instance.
         *
         * @param repositoryType    The repository type.
         * @param senzingConnUri    The connection URI for the Senzing database.
         * @param datraMartConnUri  The connection URI for the data mart database.
         * @param dataMartJdbcUrl   The JDBC URL for the data mart database.
         * @param dataMartConnector The connector for for the data mart database.
         * @param coreSettings      The Senzing core settings JSON.
         * @param recordKeys        The set of all record keys for records loaded.
         * @param entities          The map of entity ID keys to entity values.
         * @param databases         The list of database resources to manage.
         */
        private Repository(RepositoryType                       repositoryType,
                           ConnectionUri                        senzingConnUri,
                           ConnectionUri                        dataMartConnUri,
                           String                               dataMartJdbcUrl,
                           Connector                            dataMartConnector,
                           String                               coreSettings,
                           Set<String>                          dataSources,
                           Set<SzRecordKey>                     recordKeys,
                           SortedMap<Long, SzResolvedEntity>    entities,
                           List<?>                              databases)
        {
            this.repositoryType     = repositoryType;
            this.senzingConnUri     = senzingConnUri;
            this.dataMartConnUri    = dataMartConnUri;
            this.dataMartJdbcUrl    = dataMartJdbcUrl;
            this.dataMartConnector  = dataMartConnector;
            this.coreSettings       = coreSettings;
            this.dataSources        = Collections.unmodifiableSet(dataSources);
            this.databases          = Collections.unmodifiableList(new ArrayList<>(databases));
            this.recordKeys         = Collections.unmodifiableSet(recordKeys);
            this.entities           = Collections.unmodifiableSortedMap(entities);

            // figure out the set of data sources with loaded records
            Set<String> loadedSet = new TreeSet<>();
            for (SzRecordKey key : this.recordKeys) {
                loadedSet.add(key.dataSourceCode());
            }
            this.loadedSources = Collections.unmodifiableSet(loadedSet);

            // get the principles and match keys
            Map<String, Set<String>> matchKeyMap = new TreeMap<>();
            Map<String, Set<String>> principleMap = new TreeMap<>();
            for (SzResolvedEntity entity : this.entities.values()) {
                Map<Long, SzRelatedEntity> relatedMap = entity.getRelatedEntities();
                
                for (SzRelatedEntity related : relatedMap.values()) {
                    String matchKey     = related.getMatchKey();
                    String principle    = related.getPrinciple();

                    Set<String> principles  = matchKeyMap.get(matchKey);
                    Set<String> matchKeys   = principleMap.get(principle);
                    if (principles == null) {
                        principles = new TreeSet<>();
                        matchKeyMap.put(matchKey, principles);
                    }
                    if (matchKeys == null) {
                        matchKeys = new TreeSet<>();
                        principleMap.put(principle, matchKeys);
                    }
                    matchKeys.add(matchKey);
                    principles.add(principle);
                }
            }

            // make all the contained sets unmodifiable
            Iterator<Map.Entry<String, Set<String>>> iter
                = matchKeyMap.entrySet().iterator(); 
            while (iter.hasNext()) {
                Map.Entry<String, Set<String>> entry = iter.next();
                entry.setValue(Collections.unmodifiableSet(entry.getValue()));
            }
            iter = principleMap.entrySet().iterator(); 
            while (iter.hasNext()) {
                Map.Entry<String, Set<String>> entry = iter.next();
                entry.setValue(Collections.unmodifiableSet(entry.getValue()));
            }

            this.relatedMatchKeyMap = Collections.unmodifiableMap(matchKeyMap);
            this.relatedPrincipleMap = Collections.unmodifiableMap(principleMap);
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
        public ConnectionUri getSenzingConnectionUri() {
            return this.senzingConnUri;
        }

        /**
         * Gets the connection URI for the data mart database.
         *
         * @return The {@link ConnectionUri} (either {@link PostgreSqlUri} or
         *         {@link SQLiteUri}) for for the {@link SzReplicatorOptions}.
         */
        public ConnectionUri getDataMartConnectionUri() {
            return this.dataMartConnUri;
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
         * Gets the {@Link Connector} for opening a {@link Connection} to
         * the data mart database.
         * 
         * @return The {@Link Connector} for opening a {@link Connection} to
         *         the data mart database.
         */
        public Connector getDataMartConnector() {
            return this.dataMartConnector;
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
         * Gets the <b>unmodifiable</b> {@link Set} of {@link String} data source
         * codes identifying all data sources configured in the repository.
         * 
         * @return The <b>unmodifiable</b> {@link Set} of {@link String} data source
         *         codes identifying all data sources configured in the repository.
         */
        public Set<String> getConfiguredDataSources() {
            return this.dataSources;
        }

        /**
         * Gets the <b>unmodifiable</b> {@link Set} of {@link String} data source
         * codes identifying all data sources configured in the repository with
         * loaded records.
         * 
         * @return The <b>unmodifiable</b> {@link Set} of {@link String} data source
         *         codes identifying all data sources configured in the repository
         *         with loaded records.
         */
        public Set<String> getLoadedDataSources() {
            return this.loadedSources;
        }

        /**
         * Gets the <b>unmodifiable</b> {@link Set} of {@link SzRecordKey} instances
         * identifying all records loaded into the repository.
         * 
         * @return The <b>unmodifiable</b> {@link Set} of all {@link SzRecordKey} instances
         *         identifying all records loaded into the repository.
         */
        public Set<SzRecordKey> getLoadedRecordKeys() {
            return this.recordKeys;
        }

        /**
         * Gets the <b>unmodifiable</b> {@link SortedMap} of {@link Long} entity ID keys
         * to {@link SzResolvedEntity} values describing all entities loaded into the
         * repository.
         * 
         * @return The <b>unmodifiable</b> {@link SortedMap} of {@link Long} entity ID keys
         *         to {@link SzResolvedEntity} values describing all entities loaded
         *         into the repository.
         * 
         */
        public SortedMap<Long, SzResolvedEntity> getLoadedEntities() {
            return this.entities;
        }

        /**
         * Gets the <b>unmodifiable</b> {@link Map} of principle
         * {@link String} keys identifying to <b>unmodifiable</b>
         * {@link Set} values containing the {@link String} match
         * keys paired with those principles.
         * 
         * @return The <b>unmodifiable</b> {@link Map} of principle
         *         {@link String} keys identifying to
         *         <b>unmodifiable</b> {@link Set} values containing
         *         the {@link String} match keys paired with those
         *         principles.
         */
        public Map<String, Set<String>> getRelatedMatchKeys() {
            return this.relatedMatchKeyMap;
        }

        /**
         * Gets the <b>unmodifiable</b> {@link Map} of principle
         * {@link String} keys identifying to <b>unmodifiable</b>
         * {@link Set} values containing the {@link String} match
         * keys paired with those principles.
         * 
         * @return The <b>unmodifiable</b> {@link Map} of principle
         *         {@link String} keys identifying to
         *         <b>unmodifiable</b> {@link Set} values containing
         *         the {@link String} match keys paired with those
         *         principles.
         */
        public Map<String, Set<String>> getRelatedPrinciples() {
            return this.relatedPrincipleMap;
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
        for (RepositoryType repoType : RepositoryType.values()) {
            logInfo("Initializing " + repoType + " repository...");
            Repository repo = setupRepository(repoType);
            logInfo("Initialized " + repoType + " repository.");
            REPOSITORIES.put(repoType, repo);
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
                                if (file.exists()
                                    && (!Boolean.TRUE.toString().equals(
                                            System.getProperty(TEST_PRESERVE_SYSTEM_PROPERTY))))
                                {
                                    file.delete();
                                }
                                break;
                            case POSTGRESQL:
                                EmbeddedPostgres pg = (EmbeddedPostgres) db;
                                pg.close();
                                break;
                        }
                    } catch (Exception e) {
                        logError("Error cleaning up " + type + " database: " + e.getMessage());
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

            int     pgPort      = embeddedPg.getPort();
            String  pgHost      = "localhost";
            String  pgDatabase  = "postgres";
            String  pgUser      = "postgres";
            String  pgPassword  = "postgres";

            // Create the connection URI for Senzing
            PostgreSqlUri connectionUri = new PostgreSqlUri(
                pgUser, pgPassword, pgHost, pgPort, pgDatabase);

            // Create the JDBC URL for the data mart
            String jdbcUrl = "jdbc:postgresql://" + pgHost + ":" + pgPort 
                + "/" + pgDatabase + "?user=" + pgUser + "&password=" + pgPassword;
                
            // create the data mart connector
            Connector connector = new PostgreSqlConnector(
                pgHost, pgPort, pgDatabase, pgUser, pgPassword);
            
            // Create Senzing schema
            createSenzingSchema(RepositoryType.POSTGRESQL, jdbcUrl);

            // Build the core settings JSON using SzUtilities
            String coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

            // Initialize the default configuration
            Set<String> dataSources = initializeDefaultConfig(coreSettings);

            // load the truth set and handle the data mart
            Set<SzRecordKey> recordKeys = loadTruthSet(coreSettings, connectionUri);

            SortedMap<Long, SzResolvedEntity> entities = getEntities(coreSettings, recordKeys);

            // return the repository
            return new Repository(
                RepositoryType.POSTGRESQL,
                connectionUri,
                connectionUri,
                jdbcUrl,
                connector,
                coreSettings,
                dataSources,
                recordKeys,
                entities,
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
        File dataMartDbFile = File.createTempFile("datamart_test_", ".db");

        if (!Boolean.TRUE.toString().equals(System.getProperty(TEST_PRESERVE_SYSTEM_PROPERTY))) {
            senzingDbFile.deleteOnExit();
            dataMartDbFile.deleteOnExit();
        } else {
            System.err.println();
            System.err.println("- - - - - - - - - - - - - - - - - - ");
            System.err.println("SENZING DB   : " + senzingDbFile);
            System.err.println("DATA MART DB : " + dataMartDbFile);
            System.err.println();
        }
        try {
            List<File> databases = List.of(senzingDbFile, dataMartDbFile);

            // Create the connection URI for Senzing
            SQLiteUri connectionUri = new SQLiteUri(senzingDbFile);

            // Create the JDBC URL for the data mart
            String      dataMartJdbcUrl = "jdbc:sqlite:" + dataMartDbFile.getAbsolutePath();
            SQLiteUri   dataMartConnUri = new SQLiteUri(dataMartDbFile);
            
            // create the data mart connector
            Connector dataMartConnector = new SQLiteConnector(dataMartDbFile);

            // Build the Senzing JDBC URL for schema creation
            String senzingJdbcUrl = "jdbc:sqlite:" + senzingDbFile.getAbsolutePath();

            // Create Senzing schema
            createSenzingSchema(RepositoryType.SQLITE, senzingJdbcUrl);

            // Build the core settings JSON using SzUtilities
            String coreSettings = SzUtilities.basicSettingsFromDatabaseUri(connectionUri.toString());

            // Initialize the default configuration
            Set<String> dataSources = initializeDefaultConfig(coreSettings);

            // load the truth set and handle the data mart
            Set<SzRecordKey> recordKeys = loadTruthSet(coreSettings, dataMartConnUri);

            SortedMap<Long, SzResolvedEntity> entities = getEntities(coreSettings, recordKeys);

            // return the repository
            return new Repository(
                RepositoryType.SQLITE,
                connectionUri,
                dataMartConnUri,
                dataMartJdbcUrl,
                dataMartConnector,
                coreSettings,
                dataSources,
                recordKeys,
                entities,
                databases
            );

        } catch (Exception e) {
            if (!Boolean.TRUE.toString().equals(System.getProperty(TEST_PRESERVE_SYSTEM_PROPERTY))) {
                // delete the senzing DB file if created
                if (senzingDbFile != null) {
                    senzingDbFile.delete();
                }

                // delete the data mart DB file if created
                if (dataMartDbFile != null) {
                    dataMartDbFile.delete();
                }
            }

            // rethrow
            throw e;
        }
    }

    /**
     * Creates the Senzing database schema from the schema file.
     *
     * @param repoType    The repository type.
     * @param jdbcUrl The JDBC URL for the database.
     *
     * @throws SQLException If a database error occurs.
     * @throws IOException  If reading the schema file fails.
     */
    private static void createSenzingSchema(RepositoryType repoType, String jdbcUrl)
        throws SQLException, IOException
    {
        logInfo("Creating " + repoType + " Senzing Schema...");
        
        File schemaFile = repoType.getSchemaFile();
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
        } finally {
            logInfo("Created " + repoType + " Senzing Schema.");
        }
    }

    /**
     * Initializes the default Senzing configuration with required data sources.
     *
     * @param coreSettings The core settings JSON.
     *
     * @return The {@link Set} of {@link String} data source codes that were added.
     * 
     * @throws SzException If a Senzing error occurs.
     */
    private static Set<String> initializeDefaultConfig(String coreSettings) 
        throws SzException 
    {
        logInfo("Setting up default config...");
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
            config.registerDataSource("UNUSED");

            // Set as default configuration
            configManager.setDefaultConfig(config.export());

            // get the data sources
            Set<String> dataSources = new LinkedHashSet<>();
            String      registry    = config.getDataSourceRegistry();
            JsonObject  jsonObject  = parseJsonObject(registry);
            JsonArray   jsonArray   = getJsonArray(jsonObject, "DATA_SOURCES");
            for (JsonObject jsonObj : jsonArray.getValuesAs(JsonObject.class)) {
                String dataSource = getString(jsonObj, "DSRC_CODE");
                dataSources.add(dataSource);
            }

            // return the set of data sources
            return dataSources;

        } finally {
            if (env != null) {
                env.destroy();
            }
            logInfo("Set up default config.");
        }
    }

    /**
     * Loads the truth set data into the repository and replicates to the 
     * data mart with the specified JDBC URL.
     * 
     * @param coreSettings The core settings JSON.
     *
     * @param dataMartUri The data mart {@link ConnectionUri}.
     * 
     * @return The {@link Set} of {@link SzRecordKey} instances identifying
     *         all records that were loaded.
     *      
     * @throws SzException If a Senzing error occurs.
     */
    private static Set<SzRecordKey> loadTruthSet(String         coreSettings,
                                                 ConnectionUri  dataMartUri)
        throws Exception
    {
        logInfo("Loading truth set...");
        
        Class<DataMartTestExtension> c = DataMartTestExtension.class;

        SzEnvironment       env         = null;
        SzReplicatorOptions options     = new SzReplicatorOptions();
        SzReplicator        replicator  = null;
        Set<SzRecordKey>    recordKeys  = new LinkedHashSet<>();

        try (InputStream is = c.getResourceAsStream(TRUTH_SET_RESOURCE_NAME);
             InputStreamReader isr = new InputStreamReader(is, UTF_8);
             BufferedReader br = new BufferedReader(isr))
        {    
            // build the SzEnvironment
            env = SzAutoCoreEnvironment.newAutoBuilder()
                .instanceName("DataMartTestLoad")
                .settings(coreSettings)
                .concurrency(4)
                .verboseLogging(false)
                .build();

            // get the engine
            SzEngine engine = env.getEngine();

            // setup the replicator options
            options.setUsingDatabaseQueue(true);
            options.setProcessingRate(ProcessingRate.AGGRESSIVE);
            options.setDatabaseUri(dataMartUri);
            replicator = new SzReplicator(env, options, true);
            
            for (String record = br.readLine(); record != null; record = br.readLine()) 
            {
                // trim whitespace
                record = record.trim();

                // skip any blank lines
                if (record.length() == 0) {
                    continue;
                }
                // skip commented-out lines
                if (record.startsWith("#")) {
                    continue;
                }

                // parse the record as JSON
                JsonObject  jsonObj     = parseJsonObject(record);

                // get the data source and record ID
                String      dataSource  = getString(jsonObj, "DATA_SOURCE");
                String      recordId    = getString(jsonObj, "RECORD_ID");
                SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);

                recordKeys.add(recordKey);
                String info = engine.addRecord(recordKey, record, SZ_WITH_INFO_FLAGS);
                replicator.getDatabaseMessageQueue().enqueueMessage(info);
            }

            // return the record keys
            return recordKeys;

        } finally {
            logInfo("Loaded truth set.");
            if (replicator != null) {
                logInfo("Waiting for replicator completion...");
                if (!replicator.waitUntilIdle(2000L, 20000L)) {
                    replicator.shutdown(); // force shutdown here
                    if (env != null) {
                        env.destroy();
                    }
                    throw new IllegalStateException(
                        "Replicator failed to become idle after 20 seconds, aborting....");
                }

                logInfo("Replicator completed, shutting it down...");
                // shutdown the replicator
                replicator.shutdown();

                logInfo("Replicator shut down.");
            }
            if (env != null) {
                env.destroy();
            }
        }
    }


    /**
     * Retrieves all entities for the records identified in the specified
     * {@link Set} of {@link SzRecordKey} instances.
     * 
     * @param coreSettings The core settings JSON.
     *
     * @param recordKeys The {@link Set} of {@link SzRecordKey} instances
     *                   identifying the records for which to retrieve 
     *                   the entities.
     * 
     * @return The {@link Map} of {@link Long} entity ID keys to
     *         {@link SzResolvedEntity} instances describing the entities.
     * 
     * @throws SzException If a Senzing error occurs.
     */
    private static SortedMap<Long, SzResolvedEntity> getEntities(
            String           coreSettings,
            Set<SzRecordKey> recordKeys)
        throws Exception
    {
        logInfo("Retriving entities...");
        SzEnvironment env = null;

        try {    
            // build the SzEnvironment
            env = SzCoreEnvironment.newBuilder()
                .instanceName("DataMartTestLoad")
                .settings(coreSettings)
                .verboseLogging(false)
                .build();

            // get the engine
            SzEngine engine = env.getEngine();

            // create the result map
            SortedMap<Long, SzResolvedEntity> entities = new TreeMap<>();

            // track the records we have already seen
            Set<SzRecordKey> foundRecords = new HashSet<>();

            // loop through the record keys
            for (SzRecordKey recordKey : recordKeys) {
                // skip this record key if already found
                if (foundRecords.contains(recordKey)) {
                    continue;
                }

                // get the entity
                String entityJson = engine.getEntity(recordKey, ENTITY_FLAGS);

                // parse the entity
                SzResolvedEntity entity = SzResolvedEntity.parse(entityJson);

                // track the entity
                entities.put(entity.getEntityId(), entity);

                // track the records already found
                Map<com.senzing.datamart.model.SzRecordKey, SzRecord> records = entity.getRecords();
                for (com.senzing.datamart.model.SzRecordKey martKey : records.keySet()) {
                    foundRecords.add(martKey.toKey());
                }
            }

            // return the record keys
            return entities;

        } finally {
            if (env != null) {
                env.destroy();
            }
            logInfo("Retrieved entities.");
        }
    }
}
