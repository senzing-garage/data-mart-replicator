package com.senzing.datamart;

import com.senzing.datamart.handlers.RefreshEntityHandler;
import com.senzing.datamart.model.SzRecord;
import com.senzing.datamart.model.SzRelatedEntity;
import com.senzing.datamart.model.SzResolvedEntity;
import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEngine;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sdk.SzFlag;
import com.senzing.sdk.SzNotFoundException;
import com.senzing.sdk.SzRecordKey;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.core.auto.SzAutoCoreEnvironment;
import com.senzing.sql.Connector;
import com.senzing.sql.DatabaseType;
import com.senzing.sql.PostgreSqlConnector;
import com.senzing.sql.SQLiteConnector;
import com.senzing.text.TextUtilities;
import com.senzing.util.OperatingSystemFamily;
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
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

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
     * System property to randomize/shuffle the order of loading, deleting
     * and updating records.  Its value is <code>{@value}</code>.
     */
    public static final String TEST_SHUFFLE_SYSTEM_PROPERTY 
        = "com.senzing.datamart.test.shuffle";

    /**
     * Boolean flag indicating if test databases should be preserved if possible.
     */
    private static final boolean PRESERVE_TEST_DATABASES
        = Boolean.TRUE.toString().equalsIgnoreCase(
            System.getProperty(TEST_PRESERVE_SYSTEM_PROPERTY));

    /**
     * Boolean flag indicating if test records should be shuffled.
     */
    private static final boolean SHUFFLE_TEST_RECORDS
        = (!Boolean.FALSE.toString().equalsIgnoreCase(
            System.getProperty(TEST_SHUFFLE_SYSTEM_PROPERTY,
                               Boolean.FALSE.toString())));    
    /**
     * The random number generator to use for shuffling.
     */
    private static final Random SHUFFLE_PRNG;
    
    static {
        String shuffleProp = System.getProperty(
            TEST_SHUFFLE_SYSTEM_PROPERTY, 
            Boolean.FALSE.toString()).toLowerCase();

        Boolean shuffleBool = null;
        if (Boolean.TRUE.toString().equalsIgnoreCase(shuffleProp)) {
            shuffleBool = Boolean.TRUE;
        }
        if (Boolean.FALSE.toString().equalsIgnoreCase(shuffleProp)) {
            shuffleBool = Boolean.FALSE;
        }

        String seedText = (shuffleBool == null)
            ? System.getProperty(TEST_SHUFFLE_SYSTEM_PROPERTY)
            : String.valueOf(System.currentTimeMillis());
        
        long seed;
        try {
            seed = Long.parseLong(seedText);
        } catch (Exception e) {
            seed = (long) seedText.hashCode();
        }
        
        SHUFFLE_PRNG = new Random(seed);
        if (SHUFFLE_TEST_RECORDS) {
            logInfo("INITIALIZED RANDOM SEED: " + seed);
        }
    }

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
         * @param dataMartConnUri   The connection URI for the data mart database.
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
                                if (file.exists() && (!PRESERVE_TEST_DATABASES))
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

            // Preserve the PostgreSQL database if requested
            if (PRESERVE_TEST_DATABASES) {
                try {
                    File dumpFile = File.createTempFile("senzing_pg_test_", ".sql");

                    dumpPostgreSqlDatabase(jdbcUrl, dumpFile);

                    System.err.println();
                    System.err.println("- - - - - - - - - - - - - - - - - - ");
                    System.err.println("POSTGRESQL DUMP: " + dumpFile.getAbsolutePath());
                    System.err.println("Restore with: psql -d dbname < " + dumpFile.getAbsolutePath());
                    System.err.println();
                } catch (Exception e) {
                    logError("Failed to dump PostgreSQL database: " + e.getMessage());
                    // Don't fail the test setup, just log the error
                }
            }

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

        if (!PRESERVE_TEST_DATABASES) {
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

            // On Windows, wait for file lock to be released before native SDK opens the file
            if (OperatingSystemFamily.RUNTIME_OS_FAMILY.isWindows()) {
                Thread.sleep(3000);
            }

            // Build the core settings JSON using SzUtilities
            String coreSettings = SzUtilities.basicSettingsFromDatabaseUri(
                connectionUri.toString().replace(
                    "sqlite3://", "sqlite3://na:na@"));

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
            if (!PRESERVE_TEST_DATABASES) {
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
     * Dumps the PostgreSQL database to a SQL file for preservation/analysis.
     * Uses JDBC to generate SQL statements, avoiding dependency on pg_dump binary.
     *
     * @param jdbcUrl The JDBC URL for the database.
     * @param dumpFile The {@link File} where the dump should be written.
     *
     * @throws Exception If the dump fails.
     */
    private static void dumpPostgreSqlDatabase(String jdbcUrl, File dumpFile)
        throws Exception
    {
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             java.io.FileWriter writer = new java.io.FileWriter(dumpFile))
        {
            // Write header
            writer.write("-- PostgreSQL Database Dump\n");
            writer.write("-- Generated: " + new java.util.Date() + "\n\n");

            // Get list of tables (excluding system tables)
            java.sql.ResultSet tables = conn.getMetaData().getTables(
                null, "public", "%", new String[]{"TABLE"});

            java.util.List<String> tableNames = new java.util.ArrayList<>();
            while (tables.next()) {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
            tables.close();

            // Get list of views
            java.sql.ResultSet views = conn.getMetaData().getTables(
                null, "public", "%", new String[]{"VIEW"});

            java.util.List<String> viewNames = new java.util.ArrayList<>();
            while (views.next()) {
                viewNames.add(views.getString("TABLE_NAME"));
            }
            views.close();

            // Generate DDL and DML for each table
            for (String tableName : tableNames) {
                writer.write("\n-- Table: " + tableName + "\n");

                // Generate CREATE TABLE statement
                writer.write("CREATE TABLE " + tableName + " (\n");

                java.sql.ResultSet columns = conn.getMetaData().getColumns(
                    null, "public", tableName, "%");

                boolean firstColumn = true;
                while (columns.next()) {
                    if (!firstColumn) {
                        writer.write(",\n");
                    }
                    firstColumn = false;

                    String columnName = columns.getString("COLUMN_NAME");
                    String columnType = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");
                    String isNullable = columns.getString("IS_NULLABLE");

                    writer.write("    " + columnName + " " + columnType);

                    // Add size for variable length types
                    if (columnType.equalsIgnoreCase("VARCHAR") ||
                        columnType.equalsIgnoreCase("CHAR")) {
                        writer.write("(" + columnSize + ")");
                    }

                    if ("NO".equals(isNullable)) {
                        writer.write(" NOT NULL");
                    }
                }
                columns.close();

                // Add primary key constraint
                java.sql.ResultSet primaryKeys = conn.getMetaData().getPrimaryKeys(
                    null, "public", tableName);
                java.util.List<String> pkColumns = new java.util.ArrayList<>();
                while (primaryKeys.next()) {
                    pkColumns.add(primaryKeys.getString("COLUMN_NAME"));
                }
                primaryKeys.close();

                if (!pkColumns.isEmpty()) {
                    writer.write(",\n    PRIMARY KEY (");
                    writer.write(String.join(", ", pkColumns));
                    writer.write(")");
                }

                writer.write("\n);\n\n");

                // Generate INSERT statements for table data
                try (java.sql.ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM " + tableName)) {

                    java.sql.ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    while (rs.next()) {
                        StringBuilder insert = new StringBuilder();
                        insert.append("INSERT INTO ").append(tableName).append(" VALUES (");

                        for (int i = 1; i <= columnCount; i++) {
                            if (i > 1) insert.append(", ");

                            Object value = rs.getObject(i);
                            if (value == null) {
                                insert.append("NULL");
                            } else if (value instanceof String) {
                                insert.append("'").append(value.toString().replace("'", "''")).append("'");
                            } else if (value instanceof java.sql.Timestamp || value instanceof java.sql.Date) {
                                insert.append("'").append(value).append("'");
                            } else {
                                insert.append(value);
                            }
                        }
                        insert.append(");\n");
                        writer.write(insert.toString());
                    }
                }

                writer.write("\n");
            }

            // Generate CREATE INDEX statements for all indexes (excluding primary keys)
            writer.write("\n-- Indexes\n");
            for (String tableName : tableNames) {
                java.sql.ResultSet indexes = conn.getMetaData().getIndexInfo(
                    null, "public", tableName, false, false);

                java.util.Map<String, java.util.List<String>> indexMap = new java.util.LinkedHashMap<>();
                while (indexes.next()) {
                    String indexName = indexes.getString("INDEX_NAME");
                    String columnName = indexes.getString("COLUMN_NAME");

                    // Skip primary key indexes (they're already in CREATE TABLE)
                    if (indexName == null || indexName.endsWith("_pkey")) {
                        continue;
                    }

                    indexMap.computeIfAbsent(indexName, k -> new java.util.ArrayList<>()).add(columnName);
                }
                indexes.close();

                for (java.util.Map.Entry<String, java.util.List<String>> entry : indexMap.entrySet()) {
                    writer.write("CREATE INDEX " + entry.getKey() + " ON " + tableName + " (");
                    writer.write(String.join(", ", entry.getValue()));
                    writer.write(");\n");
                }
            }

            // Generate triggers
            writer.write("\n-- Triggers\n");
            for (String tableName : tableNames) {
                try (java.sql.ResultSet triggers = stmt.executeQuery(
                    "SELECT tgname, pg_get_triggerdef(oid) as triggerdef " +
                    "FROM pg_trigger " +
                    "WHERE tgrelid = '" + tableName + "'::regclass " +
                    "AND NOT tgisinternal")) {

                    while (triggers.next()) {
                        String triggerDef = triggers.getString("triggerdef");
                        writer.write(triggerDef + ";\n");
                    }
                } catch (Exception e) {
                    // Log but don't fail if trigger extraction fails
                    writer.write("-- Failed to export triggers for table " + tableName + ": " + e.getMessage() + "\n");
                }
            }

            // Generate views
            writer.write("\n-- Views\n");
            for (String viewName : viewNames) {
                try (java.sql.ResultSet viewDef = stmt.executeQuery(
                    "SELECT pg_get_viewdef('" + viewName + "'::regclass, true) as definition")) {

                    if (viewDef.next()) {
                        String definition = viewDef.getString("definition");
                        writer.write("\nCREATE VIEW " + viewName + " AS\n");
                        writer.write(definition + ";\n");
                    }
                } catch (Exception e) {
                    // Log but don't fail if view extraction fails
                    writer.write("-- Failed to export view " + viewName + ": " + e.getMessage() + "\n");
                }
            }

            writer.flush();
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

        try (FileReader     rdr     = new FileReader(schemaFile, UTF_8);
             BufferedReader br      = new BufferedReader(rdr);
             Connection     conn    = DriverManager.getConnection(jdbcUrl);
             Statement      stmt    = conn.createStatement())
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
        
        SzEnvironment       env         = null;
        SzReplicatorOptions options     = new SzReplicatorOptions();
        SzReplicator        replicator  = null;
        Set<SzRecordKey>    recordKeys  = new LinkedHashSet<>();

        try {
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
            
            // load, delete and reload to exercise the data mart fully
            List<JsonObject> records = readRecords();
            loadRecords(recordKeys, records, engine, replicator);
            processRedos(engine, replicator);
            deleteRecords(recordKeys, records, engine, replicator);
            processRedos(engine, replicator);
            loadRecords(recordKeys, records, engine, replicator);
            processRedos(engine, replicator);
            
            // return the record keys
            return recordKeys;

        } finally {
            logInfo("Loaded truth set.");
            if (replicator != null) {
                logInfo("Waiting for replicator completion...");
                if (!replicator.waitUntilIdle(2000L, 300000L)) {
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
     * Reads the records from the truth set file and returns them as a
     * {@link List} of {@link JsonObject} elements.
     * 
     * @return The {@link List} of {@link JsonObject} records to load.
     * 
     * @throws IOException If an I/O failure occurs.
     */
    protected static List<JsonObject> readRecords() throws IOException {
        Class<DataMartTestExtension> c = DataMartTestExtension.class;

        try (InputStream is = c.getResourceAsStream(TRUTH_SET_RESOURCE_NAME)) {
            return readRecords(is);
        }
    }
    
    /**
     * Reads the records from the truth set file and returns them as a
     * {@link List} of {@link JsonObject} elements.
     * 
     * @param is The {@link InputStream} from which to read the records.
     * 
     * @return The {@link List} of {@link JsonObject} records to load.
     * 
     * @throws IOException If an I/O failure occurs.
     */
    protected static List<JsonObject> readRecords(InputStream is) 
        throws IOException 
    {
        List<JsonObject> result = new ArrayList<>(500);

        try (InputStreamReader isr = new InputStreamReader(is, UTF_8);
             BufferedReader br = new BufferedReader(isr))
        {
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
                JsonObject jsonObj = parseJsonObject(record);

                // add the json object to the list
                result.add(jsonObj);
            }
        }

        // return the result
        return result;
    }

    /**
     * Loads the records from the truth set and adds the record keys
     * from the specified {@link Set}.
     * 
     * @param recordKeys The {@link Set} of {@link SzRecordKey} that
     *                   tracks loaded records.
     * @param records The {@link List} of {@link JsonObject} records.
     * @param engine The {@link SzEngine} to use.
     * @param replicator The {@link SzReplicator} to use for posting the INFO.
     * 
     * @throws SzException If a Senzing failure occurs.
     * @throws SQLException If a JDBC failure occurs.
     */
    protected static void loadRecords(Set<SzRecordKey> recordKeys,
                                      List<JsonObject> records,
                                      SzEngine         engine,
                                      SzReplicator     replicator) 
        throws SzException, SQLException
    {
        if (SHUFFLE_TEST_RECORDS) {
            logInfo("Shuffling test record load order");
            Collections.shuffle(records, SHUFFLE_PRNG);
        }

        Map<SzRecordKey, JsonObject> fixTweaksMap = new LinkedHashMap<>();
        for (JsonObject jsonObj : records) {
            // get the data source and record ID
            String      dataSource  = getString(jsonObj, "DATA_SOURCE");
            String      recordId    = getString(jsonObj, "RECORD_ID");
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);

            JsonObject tweaked = tweakRecord(jsonObj);
            if (!tweaked.equals(jsonObj)) {
                fixTweaksMap.put(recordKey, jsonObj);
                jsonObj = tweaked;
            }

            String record = toJsonText(jsonObj);

            recordKeys.add(recordKey);
            String info = engine.addRecord(recordKey, record, SZ_WITH_INFO_FLAGS);
            replicator.getDatabaseMessageQueue().enqueueMessage(info);
        }

        // loop through the previously tweaked records and fix them
        for (Map.Entry<SzRecordKey, JsonObject> entry : fixTweaksMap.entrySet()) {
            SzRecordKey recordKey = entry.getKey();
            JsonObject jsonObj = entry.getValue();
            String record = toJsonText(jsonObj);
            String info = engine.addRecord(recordKey, record, SZ_WITH_INFO_FLAGS);
            replicator.getDatabaseMessageQueue().enqueueMessage(info);
        }
    }

    /**
     * Processes the pending redo records for the specified {@link SzEngine}
     * and sends the INFO messages to the specified {@link Replicator}.
     * 
     * @param engine The {@link SzEngine} to use.
     * @param replicator The {@link SzReplicator} to use for posting the INFO.
     * 
     * @throws SzException If a Senzing failure occurs.
     * @throws SQLException If a JDBC failure occurs.
     */
    protected static void processRedos(SzEngine engine, SzReplicator replicator) 
        throws SQLException, SzException
    {
        for (String redo = engine.getRedoRecord(); 
                redo != null; 
                redo = engine.getRedoRecord()) 
        {
            String info = engine.processRedoRecord(redo, SZ_WITH_INFO_FLAGS);
            replicator.getDatabaseMessageQueue().enqueueMessage(info);
        }
    }

    /**
     * Private map used for tweaking record values.
     */
    private static final Map<String, Integer> TWEAK_MAP = Map.of(
        "REL_POINTER_ROLE", 0,
        "DRIVERS_LICENSE_NUMBER", 11,
        "SSN_NUMBER", 9,
        "PHONE_NUMBER", 10);
    
    /**
     * Conditionally tweaks the record so that it can be loaded once one way
     * and then later updated with its correct data.  This will exercise the
     * data mart replication code that handles changed relationships and changed
     * match keys.
     * 
     * @param record The record to conditionally tweaked.
     * @return The tweaked record or the specified record if the record was not
     *         tweaked.
     */
    protected static JsonObject tweakRecord(JsonObject record) {
        boolean tweak = false;
        for (String key : TWEAK_MAP.keySet()) {

            if (record.containsKey(key)) {
                tweak = true;
                break;
            }
        }
        if (!tweak) {
            return record;
        }

        int changeIndex = 0;

        // parse the record
        JsonObjectBuilder job = Json.createObjectBuilder(record);

        for (Map.Entry<String,Integer> entry : TWEAK_MAP.entrySet()) {
            String key = entry.getKey();
            int length = entry.getValue();

            if (record.containsKey(key)) {
                if (length == 0) {
                    String value = ((changeIndex++)%2 == 0) ? "FOO" : "BAR";
                    job.add(key, value);
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (int index = 0; index < length; index++) {
                        sb.append("" + (1 + ((changeIndex++)/3)));
                    }
                    job.add(key, sb.toString());
                }
            }
        }

        return job.build();
    }

    /**
     * Deletes the records from the truth set and removes the record keys
     * from the specified {@link Set}.
     * 
     * @param recordKeys The {@link Set} of {@link SzRecordKey} that
     *                   tracks loaded records.
     * @param records The {@link List} of {@link JsonObject} instances
     *                describing the records.
     * @param engine The {@link SzEngine} to use.
     * @param replicator The {@link SzReplicator} to use for posting the INFO.
     * 
     * @throws IOException If an I/O failure occurs.
     * @throws SzException If a Senzing failure occurs.
     * @throws SQLException If a JDBC failure occurs.
     */
    protected static void deleteRecords(Set<SzRecordKey>    recordKeys,
                                        List<JsonObject>    records,
                                        SzEngine            engine,
                                        SzReplicator        replicator) 
        throws IOException, SzException, SQLException
    {
        if (SHUFFLE_TEST_RECORDS) {
            logInfo("Shuffling test record deletion order");
            Collections.shuffle(records, SHUFFLE_PRNG);
        }

        for (JsonObject jsonObj : records) {
            // get the data source and record ID
            String      dataSource  = getString(jsonObj, "DATA_SOURCE");
            String      recordId    = getString(jsonObj, "RECORD_ID");
            SzRecordKey recordKey   = SzRecordKey.of(dataSource, recordId);

            recordKeys.remove(recordKey);
            String info = engine.deleteRecord(recordKey, SZ_WITH_INFO_FLAGS);
            replicator.getDatabaseMessageQueue().enqueueMessage(info);
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
        logInfo("Retrieving entities...");
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
