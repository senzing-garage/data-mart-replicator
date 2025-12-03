package com.senzing.datamart.reports;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import javax.json.JsonArray;
import javax.json.JsonObject;

import com.senzing.sdk.SzConfig;
import com.senzing.sdk.SzConfigManager;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sql.ConnectionProvider;

import static com.senzing.util.JsonUtilities.*;

/**
 * Provides annotated services for the data mart reports.
 */
public class DataMartReportsServices 
    implements EntitySizeReportsService, 
               EntityRelationsReportsService,
               LoadedStatsReportsService,
               SummaryStatsReportsService
{
    /**
     * The {@link ConnectionProvider} for this instance.
     */
    private ConnectionProvider connProvider = null;

    /**
     * The {@link SzEnvironment} to use for obtaining 
     * information about the configuration and entity repository.
     */
    private SzEnvironment env = null;

    /**
     * The data sources to exclude because they are configured 
     * by default.
     */
    private Set<String> excludedSources = null;

    /**
     * A monitor object on which to synchronize.
     */
    private final Object monitor = new Object();

    /**
     * Default constructor for derived classes that may override
     * methods so that the {@link SzEnvironment} and 
     * {@link ConnectionProvider} are not needed at the time of
     * construction.
     */
    protected DataMartReportsServices() {
        this.connProvider = null;
        this.env = null;
    }

    /**
     * Constructs with the specified {@link SzEnvironment} and
     * {@link ConnectionProvider} for obtaining the JDBC {@link Connection}.
     * 
     * @param env The {@link SzEnvironment} to use.
     * @param connProvider THe {@link ConnectionProvider} to use.
     * 
     * @throws NullPointerException If either of the the specified parameters
     *                              is <code>null</code>.
     */
    public DataMartReportsServices(SzEnvironment        env, 
                                   ConnectionProvider   connProvider) 
    {
        Objects.requireNonNull(
            env, "The SzEnvironment cannot be null");
        Objects.requireNonNull(
            connProvider, "The connection provider cannot be null");
        this.connProvider = connProvider;
        this.env = env;
    }

    /**
     * Gets the {@link SzEnvironment} for this instance.
     * 
     * @return The {@link SzEnvironment} for this instance.
     */
    protected SzEnvironment getSzEnvironment() {
        return this.env;
    }

    /**
     * Gets the {@link ConnectionProvider} for this instance.
     * 
     * @return The {@link ConnectionProvider} for this instance.
     */
    protected ConnectionProvider getConnectionProvider() {
        return this.connProvider;
    }

    /**
     * Gets the <b>unmodifiable</b> {@link Set} of {@link String} data source codes
     * identifying the data sources that are configured by default with the Senzing
     * template configuration.
     * 
     * @return The <b>unmodifiable</b> {@link Set} of {@link String} data source
     *         codes identifying the data sources that are configured by default
     *         with the Senzing template configuration.
     * 
     * @throws SzException If a failure occurs.
     */
    protected Set<String> getTemplateDefaultDataSources() throws SzException {
        synchronized (this.monitor) {
            if (this.excludedSources != null) {
                return this.excludedSources;
            }

            SzEnvironment   env         = this.getSzEnvironment();
            SzConfigManager configMgr   = env.getConfigManager();
            SzConfig        config      = configMgr.createConfig();
            String          registry    = config.getDataSourceRegistry();

            Set<String> result = new TreeSet<>();
            JsonObject  jsonObj = parseJsonObject(registry);
            JsonArray   jsonArr = getJsonArray(jsonObj, "DATA_SOURCES");
            for (JsonObject obj : jsonArr.getValuesAs(JsonObject.class)) {
                String dataSourceCode = getString(obj, "DSRC_CODE");
                result.add(dataSourceCode);
            }
            this.excludedSources = Collections.unmodifiableSet(result);

            return this.excludedSources;
        } 
    }
    /**
     * Overridden to get the configured using the {@link SzEnvironment}
     * from {@link #getSzEnvironment()}.
     * 
     * {@inheritDoc}
     */
    @Override
    public Set<String> getConfiguredDataSources(boolean excludeDefault) 
        throws SzException 
    {
        SzEnvironment   env         = this.getSzEnvironment();
        long            configId    = env.getActiveConfigId();
        SzConfigManager configMgr   = env.getConfigManager();
        SzConfig        config      = configMgr.createConfig(configId);
        String          registry    = config.getDataSourceRegistry();

        Set<String> excluded = (excludeDefault) 
            ? this.getTemplateDefaultDataSources()
            : Collections.emptySet();

        Set<String> result = new TreeSet<>();
        JsonObject  jsonObj = parseJsonObject(registry);
        JsonArray   jsonArr = getJsonArray(jsonObj, "DATA_SOURCES");
        for (JsonObject obj : jsonArr.getValuesAs(JsonObject.class)) {
            String dataSourceCode = getString(obj, "DSRC_CODE");
            if (!excluded.contains(dataSourceCode)) {
                result.add(dataSourceCode);
            }
        }

        // return the data sources
        return result;
    }

    /**
     * Overridden to get the {@link Connection} using the 
     * {@link ConnectionProvider} from {@link #getConnectionProvider()}.
     * 
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        ConnectionProvider cp = this.getConnectionProvider();
        if (cp == null) {
            throw new SQLException("No ConnectionProvider has been set");
        }
        return cp.getConnection();
    }
}
