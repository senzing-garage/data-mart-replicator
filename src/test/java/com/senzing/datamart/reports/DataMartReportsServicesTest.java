package com.senzing.datamart.reports;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.Repository;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.sdk.SzEnvironment;
import com.senzing.sdk.SzEnvironmentDestroyedException;
import com.senzing.sdk.core.SzCoreEnvironment;
import com.senzing.sdk.SzException;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

/**
 * Test class for {@link DataMartReportsServices}.
 * Tests all constructors, getter methods, and service methods.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(DataMartTestExtension.class)
public class DataMartReportsServicesTest extends AbstractReportsTest {

    /**
     * The {@link RepositoryType} to use for testing.
     */
    private RepositoryType repositoryType;

    /**
     * The {@link Repository} for testing.
     */
    private Repository repository;

    /**
     * The {@link SzCoreEnvironment} for testing.
     */
    private SzCoreEnvironment environment;

    /**
     * The {@link ConnectionProvider} for testing.
     */
    private ConnectionProvider connectionProvider;

    /**
     * BeforeAll setup - initializes the SzCoreEnvironment and ConnectionProvider.
     */
    @BeforeAll
    public void setupTest() throws Exception {
        // Call parent setup first
        super.setup();

        // Get the first repository type
        RepositoryType[] repoTypes = RepositoryType.values();
        this.repositoryType = repoTypes[0];

        // Get the repository for this type
        this.repository = DataMartTestExtension.getRepository(this.repositoryType);

        // Get the core settings from the repository
        String coreSettings = this.repository.getCoreSettings();

        // Initialize the SzCoreEnvironment
        this.environment = SzCoreEnvironment.newBuilder()
            .instanceName("DataMartReportsServicesTest")
            .settings(coreSettings)
            .verboseLogging(false)
            .build();

        // Get the connection provider from the base class
        this.connectionProvider = this.getConnectionProvider(this.repositoryType);
    }

    /**
     * AfterAll teardown - destroys the SzCoreEnvironment.
     * Note: The environment might already be destroyed by
     * testWithDestroyedEnvironment if it ran.
     */
    @AfterAll
    public void teardownTest() throws Exception {
        // Destroy the environment if it exists and is not already destroyed
        if (this.environment != null && !this.environment.isDestroyed()) {
            this.environment.destroy();
            this.environment = null;
        }

        // Call parent teardown
        super.teardown();
    }

    /**
     * Test the default constructor (no arguments).
     * Verifies that both environment and connection provider are null.
     */
    @Test
    @Order(100)
    void testDefaultConstructor() {
        DataMartReportsServices service = new DataMartReportsServices();

        assertNotNull(service, "Service should not be null");
        assertNull(service.getSzEnvironment(),
            "Environment should be null with default constructor");
        assertNull(service.getConnectionProvider(),
            "ConnectionProvider should be null with default constructor");
    }

    /**
     * Test constructor with valid SzEnvironment and ConnectionProvider.
     * Verifies both are properly stored and accessible via getters.
     */
    @Test
    @Order(200)
    void testConstructorWithValidParameters() {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        assertNotNull(service, "Service should not be null");
        assertSame(this.environment, service.getSzEnvironment(),
            "Environment should match");
        assertSame(this.connectionProvider, service.getConnectionProvider(),
            "ConnectionProvider should match");
    }

    /**
     * Test constructor with valid SzEnvironment but null ConnectionProvider.
     * Expects NullPointerException.
     */
    @Test
    @Order(300)
    void testConstructorWithNullConnectionProvider() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new DataMartReportsServices(this.environment, null),
            "Should throw NullPointerException for null ConnectionProvider"
        );

        assertTrue(exception.getMessage().contains("connection provider"),
            "Exception message should mention connection provider");
    }

    /**
     * Test constructor with null SzEnvironment but valid ConnectionProvider.
     * Expects NullPointerException.
     */
    @Test
    @Order(400)
    void testConstructorWithNullEnvironment() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new DataMartReportsServices(null, this.connectionProvider),
            "Should throw NullPointerException for null SzEnvironment"
        );

        assertTrue(exception.getMessage().contains("SzEnvironment"),
            "Exception message should mention SzEnvironment");
    }

    /**
     * Test constructor with both null parameters.
     * Expects NullPointerException (environment is checked first).
     */
    @Test
    @Order(500)
    void testConstructorWithBothNull() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new DataMartReportsServices(null, null),
            "Should throw NullPointerException for null parameters"
        );

        // The environment is checked first based on the constructor code
        assertTrue(exception.getMessage().contains("SzEnvironment"),
            "Exception message should mention SzEnvironment");
    }

    /**
     * Test getTemplateDefaultDataSources() method.
     * Verifies it returns TEST and SEARCH data sources only.
     * Calls twice to verify referential equality (caching).
     * Verifies the returned Set is unmodifiable.
     */
    @Test
    @Order(600)
    void testGetTemplateDefaultDataSources() throws SzException {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        // First call
        Set<String> dataSources1 = service.getTemplateDefaultDataSources();

        assertNotNull(dataSources1, "Data sources should not be null");
        assertEquals(2, dataSources1.size(),
            "Should have exactly 2 template default data sources");
        assertTrue(dataSources1.contains("TEST"),
            "Should contain TEST data source");
        assertTrue(dataSources1.contains("SEARCH"),
            "Should contain SEARCH data source");

        // Second call - should return the same instance (referentially equal)
        Set<String> dataSources2 = service.getTemplateDefaultDataSources();
        assertSame(dataSources1, dataSources2,
            "Second call should return the same Set instance (cached)");

        // Verify the Set is unmodifiable
        assertThrows(UnsupportedOperationException.class,
            () -> dataSources1.add("NEW_SOURCE"),
            "Should not be able to add to the returned Set");

        assertThrows(UnsupportedOperationException.class,
            () -> dataSources1.remove("TEST"),
            "Should not be able to remove from the returned Set");

        assertThrows(UnsupportedOperationException.class,
            () -> dataSources1.clear(),
            "Should not be able to clear the returned Set");
    }

    /**
     * Test getConfiguredDataSources() with excludeDefault=false.
     * Verifies all configured data sources are returned including TEST and SEARCH.
     */
    @Test
    @Order(700)
    void testGetConfiguredDataSourcesIncludingDefaults() throws SzException {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        Set<String> configuredSources = service.getConfiguredDataSources(false);
        Set<String> expectedSources = this.repository.getConfiguredDataSources();

        assertNotNull(configuredSources, "Configured data sources should not be null");
        assertEquals(expectedSources.size(), configuredSources.size(),
            "Should return all configured data sources");

        // Verify all expected sources are present
        for (String expectedSource : expectedSources) {
            assertTrue(configuredSources.contains(expectedSource),
                "Should contain configured data source: " + expectedSource);
        }

        // Verify TEST and SEARCH are included (they are template defaults)
        assertTrue(configuredSources.contains("TEST"),
            "Should include TEST when excludeDefault=false");
        assertTrue(configuredSources.contains("SEARCH"),
            "Should include SEARCH when excludeDefault=false");
    }

    /**
     * Test getConfiguredDataSources() with excludeDefault=true.
     * Verifies TEST and SEARCH data sources are excluded.
     */
    @Test
    @Order(800)
    void testGetConfiguredDataSourcesExcludingDefaults() throws SzException {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        Set<String> configuredSources = service.getConfiguredDataSources(true);
        Set<String> allSources = this.repository.getConfiguredDataSources();

        assertNotNull(configuredSources, "Configured data sources should not be null");

        // Should have fewer sources when excluding defaults
        assertTrue(configuredSources.size() < allSources.size(),
            "Should have fewer data sources when excluding defaults");

        // Verify TEST and SEARCH are excluded
        assertFalse(configuredSources.contains("TEST"),
            "Should exclude TEST when excludeDefault=true");
        assertFalse(configuredSources.contains("SEARCH"),
            "Should exclude SEARCH when excludeDefault=true");

        // Verify all returned sources are in the configured sources
        for (String source : configuredSources) {
            assertTrue(allSources.contains(source),
                "Returned source should be in configured sources: " + source);
        }
    }

    /**
     * Test getConnection() with default constructor instance.
     * Expects SQLException because no ConnectionProvider is set.
     */
    @Test
    @Order(900)
    void testGetConnectionWithDefaultConstructor() {
        DataMartReportsServices service = new DataMartReportsServices();

        SQLException exception = assertThrows(
            SQLException.class,
            () -> service.getConnection(),
            "Should throw SQLException when no ConnectionProvider is set"
        );

        assertTrue(exception.getMessage().contains("ConnectionProvider"),
            "Exception message should mention ConnectionProvider");
    }

    /**
     * Test getConnection() with properly constructed instance.
     * Verifies a valid connection can be obtained and properly closed.
     */
    @Test
    @Order(1000)
    void testGetConnectionWithValidInstance() throws SQLException {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        Connection conn = null;
        try {
            conn = service.getConnection();

            assertNotNull(conn, "Connection should not be null");
            assertFalse(conn.isClosed(), "Connection should be open");

            // Verify it's a usable connection by getting metadata
            assertNotNull(conn.getMetaData(),
                "Should be able to get connection metadata");

        } finally {
            // Always close the connection
            SQLUtilities.close(conn);
        }
    }

    /**
     * Test behavior when SzEnvironment is destroyed.
     * This MUST be the last test to run (highest order number) because it
     * destroys the shared environment. Constructs a valid instance, destroys
     * the environment, then attempts to call getTemplateDefaultDataSources().
     * Expects SzException to be thrown.
     *
     * Note: SzCoreEnvironment only allows one active instance at a time,
     * so we use the shared environment and destroy it for this test.
     * Also note: getTemplateDefaultDataSources() caches its result, so we
     * must NOT call it before destroying the environment.
     */
    @Test
    @Order(1500)
    void testWithDestroyedEnvironment() throws Exception {
        // Create service with valid parameters
        // IMPORTANT: Do NOT call getTemplateDefaultDataSources() yet - it caches the result
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        // Destroy the environment immediately
        this.environment.destroy();

        // Now attempting to use the service should throw SzEnvironmentDestroyedException
        // since getTemplateDefaultDataSources() has never been called and
        // it needs to access the environment to initialize the cache
        SzEnvironmentDestroyedException exception = assertThrows(
            SzEnvironmentDestroyedException.class,
            () -> service.getTemplateDefaultDataSources(),
            "Should throw SzEnvironmentDestroyedException when environment is destroyed"
        );

        assertNotNull(exception.getMessage(),
            "Exception should have a message");
        assertTrue(exception.getMessage().contains("destroyed"),
            "Exception message should mention destroyed");

        // Also test that getConfiguredDataSources() fails with destroyed environment
        assertThrows(
            SzEnvironmentDestroyedException.class,
            () -> service.getConfiguredDataSources(false),
            "getConfiguredDataSources should also throw SzEnvironmentDestroyedException when environment is destroyed"
        );

        // Mark environment as null so AfterAll doesn't try to destroy it again
        this.environment = null;
    }

    /**
     * Test edge case: calling getTemplateDefaultDataSources() multiple times
     * after getConfiguredDataSources() to ensure caching works correctly.
     */
    @Test
    @Order(1300)
    void testTemplateCachingAfterConfiguredCall() throws SzException {
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);

        // Call getConfiguredDataSources first
        Set<String> configured = service.getConfiguredDataSources(true);
        assertNotNull(configured, "Configured sources should not be null");

        // Now call getTemplateDefaultDataSources multiple times
        Set<String> template1 = service.getTemplateDefaultDataSources();
        Set<String> template2 = service.getTemplateDefaultDataSources();
        Set<String> template3 = service.getTemplateDefaultDataSources();

        // All should be the same instance
        assertSame(template1, template2,
            "Template data sources should be cached");
        assertSame(template2, template3,
            "Template data sources should be cached");

        // Verify correct content
        assertEquals(2, template1.size(),
            "Template should have 2 data sources");
        assertTrue(template1.contains("TEST") && template1.contains("SEARCH"),
            "Template should contain TEST and SEARCH");
    }

    /**
     * Test that getSzEnvironment() and getConnectionProvider() are accessible.
     * This is primarily for coverage since we use them in other tests,
     * but verifies the protected accessors work correctly.
     */
    @Test
    @Order(1400)
    void testProtectedAccessors() {
        // Test with default constructor
        DataMartReportsServices defaultService = new DataMartReportsServices();
        assertNull(defaultService.getSzEnvironment(),
            "Default constructor should have null environment");
        assertNull(defaultService.getConnectionProvider(),
            "Default constructor should have null connection provider");

        // Test with parameterized constructor
        DataMartReportsServices service = new DataMartReportsServices(
            this.environment, this.connectionProvider);
        assertNotNull(service.getSzEnvironment(),
            "Should have non-null environment");
        assertNotNull(service.getConnectionProvider(),
            "Should have non-null connection provider");
        assertSame(this.environment, service.getSzEnvironment(),
            "Environment should match what was passed to constructor");
        assertSame(this.connectionProvider, service.getConnectionProvider(),
            "Connection provider should match what was passed to constructor");
    }
}
