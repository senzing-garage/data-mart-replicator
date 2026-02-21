package com.senzing.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.cmdline.CommandLineOption;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static com.senzing.datamart.SzReplicatorConstants.*;
import static com.senzing.datamart.SzReplicatorOption.*;

/**
 * Comprehensive unit tests for {@link SzReplicatorOptions}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzReplicatorOptionsTest {

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    /**
     * Test default constructor initializes with expected default values.
     */
    @Test
    public void testDefaultConstructor() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Verify defaults
        assertEquals(DEFAULT_INSTANCE_NAME, options.getCoreInstanceName(),
            "Default instance name should match DEFAULT_INSTANCE_NAME");
        assertEquals(DEFAULT_CORE_CONCURRENCY, options.getCoreConcurrency(),
            "Default core concurrency should match DEFAULT_CORE_CONCURRENCY");
        assertEquals(DEFAULT_REFRESH_CONFIG_SECONDS, options.getRefreshConfigSeconds(),
            "Default refresh config seconds should match DEFAULT_REFRESH_CONFIG_SECONDS");
        assertEquals(0, options.getCoreLogLevel(),
            "Default core log level should be 0");
        assertEquals(ProcessingRate.STANDARD, options.getProcessingRate(),
            "Default processing rate should be STANDARD");
        assertFalse(options.isUsingDatabaseQueue(),
            "Default database queue usage should be false");
        assertNull(options.getCoreSettings(),
            "Default core settings should be null");
        assertNull(options.getCoreConfigurationId(),
            "Default core config ID should be null");
        assertNull(options.getDatabaseUri(),
            "Default database URI should be null");
        assertNull(options.getSQSInfoUri(),
            "Default SQS info URI should be null");
        assertNull(options.getRabbitMqUri(),
            "Default RabbitMQ URI should be null");
        assertNull(options.getRabbitMqInfoQueue(),
            "Default RabbitMQ queue should be null");
    }

    /**
     * Test constructor with JsonObject parameter.
     */
    @Test
    public void testJsonObjectConstructor() {
        JsonObject settings = Json.createObjectBuilder()
            .add("PIPELINE", Json.createObjectBuilder()
                .add("CONFIGPATH", "/etc/opt/senzing")
                .add("RESOURCEPATH", "/opt/senzing/g2/resources")
                .add("SUPPORTPATH", "/opt/senzing/data"))
            .add("SQL", Json.createObjectBuilder()
                .add("CONNECTION", "sqlite3://na:na@/var/opt/senzing/sqlite/G2C.db"))
            .build();

        SzReplicatorOptions options = new SzReplicatorOptions(settings);

        assertNotNull(options.getCoreSettings(),
            "Core settings should not be null after construction");
        assertEquals(settings, options.getCoreSettings(),
            "Core settings should match the provided JsonObject");
    }

    /**
     * Test constructor with null JsonObject throws NullPointerException.
     */
    @Test
    public void testJsonObjectConstructorNull() {
        assertThrows(NullPointerException.class, () -> {
            new SzReplicatorOptions((JsonObject) null);
        }, "Constructor should throw NullPointerException for null JsonObject");
    }

    /**
     * Test constructor with String parameter.
     */
    @Test
    public void testStringConstructor() {
        String settingsJson = "{\"PIPELINE\":{\"CONFIGPATH\":\"/etc/opt/senzing\"}}";

        SzReplicatorOptions options = new SzReplicatorOptions(settingsJson);

        assertNotNull(options.getCoreSettings(),
            "Core settings should not be null after construction");
    }

    // ========================================================================
    // Getter/Setter Tests
    // ========================================================================

    /**
     * Test getCoreSettings/setCoreSettings.
     */
    @Test
    public void testCoreSettingsGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        JsonObject settings = Json.createObjectBuilder()
            .add("TEST", "value")
            .build();

        SzReplicatorOptions result = options.setCoreSettings(settings);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(settings, options.getCoreSettings(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setCoreSettings(null);
        assertNull(options.getCoreSettings(),
            "Setting to null should work");
    }

    /**
     * Test getCoreConcurrency/setCoreConcurrency.
     */
    @Test
    public void testCoreConcurrencyGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Test setting a value
        SzReplicatorOptions result = options.setCoreConcurrency(16);
        assertSame(options, result, "Setter should return same instance");
        assertEquals(16, options.getCoreConcurrency(),
            "Getter should return the value that was set");

        // Test setting null resets to default
        options.setCoreConcurrency(null);
        assertEquals(DEFAULT_CORE_CONCURRENCY, options.getCoreConcurrency(),
            "Setting to null should reset to default");
    }

    /**
     * Test getCoreInstanceName/setCoreInstanceName.
     */
    @Test
    public void testCoreInstanceNameGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        String instanceName = "TestInstance";
        SzReplicatorOptions result = options.setCoreInstanceName(instanceName);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(instanceName, options.getCoreInstanceName(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setCoreInstanceName(null);
        assertNull(options.getCoreInstanceName(),
            "Setting to null should work");
    }

    /**
     * Test getCoreLogLevel/setCoreLogLevel.
     */
    @Test
    public void testCoreLogLevelGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SzReplicatorOptions result = options.setCoreLogLevel(1);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(1, options.getCoreLogLevel(),
            "Getter should return the value that was set");

        // Test setting to 0
        options.setCoreLogLevel(0);
        assertEquals(0, options.getCoreLogLevel(),
            "Setting to 0 should work");

        // Test negative values (edge case)
        options.setCoreLogLevel(-1);
        assertEquals(-1, options.getCoreLogLevel(),
            "Setting to negative value should work");
    }

    /**
     * Test getCoreConfigurationId/setCoreConfigurationId.
     */
    @Test
    public void testCoreConfigurationIdGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        Long configId = 12345L;
        SzReplicatorOptions result = options.setCoreConfigurationId(configId);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(configId, options.getCoreConfigurationId(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setCoreConfigurationId(null);
        assertNull(options.getCoreConfigurationId(),
            "Setting to null should work");
    }

    /**
     * Test getRefreshConfigSeconds/setRefreshConfigSeconds.
     */
    @Test
    public void testRefreshConfigSecondsGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SzReplicatorOptions result = options.setRefreshConfigSeconds(600L);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(600L, options.getRefreshConfigSeconds(),
            "Getter should return the value that was set");

        // Test setting null resets to default
        options.setRefreshConfigSeconds(null);
        assertEquals(DEFAULT_REFRESH_CONFIG_SECONDS, options.getRefreshConfigSeconds(),
            "Setting to null should reset to default");
    }

    /**
     * Test getProcessingRate/setProcessingRate.
     */
    @Test
    public void testProcessingRateGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SzReplicatorOptions result = options.setProcessingRate(ProcessingRate.LEISURELY);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(ProcessingRate.LEISURELY, options.getProcessingRate(),
            "Getter should return the value that was set");

        // Test setting to null resets to STANDARD default
        options.setProcessingRate(null);
        assertEquals(ProcessingRate.STANDARD, options.getProcessingRate(),
            "Setting to null should reset to STANDARD default");
    }

    /**
     * Test isUsingDatabaseQueue/setUsingDatabaseQueue.
     */
    @Test
    public void testUsingDatabaseQueueGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SzReplicatorOptions result = options.setUsingDatabaseQueue(true);

        assertSame(options, result, "Setter should return same instance");
        assertTrue(options.isUsingDatabaseQueue(),
            "Getter should return the value that was set");

        // Test setting back to false
        options.setUsingDatabaseQueue(false);
        assertFalse(options.isUsingDatabaseQueue(),
            "Setting to false should work");
    }

    /**
     * Test getDatabaseUri/setDatabaseUri.
     */
    @Test
    public void testDatabaseUriGetterSetter() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        ConnectionUri dbUri = new PostgreSqlUri("user", "password", "localhost", "G2");

        SzReplicatorOptions result = options.setDatabaseUri(dbUri);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(dbUri, options.getDatabaseUri(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setDatabaseUri(null);
        assertNull(options.getDatabaseUri(),
            "Setting to null should work");
    }

    /**
     * Test setDatabaseUri with unsupported URI type throws exception.
     */
    @Test
    public void testDatabaseUriUnsupportedType() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // RabbitMqUri is a ConnectionUri subclass but not a valid database URI
        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "pass", "localhost");

        // Should throw IllegalArgumentException for unsupported type
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            options.setDatabaseUri(rabbitUri);
        }, "Should throw IllegalArgumentException for unsupported database URI type");

        assertTrue(exception.getMessage().contains("Unsupported"),
            "Exception message should mention 'Unsupported'");
        assertTrue(exception.getMessage().contains("database"),
            "Exception message should mention 'database'");
    }

    /**
     * Test setDatabaseUri with SzCoreSettingsUri (now supported).
     */
    @Test
    public void testDatabaseUriWithCoreSettingsUri() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Create a SzCoreSettingsUri
        ConnectionUri coreSettingsUri = SzReplicatorOption.parseDatabaseUri(
            "sz://core-settings/SQL/CONNECTION");

        // Should succeed now that SzCoreSettingsUri is supported
        SzReplicatorOptions result = options.setDatabaseUri(coreSettingsUri);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(coreSettingsUri, options.getDatabaseUri(),
            "Should accept SzCoreSettingsUri");
        assertTrue(options.getDatabaseUri() instanceof SzCoreSettingsUri,
            "Stored URI should be SzCoreSettingsUri instance");
    }

    /**
     * Test getSQSInfoUri/setSQSInfoUri.
     */
    @Test
    public void testSQSInfoUriGetterSetter() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));

        SzReplicatorOptions result = options.setSQSInfoUri(sqsUri);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(sqsUri, options.getSQSInfoUri(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setSQSInfoUri(null);
        assertNull(options.getSQSInfoUri(),
            "Setting to null should work");
    }

    /**
     * Test getRabbitMqUri/setRabbitMqUri.
     */
    @Test
    public void testRabbitMqUriGetterSetter() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");

        SzReplicatorOptions result = options.setRabbitMqUri(rabbitUri);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(rabbitUri, options.getRabbitMqUri(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setRabbitMqUri(null);
        assertNull(options.getRabbitMqUri(),
            "Setting to null should work");
    }

    /**
     * Test getRabbitMqInfoQueue/setRabbitMqInfoQueue.
     */
    @Test
    public void testRabbitMqInfoQueueGetterSetter() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        String queueName = "senzing-info-queue";
        SzReplicatorOptions result = options.setRabbitMqInfoQueue(queueName);

        assertSame(options, result, "Setter should return same instance");
        assertEquals(queueName, options.getRabbitMqInfoQueue(),
            "Getter should return the value that was set");

        // Test setting to null
        options.setRabbitMqInfoQueue(null);
        assertNull(options.getRabbitMqInfoQueue(),
            "Setting to null should work");
    }

    // ========================================================================
    // Validation/Edge Case Tests
    // ========================================================================

    /**
     * Test that setUsingDatabaseQueue(true) throws exception when SQS is configured.
     */
    @Test
    public void testDatabaseQueueConflictWithSQS() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure SQS first
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));
        options.setSQSInfoUri(sqsUri);

        // Try to enable database queue - should throw
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setUsingDatabaseQueue(true);
        }, "Should throw IllegalStateException when database queue conflicts with SQS");

        assertTrue(exception.getMessage().contains("SQS"),
            "Exception message should mention SQS");
    }

    /**
     * Test that setUsingDatabaseQueue(true) throws exception when RabbitMQ URI is configured.
     */
    @Test
    public void testDatabaseQueueConflictWithRabbitMQUri() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure RabbitMQ URI first
        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");
        options.setRabbitMqUri(rabbitUri);

        // Try to enable database queue - should throw
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setUsingDatabaseQueue(true);
        }, "Should throw IllegalStateException when database queue conflicts with RabbitMQ URI");

        assertTrue(exception.getMessage().contains("RabbitMQ"),
            "Exception message should mention RabbitMQ");
    }

    /**
     * Test that setUsingDatabaseQueue(true) throws exception when RabbitMQ queue is configured.
     */
    @Test
    public void testDatabaseQueueConflictWithRabbitMQQueue() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure RabbitMQ queue first
        options.setRabbitMqInfoQueue("my-queue");

        // Try to enable database queue - should throw
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setUsingDatabaseQueue(true);
        }, "Should throw IllegalStateException when database queue conflicts with RabbitMQ queue");

        assertTrue(exception.getMessage().contains("RabbitMQ"),
            "Exception message should mention RabbitMQ");
    }

    /**
     * Test that setSQSInfoUri throws exception when database queue is enabled.
     */
    @Test
    public void testSQSConflictWithDatabaseQueue() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Enable database queue first
        options.setUsingDatabaseQueue(true);

        // Try to set SQS URI - should throw
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setSQSInfoUri(sqsUri);
        }, "Should throw IllegalStateException when SQS conflicts with database queue");

        assertTrue(exception.getMessage().contains("database"),
            "Exception message should mention database");
    }

    /**
     * Test that setSQSInfoUri throws exception when RabbitMQ URI is configured.
     */
    @Test
    public void testSQSConflictWithRabbitMQUri() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure RabbitMQ URI first
        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");
        options.setRabbitMqUri(rabbitUri);

        // Try to set SQS URI - should throw
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setSQSInfoUri(sqsUri);
        }, "Should throw IllegalStateException when SQS conflicts with RabbitMQ URI");

        assertTrue(exception.getMessage().contains("RabbitMQ"),
            "Exception message should mention RabbitMQ");
    }

    /**
     * Test that setSQSInfoUri throws exception when RabbitMQ queue is configured.
     */
    @Test
    public void testSQSConflictWithRabbitMQQueue() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure RabbitMQ queue first
        options.setRabbitMqInfoQueue("my-queue");

        // Try to set SQS URI - should throw
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setSQSInfoUri(sqsUri);
        }, "Should throw IllegalStateException when SQS conflicts with RabbitMQ queue");

        assertTrue(exception.getMessage().contains("RabbitMQ"),
            "Exception message should mention RabbitMQ");
    }

    /**
     * Test that setRabbitMqUri throws exception when database queue is enabled.
     */
    @Test
    public void testRabbitMQUriConflictWithDatabaseQueue() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Enable database queue first
        options.setUsingDatabaseQueue(true);

        // Try to set RabbitMQ URI - should throw
        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setRabbitMqUri(rabbitUri);
        }, "Should throw IllegalStateException when RabbitMQ URI conflicts with database queue");

        assertTrue(exception.getMessage().contains("database"),
            "Exception message should mention database");
    }

    /**
     * Test that setRabbitMqUri throws exception when SQS is configured.
     */
    @Test
    public void testRabbitMQUriConflictWithSQS() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure SQS first
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));
        options.setSQSInfoUri(sqsUri);

        // Try to set RabbitMQ URI - should throw
        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setRabbitMqUri(rabbitUri);
        }, "Should throw IllegalStateException when RabbitMQ URI conflicts with SQS");

        assertTrue(exception.getMessage().contains("SQS"),
            "Exception message should mention SQS");
    }

    /**
     * Test that setRabbitMqInfoQueue throws exception when database queue is enabled.
     */
    @Test
    public void testRabbitMQQueueConflictWithDatabaseQueue() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Enable database queue first
        options.setUsingDatabaseQueue(true);

        // Try to set RabbitMQ queue - should throw
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setRabbitMqInfoQueue("my-queue");
        }, "Should throw IllegalStateException when RabbitMQ queue conflicts with database queue");

        assertTrue(exception.getMessage().contains("database"),
            "Exception message should mention database");
    }

    /**
     * Test that setRabbitMqInfoQueue throws exception when SQS is configured.
     */
    @Test
    public void testRabbitMQQueueConflictWithSQS() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure SQS first
        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));
        options.setSQSInfoUri(sqsUri);

        // Try to set RabbitMQ queue - should throw
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            options.setRabbitMqInfoQueue("my-queue");
        }, "Should throw IllegalStateException when RabbitMQ queue conflicts with SQS");

        assertTrue(exception.getMessage().contains("SQS"),
            "Exception message should mention SQS");
    }

    /**
     * Test that setting null values doesn't trigger validation errors.
     */
    @Test
    public void testNullValuesDoNotTriggerValidation() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Configure database queue
        options.setUsingDatabaseQueue(true);

        // Setting null values should work even with conflicts
        assertDoesNotThrow(() -> {
            options.setSQSInfoUri(null);
            options.setRabbitMqUri(null);
            options.setRabbitMqInfoQueue(null);
        }, "Setting null should not trigger validation");

        // Similarly, setting false should work
        assertDoesNotThrow(() -> {
            options.setUsingDatabaseQueue(false);
        }, "Setting false should not trigger validation");
    }

    // ========================================================================
    // equals() and hashCode() Tests
    // ========================================================================

    /**
     * Test equals with same instance returns true.
     */
    @Test
    public void testEqualsSameInstance() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        assertTrue(options.equals(options),
            "equals with same instance should return true");
    }

    /**
     * Test equals with null returns false.
     */
    @Test
    public void testEqualsNull() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        assertFalse(options.equals(null),
            "equals with null should return false");
    }

    /**
     * Test equals with different class returns false.
     */
    @Test
    public void testEqualsDifferentClass() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        assertFalse(options.equals("not an SzReplicatorOptions"),
            "equals with different class should return false");
    }

    /**
     * Test equals with identical default instances returns true.
     */
    @Test
    public void testEqualsDefaultInstances() {
        SzReplicatorOptions options1 = new SzReplicatorOptions();
        SzReplicatorOptions options2 = new SzReplicatorOptions();

        assertTrue(options1.equals(options2),
            "Two default instances should be equal");
        assertEquals(options1.hashCode(), options2.hashCode(),
            "Equal objects should have equal hash codes");
    }

    /**
     * Test equals with different values returns false.
     */
    @Test
    public void testEqualsDifferentValues() {
        SzReplicatorOptions options1 = new SzReplicatorOptions();
        SzReplicatorOptions options2 = new SzReplicatorOptions();

        options1.setCoreConcurrency(16);
        options2.setCoreConcurrency(8);

        assertFalse(options1.equals(options2),
            "Instances with different values should not be equal");
    }

    /**
     * Test equals and hashCode with all properties set.
     */
    @Test
    public void testEqualsAndHashCodeAllProperties() throws Exception {
        SzReplicatorOptions options1 = new SzReplicatorOptions();
        SzReplicatorOptions options2 = new SzReplicatorOptions();

        // Set all properties to the same values
        JsonObject settings = Json.createObjectBuilder().add("TEST", "value").build();
        options1.setCoreSettings(settings);
        options2.setCoreSettings(settings);

        options1.setCoreConcurrency(16);
        options2.setCoreConcurrency(16);

        options1.setCoreInstanceName("TestInstance");
        options2.setCoreInstanceName("TestInstance");

        options1.setCoreLogLevel(1);
        options2.setCoreLogLevel(1);

        options1.setCoreConfigurationId(12345L);
        options2.setCoreConfigurationId(12345L);

        options1.setRefreshConfigSeconds(600L);
        options2.setRefreshConfigSeconds(600L);

        options1.setProcessingRate(ProcessingRate.LEISURELY);
        options2.setProcessingRate(ProcessingRate.LEISURELY);

        options1.setUsingDatabaseQueue(true);
        options2.setUsingDatabaseQueue(true);

        ConnectionUri dbUri = new PostgreSqlUri("user", "password", "localhost", "G2");
        options1.setDatabaseUri(dbUri);
        options2.setDatabaseUri(dbUri);

        assertTrue(options1.equals(options2),
            "Instances with identical properties should be equal");
        assertEquals(options1.hashCode(), options2.hashCode(),
            "Equal objects should have equal hash codes");
    }

    /**
     * Test equals with one null property.
     */
    @Test
    public void testEqualsWithNullProperty() {
        SzReplicatorOptions options1 = new SzReplicatorOptions();
        SzReplicatorOptions options2 = new SzReplicatorOptions();

        options1.setCoreInstanceName("Test");
        options2.setCoreInstanceName(null);

        assertFalse(options1.equals(options2),
            "Instances with different null properties should not be equal");
    }

    /**
     * Test hashCode consistency.
     */
    @Test
    public void testHashCodeConsistency() {
        SzReplicatorOptions options = new SzReplicatorOptions();
        options.setCoreConcurrency(16);

        int hash1 = options.hashCode();
        int hash2 = options.hashCode();

        assertEquals(hash1, hash2,
            "hashCode should be consistent across multiple calls");
    }

    // ========================================================================
    // buildJson() Tests
    // ========================================================================

    /**
     * Test buildJson with all properties set.
     */
    @Test
    public void testBuildJsonAllProperties() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Set all properties
        JsonObject settings = Json.createObjectBuilder()
            .add("TEST", "value")
            .build();
        options.setCoreSettings(settings);
        options.setCoreConcurrency(16);
        options.setCoreInstanceName("TestInstance");
        options.setCoreLogLevel(1);
        options.setCoreConfigurationId(12345L);
        options.setRefreshConfigSeconds(600L);
        options.setProcessingRate(ProcessingRate.LEISURELY);
        options.setUsingDatabaseQueue(true);

        ConnectionUri dbUri = new PostgreSqlUri("user", "password", "localhost", "G2");
        options.setDatabaseUri(dbUri);

        // Build JSON
        JsonObjectBuilder builder = options.buildJson(null);
        JsonObject json = builder.build();

        // Verify JSON contains expected properties
        assertNotNull(json, "buildJson should return non-null JsonObject");
        assertTrue(json.containsKey("coreSettings"),
            "JSON should contain coreSettings");
        assertTrue(json.containsKey("coreConcurrency"),
            "JSON should contain coreConcurrency");
        assertTrue(json.containsKey("coreInstanceName"),
            "JSON should contain coreInstanceName");
        assertTrue(json.containsKey("coreLogLevel"),
            "JSON should contain coreLogLevel");
        assertTrue(json.containsKey("coreConfigurationId"),
            "JSON should contain coreConfigurationId");
        assertTrue(json.containsKey("refreshConfigSeconds"),
            "JSON should contain refreshConfigSeconds");
        assertTrue(json.containsKey("processingRate"),
            "JSON should contain processingRate");
        assertTrue(json.containsKey("usingDatabaseQueue"),
            "JSON should contain usingDatabaseQueue");
        assertTrue(json.containsKey("databaseUri"),
            "JSON should contain databaseUri");
    }

    /**
     * Test buildJson with defaults (null properties should not appear).
     */
    @Test
    public void testBuildJsonDefaults() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Build JSON with defaults
        JsonObjectBuilder builder = options.buildJson(null);
        JsonObject json = builder.build();

        // Null properties should not be in JSON
        assertFalse(json.containsKey("coreSettings"),
            "JSON should not contain null coreSettings");
        assertFalse(json.containsKey("sqsInfoUri"),
            "JSON should not contain null sqsInfoUri");
        assertFalse(json.containsKey("rabbitMqUri"),
            "JSON should not contain null rabbitMqUri");

        // Non-null defaults should be present
        assertTrue(json.containsKey("coreConcurrency"),
            "JSON should contain non-null coreConcurrency");
        assertTrue(json.containsKey("coreInstanceName"),
            "JSON should contain non-null coreInstanceName");
    }

    /**
     * Test buildJson with provided JsonObjectBuilder.
     */
    @Test
    public void testBuildJsonWithProvidedBuilder() {
        SzReplicatorOptions options = new SzReplicatorOptions();
        options.setCoreLogLevel(2);

        JsonObjectBuilder existingBuilder = Json.createObjectBuilder()
            .add("existingProperty", "value");

        JsonObjectBuilder result = options.buildJson(existingBuilder);
        JsonObject json = result.build();

        assertSame(existingBuilder, result,
            "Should return the same builder that was passed in");
        assertTrue(json.containsKey("existingProperty"),
            "Should preserve existing properties");
        assertTrue(json.containsKey("coreLogLevel"),
            "Should add new properties");
    }

    /**
     * Test that buildJson does not throw exception.
     */
    @Test
    public void testBuildJsonNoException() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        assertDoesNotThrow(() -> {
            options.buildJson(null);
        }, "buildJson should not throw exception with defaults");

        // Set all properties
        options.setCoreConcurrency(8);
        options.setCoreInstanceName("Test");
        options.setCoreLogLevel(1);

        assertDoesNotThrow(() -> {
            options.buildJson(null);
        }, "buildJson should not throw exception with properties set");
    }

    // ========================================================================
    // parse() Tests
    // ========================================================================

    /**
     * Test parse(String) with valid JSON.
     */
    @Test
    public void testParseString() {
        String jsonText = "{\"coreConcurrency\":16,\"coreLogLevel\":1}";

        SzReplicatorOptions options = SzReplicatorOptions.parse(jsonText);

        assertNotNull(options, "Parsed options should not be null");
        assertEquals(16, options.getCoreConcurrency(),
            "Parsed coreConcurrency should match");
        assertEquals(1, options.getCoreLogLevel(),
            "Parsed coreLogLevel should match");
    }

    /**
     * Test parse(JsonObject) with valid JSON.
     */
    @Test
    public void testParseJsonObject() {
        JsonObject json = Json.createObjectBuilder()
            .add("coreConcurrency", 8)
            .add("coreInstanceName", "ParsedInstance")
            .add("usingDatabaseQueue", true)
            .build();

        SzReplicatorOptions options = SzReplicatorOptions.parse(json);

        assertNotNull(options, "Parsed options should not be null");
        assertEquals(8, options.getCoreConcurrency(),
            "Parsed coreConcurrency should match");
        assertEquals("ParsedInstance", options.getCoreInstanceName(),
            "Parsed coreInstanceName should match");
        assertTrue(options.isUsingDatabaseQueue(),
            "Parsed usingDatabaseQueue should match");
    }

    /**
     * Test round-trip: buildJson -> parse produces equivalent object.
     */
    @Test
    public void testRoundTripBuildJsonParse() throws Exception {
        SzReplicatorOptions original = new SzReplicatorOptions();

        // Set various properties (excluding enum/complex types that might not serialize)
        original.setCoreConcurrency(24);
        original.setCoreInstanceName("RoundTripTest");
        original.setCoreLogLevel(2);
        original.setCoreConfigurationId(99999L);
        original.setRefreshConfigSeconds(900L);
        original.setUsingDatabaseQueue(true);

        // Build JSON
        JsonObject json = original.buildJson(null).build();

        // Parse back
        SzReplicatorOptions parsed = SzReplicatorOptions.parse(json);

        // Verify individual properties match
        assertEquals(original.getCoreConcurrency(), parsed.getCoreConcurrency(),
            "coreConcurrency should match");
        assertEquals(original.getCoreInstanceName(), parsed.getCoreInstanceName(),
            "coreInstanceName should match");
        assertEquals(original.getCoreLogLevel(), parsed.getCoreLogLevel(),
            "coreLogLevel should match");
        assertEquals(original.getCoreConfigurationId(), parsed.getCoreConfigurationId(),
            "coreConfigurationId should match");
        assertEquals(original.getRefreshConfigSeconds(), parsed.getRefreshConfigSeconds(),
            "refreshConfigSeconds should match");
        assertEquals(original.isUsingDatabaseQueue(), parsed.isUsingDatabaseQueue(),
            "usingDatabaseQueue should match");

        // If all properties match, objects should be equal
        assertEquals(original, parsed,
            "Round-trip through JSON should produce equivalent object");
    }

    /**
     * Test round-trip with String parse.
     */
    @Test
    public void testRoundTripBuildJsonParseString() {
        SzReplicatorOptions original = new SzReplicatorOptions();
        original.setCoreConcurrency(12);
        original.setCoreLogLevel(1);

        // Build JSON and convert to string
        JsonObject json = original.buildJson(null).build();
        String jsonText = json.toString();

        // Parse from string
        SzReplicatorOptions parsed = SzReplicatorOptions.parse(jsonText);

        // Verify equivalence
        assertEquals(original, parsed,
            "Round-trip through JSON string should produce equivalent object");
    }

    /**
     * Test parse with missing properties uses defaults.
     */
    @Test
    public void testParseWithMissingProperties() {
        JsonObject json = Json.createObjectBuilder()
            .add("coreLogLevel", 1)
            // Missing other properties
            .build();

        SzReplicatorOptions options = SzReplicatorOptions.parse(json);

        assertNotNull(options, "Parsed options should not be null");
        assertEquals(1, options.getCoreLogLevel(),
            "Parsed coreLogLevel should match");
        assertEquals(DEFAULT_CORE_CONCURRENCY, options.getCoreConcurrency(),
            "Missing property should use default");
    }

    // ========================================================================
    // buildOptionsMap() Tests
    // ========================================================================

    /**
     * Test buildOptionsMap contains all expected options.
     */
    @Test
    public void testBuildOptionsMapAllOptions() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Set various properties
        JsonObject settings = Json.createObjectBuilder()
            .add("TEST", "value")
            .build();
        options.setCoreSettings(settings);
        options.setCoreConcurrency(16);
        options.setCoreInstanceName("MapTest");
        options.setCoreLogLevel(1);
        options.setCoreConfigurationId(777L);
        options.setRefreshConfigSeconds(1200L);
        options.setProcessingRate(ProcessingRate.LEISURELY);
        options.setUsingDatabaseQueue(true);

        ConnectionUri dbUri = new PostgreSqlUri("user", "password", "localhost", "G2");
        options.setDatabaseUri(dbUri);

        // Build options map
        Map<?, ?> optionsMap = options.buildOptionsMap();

        assertNotNull(optionsMap, "Options map should not be null");

        // Verify expected keys are present
        assertTrue(optionsMap.containsKey(CORE_SETTINGS),
            "Map should contain CORE_SETTINGS");
        assertTrue(optionsMap.containsKey(CORE_CONCURRENCY),
            "Map should contain CORE_CONCURRENCY");
        assertTrue(optionsMap.containsKey(CORE_INSTANCE_NAME),
            "Map should contain CORE_INSTANCE_NAME");
        assertTrue(optionsMap.containsKey(CORE_LOG_LEVEL),
            "Map should contain CORE_LOG_LEVEL");
        assertTrue(optionsMap.containsKey(CORE_CONFIG_ID),
            "Map should contain CORE_CONFIG_ID");
        assertTrue(optionsMap.containsKey(REFRESH_CONFIG_SECONDS),
            "Map should contain REFRESH_CONFIG_SECONDS");
        assertTrue(optionsMap.containsKey(PROCESSING_RATE),
            "Map should contain PROCESSING_RATE");
        assertTrue(optionsMap.containsKey(DATABASE_INFO_QUEUE),
            "Map should contain DATABASE_INFO_QUEUE");
        assertTrue(optionsMap.containsKey(DATABASE_URI),
            "Map should contain DATABASE_URI");

        // Verify values match
        assertEquals(settings, optionsMap.get(CORE_SETTINGS),
            "CORE_SETTINGS value should match");
        assertEquals(16, optionsMap.get(CORE_CONCURRENCY),
            "CORE_CONCURRENCY value should match");
        assertEquals("MapTest", optionsMap.get(CORE_INSTANCE_NAME),
            "CORE_INSTANCE_NAME value should match");
        assertEquals(1, optionsMap.get(CORE_LOG_LEVEL),
            "CORE_LOG_LEVEL value should match");
        assertEquals(777L, optionsMap.get(CORE_CONFIG_ID),
            "CORE_CONFIG_ID value should match");
        assertEquals(1200L, optionsMap.get(REFRESH_CONFIG_SECONDS),
            "REFRESH_CONFIG_SECONDS value should match");
        assertEquals(ProcessingRate.LEISURELY, optionsMap.get(PROCESSING_RATE),
            "PROCESSING_RATE value should match");
        assertEquals(true, optionsMap.get(DATABASE_INFO_QUEUE),
            "DATABASE_INFO_QUEUE value should match");
        assertEquals(dbUri, optionsMap.get(DATABASE_URI),
            "DATABASE_URI value should match");
    }

    /**
     * Test buildOptionsMap with SQS configuration.
     */
    @Test
    public void testBuildOptionsMapWithSQS() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-east-1.amazonaws.com/123456/MyQueue"));
        options.setSQSInfoUri(sqsUri);

        Map<?, ?> optionsMap = options.buildOptionsMap();

        assertTrue(optionsMap.containsKey(SQS_INFO_URI),
            "Map should contain SQS_INFO_URI");
        assertEquals(sqsUri, optionsMap.get(SQS_INFO_URI),
            "SQS_INFO_URI value should match");
    }

    /**
     * Test buildOptionsMap with RabbitMQ configuration.
     */
    @Test
    public void testBuildOptionsMapWithRabbitMQ() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        RabbitMqUri rabbitUri = new RabbitMqUri(false, "user", "password", "localhost");
        options.setRabbitMqUri(rabbitUri);
        options.setRabbitMqInfoQueue("test-queue");

        Map<?, ?> optionsMap = options.buildOptionsMap();

        assertTrue(optionsMap.containsKey(RABBITMQ_URI),
            "Map should contain RABBITMQ_URI");
        assertTrue(optionsMap.containsKey(RABBITMQ_INFO_QUEUE),
            "Map should contain RABBITMQ_INFO_QUEUE");
        assertEquals(rabbitUri, optionsMap.get(RABBITMQ_URI),
            "RABBITMQ_URI value should match");
        assertEquals("test-queue", optionsMap.get(RABBITMQ_INFO_QUEUE),
            "RABBITMQ_INFO_QUEUE value should match");
    }

    /**
     * Test buildOptionsMap with defaults.
     */
    @Test
    public void testBuildOptionsMapDefaults() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        Map<?, ?> optionsMap = options.buildOptionsMap();

        assertNotNull(optionsMap, "Options map should not be null");

        // Default values should be present
        assertTrue(optionsMap.containsKey(CORE_CONCURRENCY),
            "Map should contain CORE_CONCURRENCY with default");
        assertEquals(DEFAULT_CORE_CONCURRENCY, optionsMap.get(CORE_CONCURRENCY),
            "Default CORE_CONCURRENCY should match");

        // Null values may or may not be present depending on implementation
        // but accessing them should not throw exception
        assertDoesNotThrow(() -> optionsMap.get(CORE_SETTINGS),
            "Accessing null property should not throw");
    }

    // ========================================================================
    // toJson() Tests
    // ========================================================================

    /**
     * Test toJson() convenience method.
     */
    @Test
    public void testToJson() {
        SzReplicatorOptions options = new SzReplicatorOptions();
        options.setCoreConcurrency(16);
        options.setCoreInstanceName("ToJsonTest");
        options.setCoreLogLevel(1);

        JsonObject json = options.toJson();

        assertNotNull(json, "toJson should return non-null JsonObject");
        assertTrue(json.containsKey("coreConcurrency"),
            "JSON should contain coreConcurrency");
        assertEquals(16, json.getInt("coreConcurrency"),
            "coreConcurrency value should match");
        assertTrue(json.containsKey("coreInstanceName"),
            "JSON should contain coreInstanceName");
        assertEquals("ToJsonTest", json.getString("coreInstanceName"),
            "coreInstanceName value should match");
    }

    /**
     * Test toJson() with defaults.
     */
    @Test
    public void testToJsonDefaults() {
        SzReplicatorOptions options = new SzReplicatorOptions();

        JsonObject json = options.toJson();

        assertNotNull(json, "toJson should return non-null JsonObject");
        // Should contain non-null default values
        assertTrue(json.containsKey("coreConcurrency"),
            "JSON should contain default coreConcurrency");
    }

    // ========================================================================
    // Map Constructor Tests
    // ========================================================================

    /**
     * Test constructor with Map parameter and verify buildOptionsMap round-trip.
     */
    @Test
    public void testMapConstructorRoundTrip() throws Exception {
        // Create original options with various properties set
        SzReplicatorOptions original = new SzReplicatorOptions();
        original.setCoreConcurrency(20);
        original.setCoreInstanceName("MapConstructorTest");
        original.setCoreLogLevel(2);
        original.setCoreConfigurationId(55555L);
        original.setRefreshConfigSeconds(1800L);
        original.setProcessingRate(ProcessingRate.LEISURELY);
        original.setUsingDatabaseQueue(true);

        ConnectionUri dbUri = new PostgreSqlUri("testuser", "testpass", "testhost", "testdb");
        original.setDatabaseUri(dbUri);

        // Build options map from original
        Map<CommandLineOption, Object> optionsMap = original.buildOptionsMap();

        assertNotNull(optionsMap, "buildOptionsMap should return non-null map");

        // Construct new instance from the map
        SzReplicatorOptions reconstructed = new SzReplicatorOptions(optionsMap);

        // Verify reconstructed object equals original
        assertEquals(original, reconstructed,
            "Object constructed from buildOptionsMap should equal original");

        // Verify individual properties
        assertEquals(original.getCoreConcurrency(), reconstructed.getCoreConcurrency(),
            "coreConcurrency should match");
        assertEquals(original.getCoreInstanceName(), reconstructed.getCoreInstanceName(),
            "coreInstanceName should match");
        assertEquals(original.getCoreLogLevel(), reconstructed.getCoreLogLevel(),
            "coreLogLevel should match");
        assertEquals(original.getCoreConfigurationId(), reconstructed.getCoreConfigurationId(),
            "coreConfigurationId should match");
        assertEquals(original.getRefreshConfigSeconds(), reconstructed.getRefreshConfigSeconds(),
            "refreshConfigSeconds should match");
        assertEquals(original.getProcessingRate(), reconstructed.getProcessingRate(),
            "processingRate should match");
        assertEquals(original.isUsingDatabaseQueue(), reconstructed.isUsingDatabaseQueue(),
            "usingDatabaseQueue should match");
        assertEquals(original.getDatabaseUri(), reconstructed.getDatabaseUri(),
            "databaseUri should match");
    }

    /**
     * Test Map constructor with SQS configuration.
     */
    @Test
    public void testMapConstructorWithSQS() throws Exception {
        SzReplicatorOptions original = new SzReplicatorOptions();

        SQSUri sqsUri = new SQSUri(
            new URI("https://sqs.us-west-2.amazonaws.com/999888/TestQueue"));
        original.setSQSInfoUri(sqsUri);
        original.setCoreConcurrency(12);

        Map<CommandLineOption, Object> optionsMap = original.buildOptionsMap();
        SzReplicatorOptions reconstructed = new SzReplicatorOptions(optionsMap);

        assertEquals(original, reconstructed,
            "SQS configuration should round-trip correctly");
        assertEquals(sqsUri, reconstructed.getSQSInfoUri(),
            "SQS URI should match");
    }

    /**
     * Test Map constructor with RabbitMQ configuration.
     */
    @Test
    public void testMapConstructorWithRabbitMQ() throws Exception {
        SzReplicatorOptions original = new SzReplicatorOptions();

        RabbitMqUri rabbitUri = new RabbitMqUri(true, "admin", "secret", "rabbitmq.local");
        original.setRabbitMqUri(rabbitUri);
        original.setRabbitMqInfoQueue("info-queue");
        original.setCoreConcurrency(8);

        Map<CommandLineOption, Object> optionsMap = original.buildOptionsMap();
        SzReplicatorOptions reconstructed = new SzReplicatorOptions(optionsMap);

        assertEquals(original, reconstructed,
            "RabbitMQ configuration should round-trip correctly");
        assertEquals(rabbitUri, reconstructed.getRabbitMqUri(),
            "RabbitMQ URI should match");
        assertEquals("info-queue", reconstructed.getRabbitMqInfoQueue(),
            "RabbitMQ queue should match");
    }

    /**
     * Test Map constructor with empty map.
     */
    @Test
    public void testMapConstructorEmpty() {
        Map<CommandLineOption, Object> emptyMap = new HashMap<>();
        SzReplicatorOptions options = new SzReplicatorOptions(emptyMap);

        assertNotNull(options, "Constructor should succeed with empty map");
        assertEquals(DEFAULT_CORE_CONCURRENCY, options.getCoreConcurrency(),
            "Should use default values when map is empty");
    }

    // ========================================================================
    // setOptions() Static Method Tests
    // ========================================================================

    /**
     * Test setOptions() static method updates existing instance.
     */
    @Test
    public void testSetOptionsUpdatesExistingInstance() throws Exception {
        // Create instance with initial values
        SzReplicatorOptions options = new SzReplicatorOptions();
        options.setCoreConcurrency(8);
        options.setCoreInstanceName("InitialInstance");
        options.setCoreLogLevel(0);
        options.setRefreshConfigSeconds(300L);

        // Create a map with partial updates
        Map<CommandLineOption, Object> updatesMap = new HashMap<>();
        updatesMap.put(CORE_CONCURRENCY, 16);  // Override
        updatesMap.put(CORE_LOG_LEVEL, 2);      // Override
        // Note: coreInstanceName and refreshConfigSeconds NOT in map - should remain unchanged

        // Apply updates using setOptions
        SzReplicatorOptionsAccessor.setOptions(options, updatesMap);

        // Verify overridden properties
        assertEquals(16, options.getCoreConcurrency(),
            "coreConcurrency should be updated to map value");
        assertEquals(2, options.getCoreLogLevel(),
            "coreLogLevel should be updated to map value");

        // Verify unchanged properties
        assertEquals("InitialInstance", options.getCoreInstanceName(),
            "coreInstanceName should remain unchanged when not in map");
        assertEquals(300L, options.getRefreshConfigSeconds(),
            "refreshConfigSeconds should remain unchanged when not in map");
    }

    /**
     * Test setOptions() with all properties in map.
     */
    @Test
    public void testSetOptionsAllProperties() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Create a complete options map
        Map<CommandLineOption, Object> optionsMap = new HashMap<>();
        optionsMap.put(CORE_CONCURRENCY, 24);
        optionsMap.put(CORE_INSTANCE_NAME, "SetOptionsTest");
        optionsMap.put(CORE_LOG_LEVEL, 1);
        optionsMap.put(CORE_CONFIG_ID, 12345L);
        optionsMap.put(REFRESH_CONFIG_SECONDS, 600L);
        optionsMap.put(PROCESSING_RATE, ProcessingRate.LEISURELY);
        optionsMap.put(DATABASE_INFO_QUEUE, true);

        ConnectionUri dbUri = new PostgreSqlUri("user", "pass", "host", "db");
        optionsMap.put(DATABASE_URI, dbUri);

        // Apply all options
        SzReplicatorOptionsAccessor.setOptions(options, optionsMap);

        // Verify all properties were set
        assertEquals(24, options.getCoreConcurrency());
        assertEquals("SetOptionsTest", options.getCoreInstanceName());
        assertEquals(1, options.getCoreLogLevel());
        assertEquals(12345L, options.getCoreConfigurationId());
        assertEquals(600L, options.getRefreshConfigSeconds());
        assertEquals(ProcessingRate.LEISURELY, options.getProcessingRate());
        assertTrue(options.isUsingDatabaseQueue());
        assertEquals(dbUri, options.getDatabaseUri());
    }

    /**
     * Test setOptions() with empty map doesn't change instance.
     */
    @Test
    public void testSetOptionsEmptyMap() {
        SzReplicatorOptions options = new SzReplicatorOptions();
        options.setCoreConcurrency(16);
        options.setCoreInstanceName("Original");

        Map<CommandLineOption, Object> emptyMap = new HashMap<>();

        SzReplicatorOptionsAccessor.setOptions(options, emptyMap);

        // Values should remain unchanged
        assertEquals(16, options.getCoreConcurrency(),
            "coreConcurrency should remain unchanged");
        assertEquals("Original", options.getCoreInstanceName(),
            "coreInstanceName should remain unchanged");
    }

    /**
     * Test setOptions() preserves properties not in map.
     */
    @Test
    public void testSetOptionsPreservesUnspecifiedProperties() throws Exception {
        SzReplicatorOptions options = new SzReplicatorOptions();

        // Set initial values for all properties
        options.setCoreConcurrency(8);
        options.setCoreInstanceName("Initial");
        options.setCoreLogLevel(0);
        options.setRefreshConfigSeconds(300L);
        options.setProcessingRate(ProcessingRate.STANDARD);

        ConnectionUri initialDbUri = new PostgreSqlUri("user1", "pass1", "host1", "db1");
        options.setDatabaseUri(initialDbUri);

        // Update only a few properties via map
        Map<CommandLineOption, Object> partialMap = new HashMap<>();
        partialMap.put(CORE_CONCURRENCY, 32);
        partialMap.put(CORE_LOG_LEVEL, 3);

        SzReplicatorOptionsAccessor.setOptions(options, partialMap);

        // Verify updated properties
        assertEquals(32, options.getCoreConcurrency(),
            "Updated property should have new value");
        assertEquals(3, options.getCoreLogLevel(),
            "Updated property should have new value");

        // Verify preserved properties
        assertEquals("Initial", options.getCoreInstanceName(),
            "Unspecified property should preserve original value");
        assertEquals(300L, options.getRefreshConfigSeconds(),
            "Unspecified property should preserve original value");
        assertEquals(ProcessingRate.STANDARD, options.getProcessingRate(),
            "Unspecified property should preserve original value");
        assertEquals(initialDbUri, options.getDatabaseUri(),
            "Unspecified property should preserve original value");
    }

    // ========================================================================
    // Exception Handling Tests
    // ========================================================================

    /**
     * Test buildJson() handles exceptions thrown by getters.
     * When a getter throws RuntimeException via reflection, it gets wrapped in
     * InvocationTargetException, then unwrapped and rethrown.
     */
    @Test
    public void testBuildJsonHandlesGetterException() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        RuntimeException testException = new IllegalStateException("Test getter exception");
        options.setThrowOnGetCoreConcurrency(testException);

        // buildJson should rethrow the RuntimeException from the getter
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            options.buildJson(null);
        }, "buildJson should propagate RuntimeException from getter");

        // The exception should be the same one we threw
        assertEquals("Test getter exception", exception.getMessage(),
            "Exception message should match");
    }

    /**
     * Test buildJson() handles InvocationTargetException with RuntimeException cause.
     */
    @Test
    public void testBuildJsonHandlesInvocationTargetExceptionRuntime() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        RuntimeException cause = new RuntimeException("Test runtime exception");
        options.setThrowOnGetCoreLogLevel(cause);

        // buildJson should rethrow RuntimeException from InvocationTargetException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            options.buildJson(null);
        }, "buildJson should rethrow RuntimeException from InvocationTargetException");

        assertEquals("Test runtime exception", exception.getMessage(),
            "Exception message should match");
    }

    /**
     * Test buildJson() handles checked exceptions thrown by getters.
     * When a getter throws a checked exception, our ThrowingReplicatorOptions wraps
     * it in RuntimeException, which then gets propagated by buildJson.
     */
    @Test
    public void testBuildJsonHandlesGetterCheckedExceptionWrapped() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        Exception cause = new Exception("Test checked exception");
        options.setThrowOnGetRefreshConfigSeconds(cause);

        // buildJson should propagate the RuntimeException wrapper
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            options.buildJson(null);
        }, "buildJson should propagate RuntimeException from getter");

        assertTrue(exception.getCause() instanceof Exception,
            "RuntimeException should have the checked exception as cause");
        assertEquals("Test checked exception", exception.getCause().getMessage(),
            "Cause message should match");
    }

    /**
     * Test parse() with target handles InvocationTargetException with RuntimeException cause.
     * When a setter throws RuntimeException, it gets wrapped in InvocationTargetException,
     * then the code unwraps and rethrows the RuntimeException.
     */
    @Test
    public void testParseHandlesSetterRuntimeException() {
        JsonObject json = Json.createObjectBuilder()
            .add("coreConcurrency", 16)
            .build();

        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        RuntimeException testException = new IllegalStateException("Test setter runtime exception");
        options.setThrowOnSetCoreConcurrency(testException);

        // Use the new parse method with target instance
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SzReplicatorOptions.parse(options, json);
        }, "parse should propagate RuntimeException from setter");

        assertEquals("Test setter runtime exception", exception.getMessage(),
            "Exception message should match");
    }

    /**
     * Test parse() with target handles InvocationTargetException with checked Exception cause.
     * When a setter throws checked Exception, it gets wrapped in InvocationTargetException,
     * then the code wraps it again in RuntimeException.
     */
    @Test
    public void testParseHandlesSetterCheckedException() {
        JsonObject json = Json.createObjectBuilder()
            .add("refreshConfigSeconds", 600)
            .build();

        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        Exception testException = new Exception("Test setter checked exception");
        options.setThrowOnSetRefreshConfigSeconds(testException);

        // Use the new parse method with target instance
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SzReplicatorOptions.parse(options, json);
        }, "parse should wrap checked exception in RuntimeException");

        assertTrue(exception.getCause() instanceof Exception,
            "Should have checked exception as cause");
        assertEquals("Test setter checked exception", exception.getCause().getMessage(),
            "Cause message should match");
    }

    /**
     * Test parse(target, String) with target instance.
     */
    @Test
    public void testParseStringWithTarget() {
        String jsonText = "{\"coreConcurrency\":20,\"coreLogLevel\":3}";

        SzReplicatorOptions target = new SzReplicatorOptions();
        target.setCoreInstanceName("PreExisting");

        SzReplicatorOptions result = SzReplicatorOptions.parse(target, jsonText);

        assertSame(target, result, "Should return the same target instance");
        assertEquals(20, result.getCoreConcurrency(),
            "Should update coreConcurrency from JSON");
        assertEquals(3, result.getCoreLogLevel(),
            "Should update coreLogLevel from JSON");
        assertEquals("PreExisting", result.getCoreInstanceName(),
            "Should preserve existing coreInstanceName not in JSON");
    }

    /**
     * Test parse(target, JsonObject) with target instance.
     */
    @Test
    public void testParseJsonObjectWithTarget() {
        JsonObject json = Json.createObjectBuilder()
            .add("coreLogLevel", 2)
            .add("refreshConfigSeconds", 1200)
            .build();

        SzReplicatorOptions target = new SzReplicatorOptions();
        target.setCoreConcurrency(32);

        SzReplicatorOptions result = SzReplicatorOptions.parse(target, json);

        assertSame(target, result, "Should return the same target instance");
        assertEquals(2, result.getCoreLogLevel(),
            "Should update coreLogLevel from JSON");
        assertEquals(1200L, result.getRefreshConfigSeconds(),
            "Should update refreshConfigSeconds from JSON");
        assertEquals(32, result.getCoreConcurrency(),
            "Should preserve existing coreConcurrency not in JSON");
    }

    /**
     * Test parse(null, String) creates new instance.
     */
    @Test
    public void testParseStringWithNullTarget() {
        String jsonText = "{\"coreConcurrency\":16}";

        SzReplicatorOptions result = SzReplicatorOptions.parse(null, jsonText);

        assertNotNull(result, "Should create new instance when target is null");
        assertEquals(16, result.getCoreConcurrency(),
            "Should parse coreConcurrency correctly");
    }

    /**
     * Test parse(null, JsonObject) creates new instance.
     */
    @Test
    public void testParseJsonObjectWithNullTarget() {
        JsonObject json = Json.createObjectBuilder()
            .add("coreLogLevel", 1)
            .build();

        SzReplicatorOptions result = SzReplicatorOptions.parse(null, json);

        assertNotNull(result, "Should create new instance when target is null");
        assertEquals(1, result.getCoreLogLevel(),
            "Should parse coreLogLevel correctly");
    }

    /**
     * Test buildOptionsMap() handles InvocationTargetException with RuntimeException cause.
     * When a getter throws RuntimeException, it gets wrapped in InvocationTargetException,
     * then the code unwraps and rethrows the RuntimeException.
     */
    @Test
    public void testBuildOptionsMapHandlesGetterRuntimeException() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        RuntimeException cause = new IllegalStateException("Simulated failure");
        options.setThrowOnGetProcessingRate(cause);

        // buildOptionsMap should rethrow RuntimeException from getter
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            options.buildOptionsMap();
        }, "buildOptionsMap should rethrow RuntimeException from InvocationTargetException");

        assertEquals("Simulated failure", exception.getMessage(),
            "Exception message should match");
    }

    /**
     * Test buildOptionsMap() handles InvocationTargetException with checked Exception cause.
     * When a getter throws checked Exception, it gets wrapped in InvocationTargetException,
     * then the code wraps it again in RuntimeException.
     */
    @Test
    public void testBuildOptionsMapHandlesGetterCheckedException() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        Exception testException = new Exception("Test getter checked exception");
        options.setThrowOnGetCoreInstanceName(testException);

        // buildOptionsMap should wrap checked exception in RuntimeException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            options.buildOptionsMap();
        }, "buildOptionsMap should wrap checked exception in RuntimeException");

        assertTrue(exception.getCause() instanceof Exception,
            "Should have checked exception as cause");
        assertEquals("Test getter checked exception", exception.getCause().getMessage(),
            "Cause message should match");
    }

    /**
     * Test setOptions() handles InvocationTargetException with RuntimeException cause.
     * When a setter throws RuntimeException during reflection invoke, it gets wrapped
     * in InvocationTargetException, then the code unwraps and rethrows the RuntimeException.
     */
    @Test
    public void testSetOptionsHandlesSetterRuntimeException() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        RuntimeException testException = new IllegalStateException("Test setter runtime exception");
        options.setThrowOnSetCoreLogLevel(testException);

        Map<CommandLineOption, Object> optionsMap = new HashMap<>();
        optionsMap.put(CORE_LOG_LEVEL, 1);

        // setOptions should propagate the RuntimeException from setter
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SzReplicatorOptionsAccessor.setOptions(options, optionsMap);
        }, "setOptions should propagate RuntimeException from setter");

        assertEquals("Test setter runtime exception", exception.getMessage(),
            "Exception message should match");
    }

    /**
     * Test setOptions() handles InvocationTargetException with checked Exception cause.
     * When a setter throws checked Exception, it gets wrapped in InvocationTargetException,
     * then the code wraps the cause in RuntimeException.
     */
    @Test
    public void testSetOptionsHandlesSetterCheckedException() {
        ThrowingReplicatorOptions options = new ThrowingReplicatorOptions();
        Exception cause = new Exception("Test setter checked exception");
        options.setThrowOnSetRefreshConfigSeconds(cause);

        Map<CommandLineOption, Object> optionsMap = new HashMap<>();
        optionsMap.put(REFRESH_CONFIG_SECONDS, 600L);

        // setOptions should wrap checked exception in RuntimeException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SzReplicatorOptionsAccessor.setOptions(options, optionsMap);
        }, "setOptions should wrap checked exception in RuntimeException");

        assertTrue(exception.getCause() instanceof Exception,
            "Cause should be the checked exception");
        assertEquals("Test setter checked exception", exception.getCause().getMessage(),
            "Cause message should match");
    }

    // ========================================================================
    // Helper Classes
    // ========================================================================

    /**
     * Helper class to access protected static methods for testing.
     */
    private static class SzReplicatorOptionsAccessor extends SzReplicatorOptions {
        /**
         * Expose the protected setOptions method for testing.
         */
        public static void setOptions(SzReplicatorOptions options,
                                      Map<CommandLineOption, Object> optionsMap) {
            SzReplicatorOptions.setOptions(options, optionsMap);
        }
    }

    /**
     * Test helper class that extends SzReplicatorOptions to allow simulating
     * exceptions in getters and setters for testing error handling paths.
     */
    private static class ThrowingReplicatorOptions extends SzReplicatorOptions {
        private Exception throwOnGetCoreConcurrency = null;
        private Exception throwOnGetCoreInstanceName = null;
        private Exception throwOnGetCoreLogLevel = null;
        private Exception throwOnGetRefreshConfigSeconds = null;
        private Exception throwOnGetProcessingRate = null;

        private Exception throwOnSetCoreConcurrency = null;
        private Exception throwOnSetCoreLogLevel = null;
        private Exception throwOnSetRefreshConfigSeconds = null;

        /**
         * Sneaky throw technique to throw checked exceptions without declaring them.
         */
        @SuppressWarnings("unchecked")
        private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
            throw (E) e;
        }

        public void setThrowOnGetCoreConcurrency(Exception e) {
            this.throwOnGetCoreConcurrency = e;
        }

        public void setThrowOnGetCoreInstanceName(Exception e) {
            this.throwOnGetCoreInstanceName = e;
        }

        public void setThrowOnGetCoreLogLevel(Exception e) {
            this.throwOnGetCoreLogLevel = e;
        }

        public void setThrowOnGetRefreshConfigSeconds(Exception e) {
            this.throwOnGetRefreshConfigSeconds = e;
        }

        public void setThrowOnGetProcessingRate(Exception e) {
            this.throwOnGetProcessingRate = e;
        }

        public void setThrowOnSetCoreConcurrency(Exception e) {
            this.throwOnSetCoreConcurrency = e;
        }

        public void setThrowOnSetCoreLogLevel(Exception e) {
            this.throwOnSetCoreLogLevel = e;
        }

        public void setThrowOnSetRefreshConfigSeconds(Exception e) {
            this.throwOnSetRefreshConfigSeconds = e;
        }

        @Override
        public int getCoreConcurrency() {
            if (throwOnGetCoreConcurrency != null) {
                sneakyThrow(throwOnGetCoreConcurrency);
            }
            return super.getCoreConcurrency();
        }

        @Override
        public String getCoreInstanceName() {
            if (throwOnGetCoreInstanceName != null) {
                sneakyThrow(throwOnGetCoreInstanceName);
            }
            return super.getCoreInstanceName();
        }

        @Override
        public int getCoreLogLevel() {
            if (throwOnGetCoreLogLevel != null) {
                sneakyThrow(throwOnGetCoreLogLevel);
            }
            return super.getCoreLogLevel();
        }

        @Override
        public long getRefreshConfigSeconds() {
            if (throwOnGetRefreshConfigSeconds != null) {
                sneakyThrow(throwOnGetRefreshConfigSeconds);
            }
            return super.getRefreshConfigSeconds();
        }

        @Override
        public ProcessingRate getProcessingRate() {
            if (throwOnGetProcessingRate != null) {
                sneakyThrow(throwOnGetProcessingRate);
            }
            return super.getProcessingRate();
        }

        @Override
        public SzReplicatorOptions setCoreConcurrency(Integer concurrency) {
            if (throwOnSetCoreConcurrency != null) {
                sneakyThrow(throwOnSetCoreConcurrency);
            }
            return super.setCoreConcurrency(concurrency);
        }

        @Override
        public SzReplicatorOptions setCoreLogLevel(int logLevel) {
            if (throwOnSetCoreLogLevel != null) {
                sneakyThrow(throwOnSetCoreLogLevel);
            }
            return super.setCoreLogLevel(logLevel);
        }

        @Override
        public SzReplicatorOptions setRefreshConfigSeconds(Long seconds) {
            if (throwOnSetRefreshConfigSeconds != null) {
                sneakyThrow(throwOnSetRefreshConfigSeconds);
            }
            return super.setRefreshConfigSeconds(seconds);
        }
    }
}
