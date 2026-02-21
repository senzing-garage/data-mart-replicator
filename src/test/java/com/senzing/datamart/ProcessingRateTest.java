package com.senzing.datamart;

import com.senzing.listener.service.scheduling.AbstractSchedulingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for {@link ProcessingRate}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProcessingRateTest {

    // ========================================================================
    // Enum Constant Tests
    // ========================================================================

    /**
     * Test that all expected enum constants exist.
     */
    @Test
    public void testEnumConstants() {
        ProcessingRate[] values = ProcessingRate.values();

        assertEquals(3, values.length, "Should have 3 enum constants");

        // Verify specific constants exist
        assertNotNull(ProcessingRate.LEISURELY);
        assertNotNull(ProcessingRate.STANDARD);
        assertNotNull(ProcessingRate.AGGRESSIVE);
    }

    /**
     * Test valueOf() works for all enum constants.
     */
    @Test
    public void testValueOf() {
        assertEquals(ProcessingRate.LEISURELY, ProcessingRate.valueOf("LEISURELY"));
        assertEquals(ProcessingRate.STANDARD, ProcessingRate.valueOf("STANDARD"));
        assertEquals(ProcessingRate.AGGRESSIVE, ProcessingRate.valueOf("AGGRESSIVE"));
    }

    /**
     * Test toString() returns enum name.
     */
    @Test
    public void testToString() {
        assertEquals("LEISURELY", ProcessingRate.LEISURELY.toString());
        assertEquals("STANDARD", ProcessingRate.STANDARD.toString());
        assertEquals("AGGRESSIVE", ProcessingRate.AGGRESSIVE.toString());
    }

    // ========================================================================
    // mergeSchedulingServiceOptions() Tests
    // ========================================================================

    /**
     * Test mergeSchedulingServiceOptions with null creates new JsonObject.
     */
    @Test
    public void testMergeSchedulingServiceOptionsWithNull() {
        JsonObject result = ProcessingRate.STANDARD.mergeSchedulingServiceOptions(null);

        assertNotNull(result, "Should create new JsonObject when null is passed");
        assertTrue(result.containsKey(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY),
            "Should contain FOLLOW_UP_DELAY_KEY");
        assertTrue(result.containsKey(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY),
            "Should contain FOLLOW_UP_TIMEOUT_KEY");
    }

    /**
     * Test mergeSchedulingServiceOptions does not overwrite existing keys.
     */
    @Test
    public void testMergeSchedulingServiceOptionsNoOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 9999L)
            .add("customKey", "customValue")
            .build();

        JsonObject result = ProcessingRate.LEISURELY.mergeSchedulingServiceOptions(existing);

        assertNotNull(result);
        // Existing value should be preserved (not overwritten)
        assertEquals(9999L, result.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue(),
            "Should preserve existing FOLLOW_UP_DELAY_KEY");
        // Custom key should be preserved
        assertEquals("customValue", result.getString("customKey"),
            "Should preserve existing custom keys");
        // Timeout should be added (wasn't in existing)
        assertTrue(result.containsKey(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY),
            "Should add missing keys");
    }

    /**
     * Test mergeSchedulingServiceOptions with overwrite=true replaces existing keys.
     */
    @Test
    public void testMergeSchedulingServiceOptionsWithOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 9999L)
            .add("customKey", "customValue")
            .build();

        JsonObject result = ProcessingRate.AGGRESSIVE.mergeSchedulingServiceOptions(existing, true);

        assertNotNull(result);
        // Existing value should be overwritten
        assertEquals(100L, result.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue(),
            "Should overwrite existing FOLLOW_UP_DELAY_KEY when overwrite=true");
        // Custom key should be preserved
        assertEquals("customValue", result.getString("customKey"),
            "Should preserve custom keys");
    }

    /**
     * Test mergeSchedulingServiceOptions with overwrite=false does not replace.
     */
    @Test
    public void testMergeSchedulingServiceOptionsWithoutOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 9999L)
            .build();

        JsonObject result = ProcessingRate.STANDARD.mergeSchedulingServiceOptions(existing, false);

        assertNotNull(result);
        // Existing value should NOT be overwritten
        assertEquals(9999L, result.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue(),
            "Should not overwrite when overwrite=false");
    }

    /**
     * Test mergeSchedulingServiceOptions with different ProcessingRates.
     */
    @Test
    public void testMergeSchedulingServiceOptionsAllRates() {
        // Test LEISURELY
        JsonObject leisurely = ProcessingRate.LEISURELY.mergeSchedulingServiceOptions(null);
        assertTrue(leisurely.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue() >
                   AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY,
            "LEISURELY should have longer delay than default");

        // Test STANDARD
        JsonObject standard = ProcessingRate.STANDARD.mergeSchedulingServiceOptions(null);
        assertEquals(AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY,
            standard.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue(),
            "STANDARD should use default delay");

        // Test AGGRESSIVE
        JsonObject aggressive = ProcessingRate.AGGRESSIVE.mergeSchedulingServiceOptions(null);
        assertTrue(aggressive.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue() <
                   AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY,
            "AGGRESSIVE should have shorter delay than default");
    }

    // ========================================================================
    // addSchedulingServiceOptions() Tests
    // ========================================================================

    /**
     * Test addSchedulingServiceOptions with null creates new builder.
     */
    @Test
    public void testAddSchedulingServiceOptionsWithNull() {
        JsonObjectBuilder result = ProcessingRate.STANDARD.addSchedulingServiceOptions(null);

        assertNotNull(result, "Should create new builder when null is passed");

        JsonObject json = result.build();
        assertTrue(json.containsKey(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY),
            "Should contain FOLLOW_UP_DELAY_KEY");
    }

    /**
     * Test addSchedulingServiceOptions with provided builder.
     */
    @Test
    public void testAddSchedulingServiceOptionsWithBuilder() {
        JsonObjectBuilder existingBuilder = Json.createObjectBuilder()
            .add("existingKey", "existingValue");

        JsonObjectBuilder result = ProcessingRate.LEISURELY.addSchedulingServiceOptions(existingBuilder);

        assertSame(existingBuilder, result, "Should return same builder instance");

        JsonObject json = result.build();
        assertEquals("existingValue", json.getString("existingKey"),
            "Should preserve existing properties");
        assertTrue(json.containsKey(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY),
            "Should add scheduling options");
    }

    /**
     * Test addSchedulingServiceOptions overwrites existing keys.
     */
    @Test
    public void testAddSchedulingServiceOptionsOverwrites() {
        JsonObjectBuilder builder = Json.createObjectBuilder()
            .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 9999L);

        ProcessingRate.AGGRESSIVE.addSchedulingServiceOptions(builder);

        JsonObject json = builder.build();
        assertEquals(100L, json.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue(),
            "Should overwrite existing keys");
    }

    // ========================================================================
    // mergeReplicatorServiceOptions() Tests
    // ========================================================================

    /**
     * Test mergeReplicatorServiceOptions with null creates new JsonObject.
     */
    @Test
    public void testMergeReplicatorServiceOptionsWithNull() {
        JsonObject result = ProcessingRate.STANDARD.mergeReplicatorServiceOptions(null);

        assertNotNull(result, "Should create new JsonObject when null is passed");
        assertTrue(result.containsKey(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY),
            "Should contain REPORT_UPDATE_PERIOD_KEY");
    }

    /**
     * Test mergeReplicatorServiceOptions does not overwrite existing keys.
     */
    @Test
    public void testMergeReplicatorServiceOptionsNoOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY, 9999L)
            .add("customKey", "customValue")
            .build();

        JsonObject result = ProcessingRate.LEISURELY.mergeReplicatorServiceOptions(existing);

        assertNotNull(result);
        // Existing value should be preserved
        assertEquals(9999L, result.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue(),
            "Should preserve existing REPORT_UPDATE_PERIOD_KEY");
        assertEquals("customValue", result.getString("customKey"),
            "Should preserve custom keys");
    }

    /**
     * Test mergeReplicatorServiceOptions with overwrite=true replaces existing keys.
     */
    @Test
    public void testMergeReplicatorServiceOptionsWithOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY, 9999L)
            .build();

        JsonObject result = ProcessingRate.AGGRESSIVE.mergeReplicatorServiceOptions(existing, true);

        assertNotNull(result);
        // Existing value should be overwritten
        assertEquals(1L, result.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue(),
            "Should overwrite REPORT_UPDATE_PERIOD_KEY when overwrite=true");
    }

    /**
     * Test mergeReplicatorServiceOptions with overwrite=false does not replace.
     */
    @Test
    public void testMergeReplicatorServiceOptionsWithoutOverwrite() {
        JsonObject existing = Json.createObjectBuilder()
            .add(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY, 9999L)
            .build();

        JsonObject result = ProcessingRate.STANDARD.mergeReplicatorServiceOptions(existing, false);

        assertNotNull(result);
        // Existing value should NOT be overwritten
        assertEquals(9999L, result.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue(),
            "Should not overwrite when overwrite=false");
    }

    // ========================================================================
    // addReplicatorServiceOptions() Tests
    // ========================================================================

    /**
     * Test addReplicatorServiceOptions with null creates new builder.
     */
    @Test
    public void testAddReplicatorServiceOptionsWithNull() {
        JsonObjectBuilder result = ProcessingRate.STANDARD.addReplicatorServiceOptions(null);

        assertNotNull(result, "Should create new builder when null is passed");

        JsonObject json = result.build();
        assertTrue(json.containsKey(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY),
            "Should contain REPORT_UPDATE_PERIOD_KEY");
    }

    /**
     * Test addReplicatorServiceOptions with provided builder.
     */
    @Test
    public void testAddReplicatorServiceOptionsWithBuilder() {
        JsonObjectBuilder existingBuilder = Json.createObjectBuilder()
            .add("existingKey", "existingValue");

        JsonObjectBuilder result = ProcessingRate.LEISURELY.addReplicatorServiceOptions(existingBuilder);

        assertSame(existingBuilder, result, "Should return same builder instance");

        JsonObject json = result.build();
        assertEquals("existingValue", json.getString("existingKey"),
            "Should preserve existing properties");
        assertTrue(json.containsKey(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY),
            "Should add replicator options");
    }

    /**
     * Test addReplicatorServiceOptions overwrites existing keys.
     */
    @Test
    public void testAddReplicatorServiceOptionsOverwrites() {
        JsonObjectBuilder builder = Json.createObjectBuilder()
            .add(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY, 9999L);

        ProcessingRate.AGGRESSIVE.addReplicatorServiceOptions(builder);

        JsonObject json = builder.build();
        assertEquals(1L, json.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue(),
            "Should overwrite existing keys");
    }

    // ========================================================================
    // Rate-Specific Configuration Tests
    // ========================================================================

    /**
     * Test LEISURELY has appropriate timing values.
     */
    @Test
    public void testLeisurelyTimings() {
        JsonObject scheduling = ProcessingRate.LEISURELY.mergeSchedulingServiceOptions(null);
        JsonObject replicator = ProcessingRate.LEISURELY.mergeReplicatorServiceOptions(null);

        // LEISURELY should have longer delays (5x default)
        long followUpDelay = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();
        long followUpTimeout = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY).longValue();
        long reportPeriod = replicator.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue();

        assertEquals(AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY * 5, followUpDelay,
            "LEISURELY follow-up delay should be 5x default");
        assertEquals(AbstractSchedulingService.DEFAULT_FOLLOW_UP_TIMEOUT * 5, followUpTimeout,
            "LEISURELY follow-up timeout should be 5x default");
        assertEquals(SzReplicatorService.DEFAULT_REPORT_UPDATE_PERIOD * 5, reportPeriod,
            "LEISURELY report period should be 5x default");
    }

    /**
     * Test STANDARD has default timing values.
     */
    @Test
    public void testStandardTimings() {
        JsonObject scheduling = ProcessingRate.STANDARD.mergeSchedulingServiceOptions(null);
        JsonObject replicator = ProcessingRate.STANDARD.mergeReplicatorServiceOptions(null);

        long followUpDelay = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();
        long followUpTimeout = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY).longValue();
        long reportPeriod = replicator.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue();

        assertEquals(AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY, followUpDelay,
            "STANDARD follow-up delay should be default");
        assertEquals(AbstractSchedulingService.DEFAULT_FOLLOW_UP_TIMEOUT, followUpTimeout,
            "STANDARD follow-up timeout should be default");
        assertEquals(SzReplicatorService.DEFAULT_REPORT_UPDATE_PERIOD, reportPeriod,
            "STANDARD report period should be default");
    }

    /**
     * Test AGGRESSIVE has short timing values.
     */
    @Test
    public void testAggressiveTimings() {
        JsonObject scheduling = ProcessingRate.AGGRESSIVE.mergeSchedulingServiceOptions(null);
        JsonObject replicator = ProcessingRate.AGGRESSIVE.mergeReplicatorServiceOptions(null);

        long followUpDelay = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();
        long followUpTimeout = scheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY).longValue();
        long reportPeriod = replicator.getJsonNumber(SzReplicatorService.REPORT_UPDATE_PERIOD_KEY).longValue();

        assertEquals(100L, followUpDelay, "AGGRESSIVE follow-up delay should be 100ms");
        assertEquals(300L, followUpTimeout, "AGGRESSIVE follow-up timeout should be 300ms");
        assertEquals(1L, reportPeriod, "AGGRESSIVE report period should be 1ms");
    }

    /**
     * Test that timing values are properly ordered: AGGRESSIVE < STANDARD < LEISURELY.
     */
    @Test
    public void testTimingOrdering() {
        JsonObject leisurelyScheduling = ProcessingRate.LEISURELY.mergeSchedulingServiceOptions(null);
        JsonObject standardScheduling = ProcessingRate.STANDARD.mergeSchedulingServiceOptions(null);
        JsonObject aggressiveScheduling = ProcessingRate.AGGRESSIVE.mergeSchedulingServiceOptions(null);

        long leisurelyDelay = leisurelyScheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();
        long standardDelay = standardScheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();
        long aggressiveDelay = aggressiveScheduling.getJsonNumber(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY).longValue();

        assertTrue(aggressiveDelay < standardDelay,
            "AGGRESSIVE delay should be less than STANDARD");
        assertTrue(standardDelay < leisurelyDelay,
            "STANDARD delay should be less than LEISURELY");
    }
}
