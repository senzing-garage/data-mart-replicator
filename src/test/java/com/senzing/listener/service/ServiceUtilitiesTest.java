package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceSetupException;
import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonObject;

import static org.junit.jupiter.api.Assertions.*;

class ServiceUtilitiesTest {

    // ========================================================================
    // getConfigString(JsonObject, String, boolean) tests
    // ========================================================================

    @Test
    void testGetConfigStringRequiredPresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "testValue")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", true);

        assertEquals("testValue", result);
    }

    @Test
    void testGetConfigStringRequiredMissing() {
        JsonObject config = Json.createObjectBuilder().build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigString(config, "name", true));

        assertTrue(exception.getMessage().contains("missing"));
        assertTrue(exception.getMessage().contains("name"));
    }

    @Test
    void testGetConfigStringNotRequiredMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        String result = ServiceUtilities.getConfigString(config, "name", false);

        assertNull(result);
    }

    @Test
    void testGetConfigStringTrimsWhitespace() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "  testValue  ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", true);

        assertEquals("testValue", result);
    }

    @Test
    void testGetConfigStringEmptyStringBecomesNull() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", false);

        assertNull(result);
    }

    @Test
    void testGetConfigStringWhitespaceOnlyBecomesNull() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "   ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", false);

        assertNull(result);
    }

    @Test
    void testGetConfigStringRequiredEmptyThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "")
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigString(config, "name", true));

        assertTrue(exception.getMessage().contains("null or empty"));
        assertTrue(exception.getMessage().contains("name"));
    }

    // ========================================================================
    // getConfigString(JsonObject, String, boolean, boolean) tests
    // ========================================================================

    @Test
    void testGetConfigStringWithNormalizeTrue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "  ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", false, true);

        assertNull(result);
    }

    @Test
    void testGetConfigStringWithNormalizeFalse() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "  ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", false, false);

        assertEquals("", result);
    }

    @Test
    void testGetConfigStringRequiredWithNormalizeFalseEmptyAllowed() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", true, false);

        assertEquals("", result);
    }

    // ========================================================================
    // getConfigString(JsonObject, String, String) tests - with default value
    // ========================================================================

    @Test
    void testGetConfigStringWithDefaultValuePresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "actualValue")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", "defaultValue");

        assertEquals("actualValue", result);
    }

    @Test
    void testGetConfigStringWithDefaultValueMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        String result = ServiceUtilities.getConfigString(config, "name", "defaultValue");

        assertEquals("defaultValue", result);
    }

    @Test
    void testGetConfigStringWithNullDefaultValue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        String result = ServiceUtilities.getConfigString(config, "name", (String) null);

        assertNull(result);
    }

    // ========================================================================
    // getConfigString(JsonObject, String, String, boolean) tests
    // ========================================================================

    @Test
    void testGetConfigStringWithDefaultAndNormalizeTrue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "   ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", "default", true);

        assertNull(result);
    }

    @Test
    void testGetConfigStringWithDefaultAndNormalizeFalse() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "   ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", "default", false);

        assertEquals("", result);
    }

    @Test
    void testGetConfigStringTrimsRegardlessOfNormalize() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("name", "  value  ")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", "default", false);

        assertEquals("value", result);
    }

    // ========================================================================
    // getConfigInteger(JsonObject, String, boolean, Integer) tests
    // ========================================================================

    @Test
    void testGetConfigIntegerRequiredPresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 42)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", true, null);

        assertEquals(42, result);
    }

    @Test
    void testGetConfigIntegerRequiredMissing() {
        JsonObject config = Json.createObjectBuilder().build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigInteger(config, "count", true, null));

        assertTrue(exception.getMessage().contains("missing"));
        assertTrue(exception.getMessage().contains("count"));
    }

    @Test
    void testGetConfigIntegerNotRequiredMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", false, null);

        assertNull(result);
    }

    @Test
    void testGetConfigIntegerWithMinimumValid() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 10)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", true, 5);

        assertEquals(10, result);
    }

    @Test
    void testGetConfigIntegerWithMinimumExact() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 5)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", true, 5);

        assertEquals(5, result);
    }

    @Test
    void testGetConfigIntegerBelowMinimumThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 3)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigInteger(config, "count", true, 5));

        assertTrue(exception.getMessage().contains("cannot be less than"));
        assertTrue(exception.getMessage().contains("5"));
        assertTrue(exception.getMessage().contains("3"));
    }

    // ========================================================================
    // getConfigInteger(JsonObject, String, Integer, Integer) tests - with default
    // ========================================================================

    @Test
    void testGetConfigIntegerWithDefaultValuePresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 42)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", null, 100);

        assertEquals(42, result);
    }

    @Test
    void testGetConfigIntegerWithDefaultValueMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", null, 100);

        assertEquals(100, result);
    }

    @Test
    void testGetConfigIntegerWithDefaultAndMinimumValid() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 50)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", 10, 100);

        assertEquals(50, result);
    }

    @Test
    void testGetConfigIntegerWithDefaultAndMinimumInvalid() {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 5)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigInteger(config, "count", 10, 100));

        assertTrue(exception.getMessage().contains("cannot be less than"));
    }

    // ========================================================================
    // getConfigLong(JsonObject, String, boolean, Long) tests
    // ========================================================================

    @Test
    void testGetConfigLongRequiredPresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 9999999999L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", true, null);

        assertEquals(9999999999L, result);
    }

    @Test
    void testGetConfigLongRequiredMissing() {
        JsonObject config = Json.createObjectBuilder().build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigLong(config, "bigNumber", true, null));

        assertTrue(exception.getMessage().contains("missing"));
        assertTrue(exception.getMessage().contains("bigNumber"));
    }

    @Test
    void testGetConfigLongNotRequiredMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", false, null);

        assertNull(result);
    }

    @Test
    void testGetConfigLongWithMinimumValid() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 1000L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", true, 500L);

        assertEquals(1000L, result);
    }

    @Test
    void testGetConfigLongWithMinimumExact() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 500L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", true, 500L);

        assertEquals(500L, result);
    }

    @Test
    void testGetConfigLongBelowMinimumThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 100L)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigLong(config, "bigNumber", true, 500L));

        assertTrue(exception.getMessage().contains("cannot be less than"));
        assertTrue(exception.getMessage().contains("500"));
        assertTrue(exception.getMessage().contains("100"));
    }

    // ========================================================================
    // getConfigLong(JsonObject, String, Long, Long) tests - with default
    // ========================================================================

    @Test
    void testGetConfigLongWithDefaultValuePresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 42L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", null, 100L);

        assertEquals(42L, result);
    }

    @Test
    void testGetConfigLongWithDefaultValueMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", null, 100L);

        assertEquals(100L, result);
    }

    @Test
    void testGetConfigLongWithDefaultAndMinimumValid() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 50L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", 10L, 100L);

        assertEquals(50L, result);
    }

    @Test
    void testGetConfigLongWithDefaultAndMinimumInvalid() {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 5L)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigLong(config, "bigNumber", 10L, 100L));

        assertTrue(exception.getMessage().contains("cannot be less than"));
    }

    // ========================================================================
    // getConfigBoolean(JsonObject, String, boolean) tests
    // ========================================================================

    @Test
    void testGetConfigBooleanRequiredPresentTrue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("enabled", true)
                .build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", true);

        assertTrue(result);
    }

    @Test
    void testGetConfigBooleanRequiredPresentFalse() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("enabled", false)
                .build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", true);

        assertFalse(result);
    }

    @Test
    void testGetConfigBooleanRequiredMissing() {
        JsonObject config = Json.createObjectBuilder().build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigBoolean(config, "enabled", true));

        assertTrue(exception.getMessage().contains("missing"));
        assertTrue(exception.getMessage().contains("enabled"));
    }

    @Test
    void testGetConfigBooleanNotRequiredMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", false);

        assertNull(result);
    }

    // ========================================================================
    // getConfigBoolean(JsonObject, String, Boolean) tests - with default
    // ========================================================================

    @Test
    void testGetConfigBooleanWithDefaultValuePresent() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("enabled", true)
                .build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", false);

        assertTrue(result);
    }

    @Test
    void testGetConfigBooleanWithDefaultValueMissing() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", Boolean.TRUE);

        assertTrue(result);
    }

    @Test
    void testGetConfigBooleanWithNullDefaultValue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder().build();

        Boolean result = ServiceUtilities.getConfigBoolean(config, "enabled", (Boolean) null);

        assertNull(result);
    }

    // ========================================================================
    // Edge cases and error handling tests
    // ========================================================================

    @Test
    void testGetConfigStringWithJsonNull() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .addNull("name")
                .build();

        String result = ServiceUtilities.getConfigString(config, "name", false);

        assertNull(result);
    }

    @Test
    void testGetConfigIntegerWithNullMinimum() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", -100)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", null, null);

        assertEquals(-100, result);
    }

    @Test
    void testGetConfigLongWithNullMinimum() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", -100L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", null, null);

        assertEquals(-100L, result);
    }

    @Test
    void testGetConfigIntegerFromInvalidStringValueThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("count", "not-a-number")
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigInteger(config, "count", null, null));

        assertTrue(exception.getMessage().contains("Failed to parse"));
    }

    @Test
    void testGetConfigLongFromInvalidStringValueThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", "not-a-number")
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigLong(config, "bigNumber", null, null));

        assertTrue(exception.getMessage().contains("Failed to parse"));
    }

    @Test
    void testGetConfigBooleanFromInvalidStringValueThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("enabled", "not-a-boolean")
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigBoolean(config, "enabled", null));

        assertTrue(exception.getMessage().contains("Failed to parse"));
    }

    // Test with nested JsonObject - ensure the key lookup works at the top level
    @Test
    void testGetConfigStringFromNestedConfig() throws ServiceSetupException {
        JsonObject nested = Json.createObjectBuilder()
                .add("inner", "innerValue")
                .build();
        JsonObject config = Json.createObjectBuilder()
                .add("outer", "outerValue")
                .add("nested", nested)
                .build();

        String result = ServiceUtilities.getConfigString(config, "outer", true);

        assertEquals("outerValue", result);
    }

    @Test
    void testGetConfigIntegerZeroValue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("count", 0)
                .build();

        Integer result = ServiceUtilities.getConfigInteger(config, "count", true, null);

        assertEquals(0, result);
    }

    @Test
    void testGetConfigLongZeroValue() throws ServiceSetupException {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", 0L)
                .build();

        Long result = ServiceUtilities.getConfigLong(config, "bigNumber", true, null);

        assertEquals(0L, result);
    }

    @Test
    void testGetConfigIntegerNegativeWithPositiveMinimumThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("count", -5)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigInteger(config, "count", 0, null));

        assertTrue(exception.getMessage().contains("cannot be less than"));
    }

    @Test
    void testGetConfigLongNegativeWithPositiveMinimumThrows() {
        JsonObject config = Json.createObjectBuilder()
                .add("bigNumber", -5L)
                .build();

        ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                () -> ServiceUtilities.getConfigLong(config, "bigNumber", 0L, null));

        assertTrue(exception.getMessage().contains("cannot be less than"));
    }
}
