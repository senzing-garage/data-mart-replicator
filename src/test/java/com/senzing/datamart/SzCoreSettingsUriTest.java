package com.senzing.datamart;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SzCoreSettingsUri}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzCoreSettingsUriTest {

    static {
        // Preload URI classes to ensure they're registered with ConnectionUri
        try {
            new PostgreSqlUri("user", "pass", "host", "db");
            new SQLiteUri(new File("/tmp/preload.db"));
        } catch (Exception e) {
            // Ignore - just preloading classes
        }
    }

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    /**
     * Test constructor with jsonPath only.
     */
    @Test
    public void testConstructorWithPath() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        assertNotNull(uri);
        assertEquals("/SQL/CONNECTION", uri.getPath(),
            "Path should have leading slash");

        List<String> elements = uri.getPathElements();
        assertEquals(2, elements.size());
        assertEquals("SQL", elements.get(0));
        assertEquals("CONNECTION", elements.get(1));
    }

    /**
     * Test constructor with path that already has leading slash.
     */
    @Test
    public void testConstructorWithLeadingSlash() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("/SQL/CONNECTION");

        assertEquals("/SQL/CONNECTION", uri.getPath());

        List<String> elements = uri.getPathElements();
        assertEquals(2, elements.size());
        assertEquals("SQL", elements.get(0));
        assertEquals("CONNECTION", elements.get(1));
    }

    /**
     * Test constructor with jsonPath and query options.
     */
    @Test
    public void testConstructorWithQueryOptions() {
        Map<String, String> queryOptions = new LinkedHashMap<>();
        queryOptions.put("param1", "value1");
        queryOptions.put("param2", "value2");

        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION", queryOptions);

        assertNotNull(uri);
        assertEquals("/SQL/CONNECTION", uri.getPath());

        Map<String, String> retrievedOptions = uri.getQueryOptions();
        assertEquals(2, retrievedOptions.size());
        assertEquals("value1", retrievedOptions.get("param1"));
        assertEquals("value2", retrievedOptions.get("param2"));
    }

    /**
     * Test constructor with null jsonPath throws NullPointerException.
     */
    @Test
    public void testConstructorWithNullPath() {
        assertThrows(NullPointerException.class, () -> {
            new SzCoreSettingsUri(null);
        }, "Should throw NullPointerException for null jsonPath");
    }

    /**
     * Test constructor with empty path element throws IllegalArgumentException.
     */
    @Test
    public void testConstructorWithEmptyPathElement() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new SzCoreSettingsUri("SQL//CONNECTION");
        }, "Should throw IllegalArgumentException for empty path element");

        assertTrue(exception.getMessage().contains("empty"),
            "Exception message should mention empty path element");
    }

    /**
     * Test that pathElements list is unmodifiable.
     */
    @Test
    public void testPathElementsUnmodifiable() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");
        List<String> elements = uri.getPathElements();

        assertThrows(UnsupportedOperationException.class, () -> {
            elements.add("NEW");
        }, "Path elements list should be unmodifiable");
    }

    // ========================================================================
    // parse() Method Tests
    // ========================================================================

    /**
     * Test parse() with valid URI.
     */
    @Test
    public void testParseValidUri() {
        SzCoreSettingsUri uri = SzCoreSettingsUri.parse("sz://core-settings/SQL/CONNECTION");

        assertNotNull(uri);
        assertEquals("/SQL/CONNECTION", uri.getPath());
    }

    /**
     * Test parse() with query options.
     */
    @Test
    public void testParseWithQueryOptions() {
        SzCoreSettingsUri uri = SzCoreSettingsUri.parse(
            "sz://core-settings/SQL/CONNECTION?param1=value1&param2=value2");

        assertNotNull(uri);
        assertEquals("/SQL/CONNECTION", uri.getPath());

        Map<String, String> options = uri.getQueryOptions();
        assertEquals(2, options.size());
        assertEquals("value1", options.get("param1"));
        assertEquals("value2", options.get("param2"));
    }

    /**
     * Test parse() with null throws NullPointerException.
     */
    @Test
    public void testParseWithNull() {
        assertThrows(NullPointerException.class, () -> {
            SzCoreSettingsUri.parse(null);
        }, "Should throw NullPointerException for null URI");
    }

    /**
     * Test parse() with invalid prefix throws IllegalArgumentException.
     */
    @Test
    public void testParseWithInvalidPrefix() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            SzCoreSettingsUri.parse("http://core-settings/SQL/CONNECTION");
        }, "Should throw IllegalArgumentException for invalid prefix");

        assertTrue(exception.getMessage().contains("sz://core-settings/"),
            "Exception message should mention required prefix");
    }

    /**
     * Test parse() with missing scheme.
     */
    @Test
    public void testParseWithMissingScheme() {
        assertThrows(IllegalArgumentException.class, () -> {
            SzCoreSettingsUri.parse("SQL/CONNECTION");
        }, "Should throw IllegalArgumentException for missing scheme");
    }

    /**
     * Test parse() with case-insensitive scheme.
     */
    @Test
    public void testParseWithUppercaseScheme() {
        SzCoreSettingsUri uri = SzCoreSettingsUri.parse("SZ://CORE-SETTINGS/SQL/CONNECTION");

        assertNotNull(uri, "Parse should be case-insensitive for scheme");
        assertEquals("/SQL/CONNECTION", uri.getPath());
    }

    // ========================================================================
    // resolveUri() Method Tests
    // ========================================================================

    /**
     * Test resolveUri() with valid JSON path.
     */
    @Test
    public void testResolveUriWithValidPath() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("SQL", Json.createObjectBuilder()
                .add("CONNECTION", "postgresql://user:pass@localhost:5432/dbname"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNotNull(resolved, "Should resolve to a ConnectionUri");
        assertTrue(resolved instanceof PostgreSqlUri,
            "Should resolve to PostgreSqlUri");
    }

    /**
     * Test resolveUri() with String parameter.
     */
    @Test
    public void testResolveUriWithString() {
        String coreSettings = "{\"SQL\":{\"CONNECTION\":\"sqlite3://na:na@/tmp/testdb.db\"}}";

        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNotNull(resolved, "Should resolve to a ConnectionUri");
        assertTrue(resolved instanceof SQLiteUri,
            "Should resolve to SQLiteUri");
    }

    /**
     * Test resolveUri() with non-existent path returns null.
     */
    @Test
    public void testResolveUriWithNonExistentPath() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("SQL", Json.createObjectBuilder()
                .add("CONNECTION", "postgresql://user:pass@localhost:5432/G2"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("NONEXISTENT/PATH");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNull(resolved, "Should return null for non-existent path");
    }

    /**
     * Test resolveUri() with array index in path.
     */
    @Test
    public void testResolveUriWithArrayIndex() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("DATABASES", Json.createArrayBuilder()
                .add("postgresql://user:pass@localhost:5432/db1")
                .add("postgresql://user:pass@localhost:5432/db2"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("DATABASES/0");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNotNull(resolved, "Should resolve array element");
        assertTrue(resolved instanceof PostgreSqlUri,
            "Should resolve to PostgreSqlUri");
    }

    /**
     * Test resolveUri() with invalid array index throws IllegalArgumentException.
     */
    @Test
    public void testResolveUriWithInvalidArrayIndex() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("DATABASES", Json.createArrayBuilder()
                .add("postgresql://user:pass@localhost:5432/db1"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("DATABASES/notanumber");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            uri.resolveUri(coreSettings);
        }, "Should throw IllegalArgumentException for non-numeric array index");

        assertTrue(exception.getMessage().contains("array index"),
            "Exception message should mention array index");
    }

    /**
     * Test resolveUri() with negative array index throws IllegalArgumentException.
     */
    @Test
    public void testResolveUriWithNegativeArrayIndex() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("DATABASES", Json.createArrayBuilder()
                .add("postgresql://user:pass@localhost:5432/db1"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("DATABASES/-1");

        assertThrows(IllegalArgumentException.class, () -> {
            uri.resolveUri(coreSettings);
        }, "Should throw IllegalArgumentException for negative array index");
    }

    /**
     * Test resolveUri() with array index out of bounds returns null.
     */
    @Test
    public void testResolveUriWithArrayIndexOutOfBounds() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("DATABASES", Json.createArrayBuilder()
                .add("postgresql://user:pass@localhost:5432/db1"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("DATABASES/5");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNull(resolved, "Should return null for out-of-bounds array index");
    }

    /**
     * Test resolveUri() with null coreSettings throws NullPointerException.
     */
    @Test
    public void testResolveUriWithNullSettings() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        assertThrows(NullPointerException.class, () -> {
            uri.resolveUri((JsonObject) null);
        }, "Should throw NullPointerException for null coreSettings");

        assertThrows(NullPointerException.class, () -> {
            uri.resolveUri((String) null);
        }, "Should throw NullPointerException for null coreSettings string");
    }

    // ========================================================================
    // toString() Tests
    // ========================================================================

    /**
     * Test toString() without query options.
     */
    @Test
    public void testToStringWithoutQueryOptions() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");
        String result = uri.toString();

        assertEquals("sz://core-settings/SQL/CONNECTION", result);
    }

    /**
     * Test toString() with query options.
     */
    @Test
    public void testToStringWithQueryOptions() {
        Map<String, String> queryOptions = new LinkedHashMap<>();
        queryOptions.put("param1", "value1");

        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION", queryOptions);
        String result = uri.toString();

        assertTrue(result.startsWith("sz://core-settings/SQL/CONNECTION"),
            "Should start with scheme and path");
        assertTrue(result.contains("param1=value1"),
            "Should contain query parameter");
    }

    // ========================================================================
    // equals() and hashCode() Tests
    // ========================================================================

    /**
     * Test equals() with same instance returns true.
     */
    @Test
    public void testEqualsSameInstance() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        assertTrue(uri.equals(uri), "Should equal itself");
    }

    /**
     * Test equals() with null returns false.
     */
    @Test
    public void testEqualsWithNull() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        assertFalse(uri.equals(null), "Should not equal null");
    }

    /**
     * Test equals() with different class returns false.
     */
    @Test
    public void testEqualsWithDifferentClass() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        assertFalse(uri.equals("not a URI"), "Should not equal different class");
    }

    /**
     * Test equals() with equal instances.
     */
    @Test
    public void testEqualsWithEqualInstances() {
        SzCoreSettingsUri uri1 = new SzCoreSettingsUri("SQL/CONNECTION");
        SzCoreSettingsUri uri2 = new SzCoreSettingsUri("SQL/CONNECTION");

        assertTrue(uri1.equals(uri2), "Equal instances should be equal");
        assertEquals(uri1.hashCode(), uri2.hashCode(),
            "Equal instances should have equal hash codes");
    }

    /**
     * Test equals() with different paths.
     */
    @Test
    public void testEqualsWithDifferentPaths() {
        SzCoreSettingsUri uri1 = new SzCoreSettingsUri("SQL/CONNECTION");
        SzCoreSettingsUri uri2 = new SzCoreSettingsUri("SQL/OTHER");

        assertFalse(uri1.equals(uri2), "Different paths should not be equal");
    }

    /**
     * Test equals() with different query options.
     */
    @Test
    public void testEqualsWithDifferentQueryOptions() {
        Map<String, String> options1 = Map.of("param1", "value1");
        Map<String, String> options2 = Map.of("param2", "value2");

        SzCoreSettingsUri uri1 = new SzCoreSettingsUri("SQL/CONNECTION", options1);
        SzCoreSettingsUri uri2 = new SzCoreSettingsUri("SQL/CONNECTION", options2);

        assertFalse(uri1.equals(uri2), "Different query options should not be equal");
    }

    /**
     * Test hashCode() consistency.
     */
    @Test
    public void testHashCodeConsistency() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("SQL/CONNECTION");

        int hash1 = uri.hashCode();
        int hash2 = uri.hashCode();

        assertEquals(hash1, hash2, "Hash code should be consistent");
    }

    // ========================================================================
    // Additional Edge Cases
    // ========================================================================

    /**
     * Test with single path element.
     */
    @Test
    public void testSinglePathElement() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("CONNECTION");

        assertEquals("/CONNECTION", uri.getPath());
        assertEquals(1, uri.getPathElements().size());
        assertEquals("CONNECTION", uri.getPathElements().get(0));
    }

    /**
     * Test with complex path.
     */
    @Test
    public void testComplexPath() {
        SzCoreSettingsUri uri = new SzCoreSettingsUri("LEVEL1/LEVEL2/LEVEL3/CONNECTION");

        assertEquals("/LEVEL1/LEVEL2/LEVEL3/CONNECTION", uri.getPath());
        assertEquals(4, uri.getPathElements().size());
        assertEquals("LEVEL1", uri.getPathElements().get(0));
        assertEquals("LEVEL2", uri.getPathElements().get(1));
        assertEquals("LEVEL3", uri.getPathElements().get(2));
        assertEquals("CONNECTION", uri.getPathElements().get(3));
    }

    /**
     * Test parse() and toString() round-trip.
     */
    @Test
    public void testParseToStringRoundTrip() {
        String original = "sz://core-settings/SQL/CONNECTION?param=value";
        SzCoreSettingsUri uri = SzCoreSettingsUri.parse(original);
        String result = uri.toString();

        assertTrue(result.contains("sz://core-settings/SQL/CONNECTION"),
            "Should preserve scheme and path");
        assertTrue(result.contains("param=value"),
            "Should preserve query parameters");
    }

    /**
     * Test resolveUri() with nested JSON objects.
     */
    @Test
    public void testResolveUriWithNestedObjects() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("LEVEL1", Json.createObjectBuilder()
                .add("LEVEL2", Json.createObjectBuilder()
                    .add("CONNECTION", "sqlite3://na:na@/tmp/testdb.db")))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("LEVEL1/LEVEL2/CONNECTION");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNotNull(resolved, "Should resolve nested path");
        assertTrue(resolved instanceof SQLiteUri,
            "Should resolve to SQLiteUri");
    }

    /**
     * Test resolveUri() returns null when intermediate path doesn't exist.
     */
    @Test
    public void testResolveUriWithMissingIntermediatePath() {
        JsonObject coreSettings = Json.createObjectBuilder()
            .add("SQL", Json.createObjectBuilder()
                .add("CONNECTION", "postgresql://user:pass@localhost:5432/G2"))
            .build();

        SzCoreSettingsUri uri = new SzCoreSettingsUri("NOSQL/CONNECTION");
        ConnectionUri resolved = uri.resolveUri(coreSettings);

        assertNull(resolved, "Should return null when intermediate path doesn't exist");
    }
}
