package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SzRecordKeyTest {

    @Test
    void testConstructorAndGetters() {
        String dataSource = "CUSTOMERS";
        String recordId = "REC-001";

        SzRecordKey key = new SzRecordKey(dataSource, recordId);

        assertEquals(dataSource, key.getDataSource());
        assertEquals(recordId, key.getRecordId());
    }

    @Test
    void testConstructorWithNullDataSource() {
        SzRecordKey key = new SzRecordKey(null, "REC-001");
        assertNull(key.getDataSource());
        assertEquals("REC-001", key.getRecordId());
    }

    @Test
    void testConstructorWithNullRecordId() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", null);
        assertEquals("CUSTOMERS", key.getDataSource());
        assertNull(key.getRecordId());
    }

    @Test
    void testEqualsWithSameReference() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        assertEquals(key, key);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("CUSTOMERS", "REC-001");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("VENDORS", "REC-001");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentRecordId() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("CUSTOMERS", "REC-002");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithNull() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        assertNotEquals(null, key);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        String other = "CUSTOMERS:REC-001";
        assertNotEquals(key, other);
    }

    @Test
    void testHashCodeConsistency() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("CUSTOMERS", "REC-001");

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testHashCodeDifferentForDifferentObjects() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("VENDORS", "REC-002");

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testCompareToWithNull() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        assertTrue(key.compareTo(null) < 0);
    }

    @Test
    void testCompareToEqual() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecordKey key2 = new SzRecordKey("CUSTOMERS", "REC-001");

        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    void testCompareToDifferentDataSource() {
        SzRecordKey key1 = new SzRecordKey("AAA", "REC-001");
        SzRecordKey key2 = new SzRecordKey("ZZZ", "REC-001");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    void testCompareToDifferentRecordId() {
        SzRecordKey key1 = new SzRecordKey("CUSTOMERS", "AAA");
        SzRecordKey key2 = new SzRecordKey("CUSTOMERS", "ZZZ");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    void testBuildJson() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        JsonObjectBuilder builder = Json.createObjectBuilder();
        key.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals("CUSTOMERS", json.getString("src"));
        assertEquals("REC-001", json.getString("id"));
    }

    @Test
    void testToJsonObject() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        JsonObject json = key.toJsonObject();

        assertEquals("CUSTOMERS", json.getString("src"));
        assertEquals("REC-001", json.getString("id"));
    }

    @Test
    void testToJsonText() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        String jsonText = key.toJsonText();

        assertNotNull(jsonText);
        assertTrue(jsonText.contains("CUSTOMERS"));
        assertTrue(jsonText.contains("REC-001"));
    }

    @Test
    void testToJsonTextPrettyPrint() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        String jsonTextPretty = key.toJsonText(true);
        String jsonTextCompact = key.toJsonText(false);

        assertNotNull(jsonTextPretty);
        assertNotNull(jsonTextCompact);
        assertTrue(jsonTextPretty.contains("CUSTOMERS"));
    }

    @Test
    void testToString() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        String result = key.toString();

        assertNotNull(result);
        assertEquals(key.toJsonText(), result);
    }

    @ParameterizedTest
    @MethodSource("provideParseTestCases")
    void testParse(String srcKey, String srcValue, String idKey, String idValue) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add(srcKey, srcValue);
        builder.add(idKey, idValue);
        JsonObject json = builder.build();

        SzRecordKey key = SzRecordKey.parse(json);

        assertEquals(srcValue, key.getDataSource());
        assertEquals(idValue, key.getRecordId());
    }

    static Stream<Arguments> provideParseTestCases() {
        return Stream.of(
            Arguments.of("src", "DS1", "id", "R1"),
            Arguments.of("dataSourceCode", "DS2", "recordId", "R2"),
            Arguments.of("DATA_SOURCE", "DS3", "RECORD_ID", "R3")
        );
    }

    @Test
    void testParseWithMissingSrc() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", "REC-001");
        JsonObject json = builder.build();

        assertThrows(IllegalArgumentException.class, () -> SzRecordKey.parse(json));
    }

    @Test
    void testParseWithMissingId() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("src", "CUSTOMERS");
        JsonObject json = builder.build();

        assertThrows(IllegalArgumentException.class, () -> SzRecordKey.parse(json));
    }

    @Test
    void testToKey() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        com.senzing.sdk.SzRecordKey sdkKey = key.toKey();

        assertNotNull(sdkKey);
        assertEquals("CUSTOMERS", sdkKey.dataSourceCode());
        assertEquals("REC-001", sdkKey.recordId());
    }

    @Test
    void testRoundTripThroughJson() {
        SzRecordKey original = new SzRecordKey("CUSTOMERS", "REC-001");
        JsonObject json = original.toJsonObject();
        SzRecordKey parsed = SzRecordKey.parse(json);

        assertEquals(original, parsed);
    }
}
