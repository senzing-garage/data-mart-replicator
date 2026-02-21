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

class SzRecordTest {

    @Test
    void testConstructorWithDataSourceAndRecordId() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
        assertEquals("NAME+DOB", record.getMatchKey());
        assertEquals("MFF", record.getPrinciple());
    }

    @Test
    void testConstructorWithRecordKey() {
        SzRecordKey key = new SzRecordKey("CUSTOMERS", "REC-001");
        SzRecord record = new SzRecord(key, "NAME+DOB", "MFF");

        assertEquals(key, record.getRecordKey());
        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
        assertEquals("NAME+DOB", record.getMatchKey());
        assertEquals("MFF", record.getPrinciple());
    }

    @Test
    void testConstructorWithNullRecordKey() {
        assertThrows(NullPointerException.class, () ->
            new SzRecord(null, "NAME+DOB", "MFF"));
    }

    @Test
    void testConstructorWithNullMatchKey() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", null, "MFF");
        assertNull(record.getMatchKey());
        assertEquals("MFF", record.getPrinciple());
    }

    @Test
    void testConstructorWithNullPrinciple() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", null);
        assertEquals("NAME+DOB", record.getMatchKey());
        assertNull(record.getPrinciple());
    }

    @Test
    void testConstructorNormalizesEmptyMatchKey() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "   ", "MFF");
        assertNull(record.getMatchKey());
    }

    @Test
    void testConstructorNormalizesEmptyPrinciple() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "   ");
        assertNull(record.getPrinciple());
    }

    @Test
    void testConstructorTrimsMatchKey() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "  NAME+DOB  ", "MFF");
        assertEquals("NAME+DOB", record.getMatchKey());
    }

    @Test
    void testConstructorTrimsPrinciple() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "  MFF  ");
        assertEquals("MFF", record.getPrinciple());
    }

    @Test
    void testGetRecordKey() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecordKey key = record.getRecordKey();

        assertNotNull(key);
        assertEquals("CUSTOMERS", key.getDataSource());
        assertEquals("REC-001", key.getRecordId());
    }

    @Test
    void testEqualsWithSameReference() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        assertEquals(record, record);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecord record2 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");

        assertEquals(record1, record2);
        assertEquals(record2, record1);
    }

    @Test
    void testEqualsWithDifferentRecordKey() {
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecord record2 = new SzRecord("VENDORS", "REC-001", "NAME+DOB", "MFF");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecord record2 = new SzRecord("CUSTOMERS", "REC-001", "ADDRESS", "MFF");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecord record2 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFS");

        assertNotEquals(record1, record2);
    }

    @Test
    void testEqualsWithNull() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        assertNotEquals(null, record);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        String other = "CUSTOMERS:REC-001";
        assertNotEquals(record, other);
    }

    @Test
    void testHashCodeConsistency() {
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        SzRecord record2 = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");

        assertEquals(record1.hashCode(), record2.hashCode());
    }

    @Test
    void testBuildJson() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        JsonObjectBuilder builder = Json.createObjectBuilder();
        record.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals("CUSTOMERS", json.getString("src"));
        assertEquals("REC-001", json.getString("id"));
        assertEquals("NAME+DOB", json.getString("mkey"));
        assertEquals("MFF", json.getString("rule"));
    }

    @Test
    void testBuildJsonWithNullMatchKeyAndPrinciple() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", null, null);
        JsonObjectBuilder builder = Json.createObjectBuilder();
        record.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals("CUSTOMERS", json.getString("src"));
        assertEquals("REC-001", json.getString("id"));
        assertFalse(json.containsKey("mkey"));
        assertFalse(json.containsKey("rule"));
    }

    @Test
    void testToJsonObject() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        JsonObject json = record.toJsonObject();

        assertNotNull(json);
        assertEquals("CUSTOMERS", json.getString("src"));
    }

    @Test
    void testToJsonText() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        String jsonText = record.toJsonText();

        assertNotNull(jsonText);
        assertTrue(jsonText.contains("CUSTOMERS"));
    }

    @Test
    void testToJsonTextPrettyPrint() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        String jsonTextPretty = record.toJsonText(true);

        assertNotNull(jsonTextPretty);
    }

    @Test
    void testToString() {
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        String result = record.toString();

        assertNotNull(result);
        assertEquals(record.toJsonText(), result);
    }

    @ParameterizedTest
    @MethodSource("provideParseTestCases")
    void testParse(String mkeyField, String ruleField) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("src", "CUSTOMERS");
        builder.add("id", "REC-001");
        builder.add(mkeyField, "NAME+DOB");
        builder.add(ruleField, "MFF");
        JsonObject json = builder.build();

        SzRecord record = SzRecord.parse(json);

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
        assertEquals("NAME+DOB", record.getMatchKey());
        assertEquals("MFF", record.getPrinciple());
    }

    static Stream<Arguments> provideParseTestCases() {
        return Stream.of(
            Arguments.of("mkey", "rule"),
            Arguments.of("matchKey", "principle"),
            Arguments.of("MATCH_KEY", "ERRULE_CODE")
        );
    }

    @Test
    void testParseWithNoMatchKeyOrPrinciple() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("src", "CUSTOMERS");
        builder.add("id", "REC-001");
        JsonObject json = builder.build();

        SzRecord record = SzRecord.parse(json);

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC-001", record.getRecordId());
        assertNull(record.getMatchKey());
        assertNull(record.getPrinciple());
    }

    @Test
    void testRoundTripThroughJson() {
        SzRecord original = new SzRecord("CUSTOMERS", "REC-001", "NAME+DOB", "MFF");
        JsonObject json = original.toJsonObject();
        SzRecord parsed = SzRecord.parse(json);

        assertEquals(original, parsed);
    }

    @Test
    void testRoundTripWithNullOptionalFields() {
        SzRecord original = new SzRecord("CUSTOMERS", "REC-001", null, null);
        JsonObject json = original.toJsonObject();
        SzRecord parsed = SzRecord.parse(json);

        assertEquals(original, parsed);
    }
}
