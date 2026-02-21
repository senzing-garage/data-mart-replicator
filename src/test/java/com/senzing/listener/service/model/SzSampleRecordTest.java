package com.senzing.listener.service.model;

import com.senzing.util.JsonUtilities;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Unit tests for {@link SzSampleRecord}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzSampleRecordTest {

    /**
     * Provides test parameters for property combinations.
     */
    public List<Arguments> getPropertyParameters() {
        List<Arguments> result = new LinkedList<>();
        result.add(arguments("CUSTOMERS", "REC001", Arrays.asList("FLAG1", "FLAG2")));
        result.add(arguments("EMPLOYEES", "REC002", Collections.singletonList("SINGLE_FLAG")));
        result.add(arguments("DATA_SOURCE", "RECORD_ID", Collections.emptyList()));
        result.add(arguments(null, null, null));
        result.add(arguments(null, "REC003", Arrays.asList("FLAG_A")));
        result.add(arguments("SOURCE", null, null));
        result.add(arguments("", "", Collections.emptyList()));
        result.add(arguments("DS", "ID", Arrays.asList("FLAG1", null, "FLAG2"))); // null in collection
        return result;
    }

    @Test
    public void defaultConstructTest() {
        SzSampleRecord record = new SzSampleRecord();
        assertNull(record.getDataSource(), "Data source should be null");
        assertNull(record.getRecordId(), "Record ID should be null");
        assertNotNull(record.getFlags(), "Flags should not be null");
        assertTrue(record.getFlags().isEmpty(), "Flags should be empty");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void constructTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);
        assertEquals(dataSource, record.getDataSource(), "Data source mismatch");
        assertEquals(recordId, record.getRecordId(), "Record ID mismatch");
        assertNotNull(record.getFlags(), "Flags should not be null");

        // Verify nulls removed from flags
        Set<String> expectedFlags = new LinkedHashSet<>();
        if (flags != null) {
            for (String flag : flags) {
                if (flag != null) {
                    expectedFlags.add(flag);
                }
            }
        }
        assertEquals(expectedFlags, record.getFlags(), "Flags mismatch");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void propertySetterTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord();

        record.setDataSource(dataSource);
        record.setRecordId(recordId);
        record.setFlags(flags);

        assertEquals(dataSource, record.getDataSource(), "Data source mismatch");
        assertEquals(recordId, record.getRecordId(), "Record ID mismatch");
        assertNotNull(record.getFlags(), "Flags should not be null");
    }

    @Test
    public void addFlagTest() {
        SzSampleRecord record = new SzSampleRecord();
        record.addFlag("FLAG1");
        record.addFlag("FLAG2");

        Set<String> flags = record.getFlags();
        assertEquals(2, flags.size(), "Should have 2 flags");
        assertTrue(flags.contains("FLAG1"), "Should contain FLAG1");
        assertTrue(flags.contains("FLAG2"), "Should contain FLAG2");
    }

    @Test
    public void addFlagNullTest() {
        SzSampleRecord record = new SzSampleRecord();
        assertThrows(NullPointerException.class, () -> record.addFlag(null),
                "Adding null flag should throw NullPointerException");
    }

    @Test
    public void flagsUnmodifiableTest() {
        SzSampleRecord record = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG1"));
        Set<String> flags = record.getFlags();
        assertThrows(UnsupportedOperationException.class, () -> flags.add("NEW_FLAG"),
                "Flags set should be unmodifiable");
    }

    @Test
    public void setFlagsNullClearsTest() {
        SzSampleRecord record = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG1", "FLAG2"));
        assertEquals(2, record.getFlags().size(), "Should have 2 flags initially");

        record.setFlags(null);
        assertTrue(record.getFlags().isEmpty(), "Flags should be empty after setting null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void equalsTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record1 = new SzSampleRecord(dataSource, recordId, flags);
        SzSampleRecord record2 = new SzSampleRecord(dataSource, recordId, flags);

        assertEquals(record1, record2, "Equivalent records should be equal");
        assertEquals(record1, record1, "Record should equal itself");
    }

    @Test
    public void notEqualsNullTest() {
        SzSampleRecord record = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG"));
        assertNotEquals(record, null, "Record should not equal null");
    }

    @Test
    public void notEqualsDifferentClassTest() {
        SzSampleRecord record = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG"));
        assertNotEquals(record, "not a record", "Record should not equal different class");
    }

    @Test
    public void notEqualsDifferentDataSourceTest() {
        SzSampleRecord record1 = new SzSampleRecord("DS1", "ID", Arrays.asList("FLAG"));
        SzSampleRecord record2 = new SzSampleRecord("DS2", "ID", Arrays.asList("FLAG"));
        assertNotEquals(record1, record2, "Records with different data sources should not be equal");
    }

    @Test
    public void notEqualsDifferentRecordIdTest() {
        SzSampleRecord record1 = new SzSampleRecord("DS", "ID1", Arrays.asList("FLAG"));
        SzSampleRecord record2 = new SzSampleRecord("DS", "ID2", Arrays.asList("FLAG"));
        assertNotEquals(record1, record2, "Records with different record IDs should not be equal");
    }

    @Test
    public void notEqualsDifferentFlagsTest() {
        SzSampleRecord record1 = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG1"));
        SzSampleRecord record2 = new SzSampleRecord("DS", "ID", Arrays.asList("FLAG2"));
        assertNotEquals(record1, record2, "Records with different flags should not be equal");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void hashCodeConsistencyTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record1 = new SzSampleRecord(dataSource, recordId, flags);
        SzSampleRecord record2 = new SzSampleRecord(dataSource, recordId, flags);

        if (record1.equals(record2)) {
            assertEquals(record1.hashCode(), record2.hashCode(),
                    "Equal objects must have equal hash codes");
        }
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toStringTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);
        assertDoesNotThrow(() -> record.toString(),
                "toString should not throw exception");
        assertNotNull(record.toString(), "toString should not return null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toJsonTextTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);

        String jsonText = record.toJsonText();
        assertNotNull(jsonText, "JSON text should not be null");

        // Parse back and verify
        JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
        assertNotNull(jsonObject, "Parsed JSON should not be null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toJsonTextPrettyPrintTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);

        String jsonText = record.toJsonText(true);
        assertNotNull(jsonText, "Pretty-printed JSON text should not be null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toJsonObjectTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);

        JsonObject jsonObject = record.toJsonObject();
        assertNotNull(jsonObject, "JSON object should not be null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toJsonObjectBuilderWithNullTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);

        JsonObjectBuilder builder = record.toJsonObjectBuilder(null);
        assertNotNull(builder, "Builder should not be null");
        assertNotNull(builder.build(), "Built JSON should not be null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toJsonObjectBuilderWithExistingTest(String dataSource, String recordId, List<String> flags) {
        SzSampleRecord record = new SzSampleRecord(dataSource, recordId, flags);

        JsonObjectBuilder existingBuilder = Json.createObjectBuilder();
        existingBuilder.add("existingKey", "existingValue");

        JsonObjectBuilder builder = record.toJsonObjectBuilder(existingBuilder);
        JsonObject result = builder.build();

        assertTrue(result.containsKey("existingKey"), "Should retain existing key");
        assertEquals("existingValue", result.getString("existingKey"));
    }

    @Test
    public void fromJsonNullTextTest() {
        SzSampleRecord record = SzSampleRecord.fromJson((String) null);
        assertNull(record, "fromJson with null text should return null");
    }

    @Test
    public void fromJsonNullObjectTest() {
        SzSampleRecord record = SzSampleRecord.fromJson((JsonObject) null);
        assertNull(record, "fromJson with null object should return null");
    }

    @Test
    public void fromRawJsonNullTextTest() {
        SzSampleRecord record = SzSampleRecord.fromRawJson((String) null);
        assertNull(record, "fromRawJson with null text should return null");
    }

    @Test
    public void fromRawJsonNullObjectTest() {
        SzSampleRecord record = SzSampleRecord.fromRawJson((JsonObject) null);
        assertNull(record, "fromRawJson with null object should return null");
    }

    @Test
    public void fromJsonMissingPropertiesTest() {
        JsonObject jsonObject = Json.createObjectBuilder()
                .add("someOtherKey", "value")
                .build();

        assertThrows(IllegalArgumentException.class,
                () -> SzSampleRecord.fromJson(jsonObject),
                "Should throw when required properties missing");
    }

    @Test
    public void fromJsonRoundTripTest() {
        SzSampleRecord original = new SzSampleRecord("CUSTOMERS", "REC001",
                Arrays.asList("FLAG1", "FLAG2"));

        String jsonText = original.toJsonText();
        SzSampleRecord parsed = SzSampleRecord.fromJson(jsonText);

        assertEquals(original, parsed, "Round-trip should produce equal object");
    }

    @Test
    public void fromJsonObjectRoundTripTest() {
        SzSampleRecord original = new SzSampleRecord("CUSTOMERS", "REC001",
                Arrays.asList("FLAG1", "FLAG2"));

        JsonObject jsonObject = original.toJsonObject();
        SzSampleRecord parsed = SzSampleRecord.fromJson(jsonObject);

        assertEquals(original, parsed, "Round-trip should produce equal object");
    }

    @Test
    public void fromRawJsonTextTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzSampleRecord.RAW_DATA_SOURCE_KEY, "CUSTOMERS");
        JsonUtilities.add(job, SzSampleRecord.RAW_RECORD_ID_KEY, "REC001");

        JsonArrayBuilder jab = Json.createArrayBuilder();
        jab.add("FLAG1");
        jab.add("FLAG2");
        job.add(SzSampleRecord.RAW_FLAGS_KEY, jab);

        String jsonText = JsonUtilities.toJsonText(job.build());
        SzSampleRecord record = SzSampleRecord.fromRawJson(jsonText);

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("REC001", record.getRecordId());
        assertEquals(2, record.getFlags().size());
    }

    @Test
    public void fromRawJsonObjectTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzSampleRecord.RAW_DATA_SOURCE_KEY, "EMPLOYEES");
        JsonUtilities.add(job, SzSampleRecord.RAW_RECORD_ID_KEY, "EMP001");

        JsonArrayBuilder jab = Json.createArrayBuilder();
        jab.add("RESOLVED");
        job.add(SzSampleRecord.RAW_FLAGS_KEY, jab);

        SzSampleRecord record = SzSampleRecord.fromRawJson(job.build());

        assertEquals("EMPLOYEES", record.getDataSource());
        assertEquals("EMP001", record.getRecordId());
        assertTrue(record.getFlags().contains("RESOLVED"));
    }

    /**
     * Tests parsing raw JSON with absent FLAGS key.
     * The absent key should be treated as an empty set.
     */
    @Test
    public void fromRawJsonWithFlagsAbsentTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "CUST-001"
            }
            """;

        SzSampleRecord record = SzSampleRecord.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", record.getDataSource());
        assertEquals("CUST-001", record.getRecordId());
        assertTrue(record.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON with absent flags key.
     */
    @Test
    public void fromJsonWithFlagsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzSampleRecord.DATA_SOURCE_KEY, "EMPLOYEES");
        JsonUtilities.add(job, SzSampleRecord.RECORD_ID_KEY, "EMP-001");

        SzSampleRecord record = SzSampleRecord.fromJson(job.build());

        assertEquals("EMPLOYEES", record.getDataSource());
        assertEquals("EMP-001", record.getRecordId());
        assertTrue(record.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
    }

    /**
     * Tests parsing raw JSON text with absent FLAGS key.
     */
    @Test
    public void fromRawJsonTextWithFlagsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add(SzSampleRecord.RAW_DATA_SOURCE_KEY, "VENDORS");
        job.add(SzSampleRecord.RAW_RECORD_ID_KEY, "V-100");

        String jsonText = JsonUtilities.toJsonText(job.build());
        SzSampleRecord record = SzSampleRecord.fromRawJson(jsonText);

        assertEquals("VENDORS", record.getDataSource());
        assertEquals("V-100", record.getRecordId());
        assertTrue(record.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON text with absent flags key.
     */
    @Test
    public void fromJsonTextWithFlagsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzSampleRecord.DATA_SOURCE_KEY, "WATCHLIST");
        JsonUtilities.add(job, SzSampleRecord.RECORD_ID_KEY, "WL-001");

        String jsonText = JsonUtilities.toJsonText(job.build());
        SzSampleRecord record = SzSampleRecord.fromJson(jsonText);

        assertEquals("WATCHLIST", record.getDataSource());
        assertEquals("WL-001", record.getRecordId());
        assertTrue(record.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
    }
}
