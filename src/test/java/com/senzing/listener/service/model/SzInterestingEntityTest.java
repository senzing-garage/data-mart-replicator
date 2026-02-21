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
 * Unit tests for {@link SzInterestingEntity}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzInterestingEntityTest {

    /**
     * Creates a sample record for testing.
     */
    private SzSampleRecord createSampleRecord(String dataSource, String recordId) {
        return new SzSampleRecord(dataSource, recordId, Arrays.asList("FLAG"));
    }

    /**
     * Provides test parameters for property combinations.
     */
    public List<Arguments> getPropertyParameters() {
        List<Arguments> result = new LinkedList<>();

        // entityId, degrees, flags, sampleRecords
        result.add(arguments(100L, 1, Arrays.asList("FLAG1", "FLAG2"),
                Arrays.asList(new SzSampleRecord("DS1", "R1", Arrays.asList("F1")))));
        result.add(arguments(200L, 2, Collections.singletonList("RESOLVED"),
                Arrays.asList(
                        new SzSampleRecord("DS1", "R1", Arrays.asList("F1")),
                        new SzSampleRecord("DS2", "R2", Arrays.asList("F2")))));
        result.add(arguments(300L, 0, Collections.emptyList(), Collections.emptyList()));
        result.add(arguments(null, null, null, null));
        result.add(arguments(null, 1, Arrays.asList("FLAG"), null));
        result.add(arguments(400L, null, null, Collections.emptyList()));
        result.add(arguments(500L, 3, Arrays.asList("A", null, "B"),
                Arrays.asList(new SzSampleRecord("DS", "R", null), null)));
        return result;
    }

    @Test
    public void defaultConstructTest() {
        SzInterestingEntity entity = new SzInterestingEntity();
        assertNull(entity.getEntityId(), "Entity ID should be null");
        assertNull(entity.getDegrees(), "Degrees should be null");
        assertNotNull(entity.getFlags(), "Flags should not be null");
        assertTrue(entity.getFlags().isEmpty(), "Flags should be empty");
        assertNotNull(entity.getSampleRecords(), "Sample records should not be null");
        assertTrue(entity.getSampleRecords().isEmpty(), "Sample records should be empty");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void constructTest(Long entityId, Integer degrees, List<String> flags,
                              List<SzSampleRecord> sampleRecords) {
        SzInterestingEntity entity = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);

        assertEquals(entityId, entity.getEntityId(), "Entity ID mismatch");
        assertEquals(degrees, entity.getDegrees(), "Degrees mismatch");
        assertNotNull(entity.getFlags(), "Flags should not be null");
        assertNotNull(entity.getSampleRecords(), "Sample records should not be null");

        // Verify nulls removed from flags
        if (flags != null) {
            for (String flag : entity.getFlags()) {
                assertNotNull(flag, "Null flags should be removed");
            }
        }

        // Verify nulls removed from sample records
        if (sampleRecords != null) {
            for (SzSampleRecord record : entity.getSampleRecords()) {
                assertNotNull(record, "Null sample records should be removed");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void propertySetterTest(Long entityId, Integer degrees, List<String> flags,
                                   List<SzSampleRecord> sampleRecords) {
        SzInterestingEntity entity = new SzInterestingEntity();

        entity.setEntityId(entityId);
        entity.setDegrees(degrees);
        entity.setFlags(flags);
        entity.setSampleRecords(sampleRecords);

        assertEquals(entityId, entity.getEntityId(), "Entity ID mismatch");
        assertEquals(degrees, entity.getDegrees(), "Degrees mismatch");
        assertNotNull(entity.getFlags(), "Flags should not be null");
        assertNotNull(entity.getSampleRecords(), "Sample records should not be null");
    }

    @Test
    public void addFlagTest() {
        SzInterestingEntity entity = new SzInterestingEntity();
        entity.addFlag("FLAG1");
        entity.addFlag("FLAG2");

        Set<String> flags = entity.getFlags();
        assertEquals(2, flags.size(), "Should have 2 flags");
        assertTrue(flags.contains("FLAG1"), "Should contain FLAG1");
        assertTrue(flags.contains("FLAG2"), "Should contain FLAG2");
    }

    @Test
    public void addFlagNullTest() {
        SzInterestingEntity entity = new SzInterestingEntity();
        assertThrows(NullPointerException.class, () -> entity.addFlag(null),
                "Adding null flag should throw NullPointerException");
    }

    @Test
    public void addSampleRecordTest() {
        SzInterestingEntity entity = new SzInterestingEntity();
        SzSampleRecord record1 = createSampleRecord("DS1", "R1");
        SzSampleRecord record2 = createSampleRecord("DS2", "R2");

        entity.addSampleRecord(record1);
        entity.addSampleRecord(record2);

        List<SzSampleRecord> records = entity.getSampleRecords();
        assertEquals(2, records.size(), "Should have 2 sample records");
        assertEquals(record1, records.get(0), "First record mismatch");
        assertEquals(record2, records.get(1), "Second record mismatch");
    }

    @Test
    public void addSampleRecordNullTest() {
        SzInterestingEntity entity = new SzInterestingEntity();
        assertThrows(NullPointerException.class, () -> entity.addSampleRecord(null),
                "Adding null sample record should throw NullPointerException");
    }

    @Test
    public void flagsUnmodifiableTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1,
                Arrays.asList("FLAG"), null);
        Set<String> flags = entity.getFlags();
        assertThrows(UnsupportedOperationException.class, () -> flags.add("NEW_FLAG"),
                "Flags set should be unmodifiable");
    }

    @Test
    public void sampleRecordsUnmodifiableTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1, null,
                Arrays.asList(createSampleRecord("DS", "R")));
        List<SzSampleRecord> records = entity.getSampleRecords();
        assertThrows(UnsupportedOperationException.class,
                () -> records.add(createSampleRecord("DS2", "R2")),
                "Sample records list should be unmodifiable");
    }

    @Test
    public void setFlagsNullClearsTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1,
                Arrays.asList("FLAG1", "FLAG2"), null);
        assertEquals(2, entity.getFlags().size(), "Should have 2 flags initially");

        entity.setFlags(null);
        assertTrue(entity.getFlags().isEmpty(), "Flags should be empty after setting null");
    }

    @Test
    public void setSampleRecordsNullClearsTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1, null,
                Arrays.asList(createSampleRecord("DS", "R")));
        assertEquals(1, entity.getSampleRecords().size(), "Should have 1 record initially");

        entity.setSampleRecords(null);
        assertTrue(entity.getSampleRecords().isEmpty(),
                "Sample records should be empty after setting null");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void equalsTest(Long entityId, Integer degrees, List<String> flags,
                           List<SzSampleRecord> sampleRecords) {
        SzInterestingEntity entity1 = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);
        SzInterestingEntity entity2 = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);

        assertEquals(entity1, entity2, "Equivalent entities should be equal");
        assertEquals(entity1, entity1, "Entity should equal itself");
    }

    @Test
    public void notEqualsNullTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1,
                Arrays.asList("FLAG"), null);
        assertNotEquals(entity, null, "Entity should not equal null");
    }

    @Test
    public void notEqualsDifferentClassTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1,
                Arrays.asList("FLAG"), null);
        assertNotEquals(entity, "not an entity", "Entity should not equal different class");
    }

    @Test
    public void notEqualsDifferentEntityIdTest() {
        SzInterestingEntity entity1 = new SzInterestingEntity(100L, 1, null, null);
        SzInterestingEntity entity2 = new SzInterestingEntity(200L, 1, null, null);
        assertNotEquals(entity1, entity2, "Entities with different IDs should not be equal");
    }

    @Test
    public void notEqualsDifferentDegreesTest() {
        SzInterestingEntity entity1 = new SzInterestingEntity(100L, 1, null, null);
        SzInterestingEntity entity2 = new SzInterestingEntity(100L, 2, null, null);
        assertNotEquals(entity1, entity2, "Entities with different degrees should not be equal");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void hashCodeConsistencyTest(Long entityId, Integer degrees, List<String> flags,
                                        List<SzSampleRecord> sampleRecords) {
        SzInterestingEntity entity1 = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);
        SzInterestingEntity entity2 = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);

        if (entity1.equals(entity2)) {
            assertEquals(entity1.hashCode(), entity2.hashCode(),
                    "Equal objects must have equal hash codes");
        }
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toStringTest(Long entityId, Integer degrees, List<String> flags,
                             List<SzSampleRecord> sampleRecords) {
        SzInterestingEntity entity = new SzInterestingEntity(entityId, degrees, flags, sampleRecords);
        assertDoesNotThrow(() -> entity.toString(),
                "toString should not throw exception");
        assertNotNull(entity.toString(), "toString should not return null");
    }

    @Test
    public void toJsonTextTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 2,
                Arrays.asList("FLAG1", "FLAG2"),
                Arrays.asList(createSampleRecord("DS", "R")));

        String jsonText = entity.toJsonText();
        assertNotNull(jsonText, "JSON text should not be null");

        JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
        assertNotNull(jsonObject, "Parsed JSON should not be null");
    }

    @Test
    public void toJsonTextPrettyPrintTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1, null, null);

        String jsonText = entity.toJsonText(true);
        assertNotNull(jsonText, "Pretty-printed JSON text should not be null");
    }

    @Test
    public void toJsonObjectTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1,
                Arrays.asList("FLAG"), Arrays.asList(createSampleRecord("DS", "R")));

        JsonObject jsonObject = entity.toJsonObject();
        assertNotNull(jsonObject, "JSON object should not be null");
    }

    @Test
    public void toJsonObjectBuilderWithNullTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1, null, null);

        JsonObjectBuilder builder = entity.toJsonObjectBuilder(null);
        assertNotNull(builder, "Builder should not be null");
        assertNotNull(builder.build(), "Built JSON should not be null");
    }

    @Test
    public void toJsonObjectBuilderWithExistingTest() {
        SzInterestingEntity entity = new SzInterestingEntity(100L, 1, null, null);

        JsonObjectBuilder existingBuilder = Json.createObjectBuilder();
        existingBuilder.add("existingKey", "existingValue");

        JsonObjectBuilder builder = entity.toJsonObjectBuilder(existingBuilder);
        JsonObject result = builder.build();

        assertTrue(result.containsKey("existingKey"), "Should retain existing key");
    }

    @Test
    public void fromJsonNullTextTest() {
        SzInterestingEntity entity = SzInterestingEntity.fromJson((String) null);
        assertNull(entity, "fromJson with null text should return null");
    }

    @Test
    public void fromJsonNullObjectTest() {
        SzInterestingEntity entity = SzInterestingEntity.fromJson((JsonObject) null);
        assertNull(entity, "fromJson with null object should return null");
    }

    @Test
    public void fromRawJsonNullTextTest() {
        SzInterestingEntity entity = SzInterestingEntity.fromRawJson((String) null);
        assertNull(entity, "fromRawJson with null text should return null");
    }

    @Test
    public void fromRawJsonNullObjectTest() {
        SzInterestingEntity entity = SzInterestingEntity.fromRawJson((JsonObject) null);
        assertNull(entity, "fromRawJson with null object should return null");
    }

    @Test
    public void fromJsonMissingPropertiesTest() {
        JsonObject jsonObject = Json.createObjectBuilder()
                .add("someOtherKey", "value")
                .build();

        assertThrows(IllegalArgumentException.class,
                () -> SzInterestingEntity.fromJson(jsonObject),
                "Should throw when required properties missing");
    }

    @Test
    public void fromJsonRoundTripTest() {
        SzInterestingEntity original = new SzInterestingEntity(100L, 2,
                Arrays.asList("FLAG1", "FLAG2"),
                Arrays.asList(createSampleRecord("DS", "R1")));

        String jsonText = original.toJsonText();
        SzInterestingEntity parsed = SzInterestingEntity.fromJson(jsonText);

        assertEquals(original, parsed, "Round-trip should produce equal object");
    }

    @Test
    public void fromJsonObjectRoundTripTest() {
        SzInterestingEntity original = new SzInterestingEntity(200L, 1,
                Arrays.asList("RESOLVED"),
                Arrays.asList(createSampleRecord("CUSTOMERS", "C001")));

        JsonObject jsonObject = original.toJsonObject();
        SzInterestingEntity parsed = SzInterestingEntity.fromJson(jsonObject);

        assertEquals(original, parsed, "Round-trip should produce equal object");
    }

    @Test
    public void fromRawJsonTextTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInterestingEntity.RAW_ENTITY_ID_KEY, 100L);
        JsonUtilities.add(job, SzInterestingEntity.RAW_DEGREES_KEY, 1);

        JsonArrayBuilder flagsBuilder = Json.createArrayBuilder();
        flagsBuilder.add("FLAG1");
        job.add(SzInterestingEntity.RAW_FLAGS_KEY, flagsBuilder);

        JsonArrayBuilder recordsBuilder = Json.createArrayBuilder();
        JsonObjectBuilder recordJob = Json.createObjectBuilder();
        JsonUtilities.add(recordJob, SzSampleRecord.RAW_DATA_SOURCE_KEY, "DS");
        JsonUtilities.add(recordJob, SzSampleRecord.RAW_RECORD_ID_KEY, "R1");
        recordJob.add(SzSampleRecord.RAW_FLAGS_KEY, Json.createArrayBuilder());
        recordsBuilder.add(recordJob);
        job.add(SzInterestingEntity.RAW_SAMPLE_RECORDS_KEY, recordsBuilder);

        String jsonText = JsonUtilities.toJsonText(job.build());
        SzInterestingEntity entity = SzInterestingEntity.fromRawJson(jsonText);

        assertEquals(Long.valueOf(100L), entity.getEntityId());
        assertEquals(Integer.valueOf(1), entity.getDegrees());
        assertTrue(entity.getFlags().contains("FLAG1"));
    }

    /**
     * Tests parsing raw JSON with absent FLAGS key.
     * The absent key should be treated as an empty set.
     */
    @Test
    public void fromRawJsonWithFlagsAbsentTest() {
        String rawJson = """
            {
                "ENTITY_ID": 100,
                "DEGREES": 1,
                "SAMPLE_RECORDS": []
            }
            """;

        SzInterestingEntity entity = SzInterestingEntity.fromRawJson(rawJson);

        assertEquals(Long.valueOf(100L), entity.getEntityId());
        assertEquals(Integer.valueOf(1), entity.getDegrees());
        assertTrue(entity.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
        assertTrue(entity.getSampleRecords().isEmpty());
    }

    /**
     * Tests parsing raw JSON with absent SAMPLE_RECORDS key.
     * The absent key should be treated as an empty list.
     */
    @Test
    public void fromRawJsonWithSampleRecordsAbsentTest() {
        String rawJson = """
            {
                "ENTITY_ID": 200,
                "DEGREES": 2,
                "FLAGS": ["RESOLVED"]
            }
            """;

        SzInterestingEntity entity = SzInterestingEntity.fromRawJson(rawJson);

        assertEquals(Long.valueOf(200L), entity.getEntityId());
        assertEquals(Integer.valueOf(2), entity.getDegrees());
        assertTrue(entity.getFlags().contains("RESOLVED"));
        assertTrue(entity.getSampleRecords().isEmpty(),
                "Sample records should be empty when key is absent");
    }

    /**
     * Tests parsing raw JSON with both FLAGS and SAMPLE_RECORDS keys absent.
     * Both should be treated as empty collections.
     */
    @Test
    public void fromRawJsonWithBothFlagsAndSampleRecordsAbsentTest() {
        String rawJson = """
            {
                "ENTITY_ID": 300,
                "DEGREES": 0
            }
            """;

        SzInterestingEntity entity = SzInterestingEntity.fromRawJson(rawJson);

        assertEquals(Long.valueOf(300L), entity.getEntityId());
        assertEquals(Integer.valueOf(0), entity.getDegrees());
        assertTrue(entity.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
        assertTrue(entity.getSampleRecords().isEmpty(),
                "Sample records should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON with absent flags key.
     */
    @Test
    public void fromJsonWithFlagsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInterestingEntity.ENTITY_ID_KEY, 100L);
        JsonUtilities.add(job, SzInterestingEntity.DEGREES_KEY, 1);
        job.add(SzInterestingEntity.SAMPLE_RECORDS_KEY, Json.createArrayBuilder());

        SzInterestingEntity entity = SzInterestingEntity.fromJson(job.build());

        assertEquals(Long.valueOf(100L), entity.getEntityId());
        assertEquals(Integer.valueOf(1), entity.getDegrees());
        assertTrue(entity.getFlags().isEmpty(),
                "Flags should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON with absent sampleRecords key.
     */
    @Test
    public void fromJsonWithSampleRecordsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInterestingEntity.ENTITY_ID_KEY, 200L);
        JsonUtilities.add(job, SzInterestingEntity.DEGREES_KEY, 2);
        job.add(SzInterestingEntity.FLAGS_KEY, Json.createArrayBuilder().add("FLAG"));

        SzInterestingEntity entity = SzInterestingEntity.fromJson(job.build());

        assertEquals(Long.valueOf(200L), entity.getEntityId());
        assertEquals(Integer.valueOf(2), entity.getDegrees());
        assertTrue(entity.getFlags().contains("FLAG"));
        assertTrue(entity.getSampleRecords().isEmpty(),
                "Sample records should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON with both flags and sampleRecords keys absent.
     */
    @Test
    public void fromJsonWithBothFlagsAndSampleRecordsAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInterestingEntity.ENTITY_ID_KEY, 300L);
        JsonUtilities.add(job, SzInterestingEntity.DEGREES_KEY, 0);

        SzInterestingEntity entity = SzInterestingEntity.fromJson(job.build());

        assertEquals(Long.valueOf(300L), entity.getEntityId());
        assertEquals(Integer.valueOf(0), entity.getDegrees());
        assertTrue(entity.getFlags().isEmpty());
        assertTrue(entity.getSampleRecords().isEmpty());
    }
}
