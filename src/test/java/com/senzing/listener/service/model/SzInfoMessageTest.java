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
 * Unit tests for {@link SzInfoMessage}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SzInfoMessageTest {

    /**
     * Creates a sample SzInterestingEntity for testing.
     */
    private SzInterestingEntity createInterestingEntity(Long entityId) {
        return new SzInterestingEntity(entityId, 1, Arrays.asList("FLAG"),
                Arrays.asList(new SzSampleRecord("DS", "R" + entityId, Arrays.asList("F"))));
    }

    /**
     * Creates a sample SzNotice for testing.
     */
    private SzNotice createNotice(String code) {
        return new SzNotice(code, code + " description");
    }

    /**
     * Provides test parameters for property combinations.
     */
    public List<Arguments> getPropertyParameters() {
        List<Arguments> result = new LinkedList<>();

        // dataSource, recordId, affectedEntities, interestingEntities, notices
        result.add(arguments("CUSTOMERS", "REC001",
                Arrays.asList(100L, 200L),
                Arrays.asList(new SzInterestingEntity(100L, 1, Arrays.asList("FLAG"),
                        Arrays.asList(new SzSampleRecord("DS", "R1", Arrays.asList("F"))))),
                Arrays.asList(new SzNotice("CODE1", "Description1"))));

        result.add(arguments("EMPLOYEES", "EMP001",
                Collections.singletonList(300L),
                Collections.emptyList(),
                Collections.emptyList()));

        result.add(arguments(null, null, null, null, null));

        result.add(arguments("DS", "ID",
                Collections.emptyList(),
                null,
                Arrays.asList(new SzNotice("NOTICE", "Desc"))));

        result.add(arguments("", "", Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList()));

        // With nulls in collections
        result.add(arguments("DS", "ID",
                Arrays.asList(100L, null, 200L),
                Arrays.asList(new SzInterestingEntity(100L, 1, null, null), null),
                Arrays.asList(new SzNotice("CODE", "Desc"), null)));

        return result;
    }

    @Test
    public void defaultConstructTest() {
        SzInfoMessage msg = new SzInfoMessage();
        assertNull(msg.getDataSource(), "Data source should be null");
        assertNull(msg.getRecordId(), "Record ID should be null");
        assertNotNull(msg.getAffectedEntities(), "Affected entities should not be null");
        assertTrue(msg.getAffectedEntities().isEmpty(), "Affected entities should be empty");
        assertNotNull(msg.getInterestingEntities(), "Interesting entities should not be null");
        assertTrue(msg.getInterestingEntities().isEmpty(), "Interesting entities should be empty");
        assertNotNull(msg.getNotices(), "Notices should not be null");
        assertTrue(msg.getNotices().isEmpty(), "Notices should be empty");
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void constructTest(String dataSource, String recordId,
                              List<Long> affectedEntities,
                              List<SzInterestingEntity> interestingEntities,
                              List<SzNotice> notices) {
        SzInfoMessage msg = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);

        assertEquals(dataSource, msg.getDataSource(), "Data source mismatch");
        assertEquals(recordId, msg.getRecordId(), "Record ID mismatch");
        assertNotNull(msg.getAffectedEntities(), "Affected entities should not be null");
        assertNotNull(msg.getInterestingEntities(), "Interesting entities should not be null");
        assertNotNull(msg.getNotices(), "Notices should not be null");

        // Verify nulls removed from collections
        for (Long entityId : msg.getAffectedEntities()) {
            assertNotNull(entityId, "Null entity IDs should be removed");
        }
        for (SzInterestingEntity entity : msg.getInterestingEntities()) {
            assertNotNull(entity, "Null interesting entities should be removed");
        }
        for (SzNotice notice : msg.getNotices()) {
            assertNotNull(notice, "Null notices should be removed");
        }
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void propertySetterTest(String dataSource, String recordId,
                                   List<Long> affectedEntities,
                                   List<SzInterestingEntity> interestingEntities,
                                   List<SzNotice> notices) {
        SzInfoMessage msg = new SzInfoMessage();

        msg.setDataSource(dataSource);
        msg.setRecordId(recordId);
        msg.setAffectedEntities(affectedEntities);
        msg.setInterestingEntities(interestingEntities);
        msg.setNotices(notices);

        assertEquals(dataSource, msg.getDataSource(), "Data source mismatch");
        assertEquals(recordId, msg.getRecordId(), "Record ID mismatch");
        assertNotNull(msg.getAffectedEntities(), "Affected entities should not be null");
        assertNotNull(msg.getInterestingEntities(), "Interesting entities should not be null");
        assertNotNull(msg.getNotices(), "Notices should not be null");
    }

    @Test
    public void addAffectedEntityTest() {
        SzInfoMessage msg = new SzInfoMessage();
        msg.addAffectedEntity(100L);
        msg.addAffectedEntity(200L);

        Set<Long> entities = msg.getAffectedEntities();
        assertEquals(2, entities.size(), "Should have 2 affected entities");
        assertTrue(entities.contains(100L), "Should contain entity 100");
        assertTrue(entities.contains(200L), "Should contain entity 200");
    }

    @Test
    public void addAffectedEntityNullTest() {
        SzInfoMessage msg = new SzInfoMessage();
        assertThrows(NullPointerException.class, () -> msg.addAffectedEntity(null),
                "Adding null entity ID should throw NullPointerException");
    }

    @Test
    public void addInterestingEntityTest() {
        SzInfoMessage msg = new SzInfoMessage();
        SzInterestingEntity entity1 = createInterestingEntity(100L);
        SzInterestingEntity entity2 = createInterestingEntity(200L);

        msg.addInterestingEntity(entity1);
        msg.addInterestingEntity(entity2);

        List<SzInterestingEntity> entities = msg.getInterestingEntities();
        assertEquals(2, entities.size(), "Should have 2 interesting entities");
    }

    @Test
    public void addInterestingEntityNullTest() {
        SzInfoMessage msg = new SzInfoMessage();
        assertThrows(NullPointerException.class, () -> msg.addInterestingEntity(null),
                "Adding null interesting entity should throw NullPointerException");
    }

    @Test
    public void addNoticeTest() {
        SzInfoMessage msg = new SzInfoMessage();
        SzNotice notice1 = createNotice("CODE1");
        SzNotice notice2 = createNotice("CODE2");

        msg.addNotice(notice1);
        msg.addNotice(notice2);

        List<SzNotice> notices = msg.getNotices();
        assertEquals(2, notices.size(), "Should have 2 notices");
    }

    @Test
    public void addNoticeNullTest() {
        SzInfoMessage msg = new SzInfoMessage();
        assertThrows(NullPointerException.class, () -> msg.addNotice(null),
                "Adding null notice should throw NullPointerException");
    }

    @Test
    public void affectedEntitiesUnmodifiableTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID",
                Arrays.asList(100L), null, null);
        Set<Long> entities = msg.getAffectedEntities();
        assertThrows(UnsupportedOperationException.class, () -> entities.add(200L),
                "Affected entities set should be unmodifiable");
    }

    @Test
    public void interestingEntitiesUnmodifiableTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null,
                Arrays.asList(createInterestingEntity(100L)), null);
        List<SzInterestingEntity> entities = msg.getInterestingEntities();
        assertThrows(UnsupportedOperationException.class,
                () -> entities.add(createInterestingEntity(200L)),
                "Interesting entities list should be unmodifiable");
    }

    @Test
    public void noticesUnmodifiableTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null,
                Arrays.asList(createNotice("CODE")));
        List<SzNotice> notices = msg.getNotices();
        assertThrows(UnsupportedOperationException.class,
                () -> notices.add(createNotice("CODE2")),
                "Notices list should be unmodifiable");
    }

    @Test
    public void setAffectedEntitiesNullClearsTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID",
                Arrays.asList(100L, 200L), null, null);
        assertEquals(2, msg.getAffectedEntities().size());

        msg.setAffectedEntities(null);
        assertTrue(msg.getAffectedEntities().isEmpty());
    }

    @Test
    public void setInterestingEntitiesNullClearsTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null,
                Arrays.asList(createInterestingEntity(100L)), null);
        assertEquals(1, msg.getInterestingEntities().size());

        msg.setInterestingEntities(null);
        assertTrue(msg.getInterestingEntities().isEmpty());
    }

    @Test
    public void setNoticesNullClearsTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null,
                Arrays.asList(createNotice("CODE")));
        assertEquals(1, msg.getNotices().size());

        msg.setNotices(null);
        assertTrue(msg.getNotices().isEmpty());
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void equalsTest(String dataSource, String recordId,
                           List<Long> affectedEntities,
                           List<SzInterestingEntity> interestingEntities,
                           List<SzNotice> notices) {
        SzInfoMessage msg1 = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);
        SzInfoMessage msg2 = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);

        assertEquals(msg1, msg2, "Equivalent messages should be equal");
        assertEquals(msg1, msg1, "Message should equal itself");
    }

    @Test
    public void notEqualsNullTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null, null);
        assertNotEquals(msg, null, "Message should not equal null");
    }

    @Test
    public void notEqualsDifferentClassTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null, null);
        assertNotEquals(msg, "not a message", "Message should not equal different class");
    }

    @Test
    public void notEqualsDifferentDataSourceTest() {
        SzInfoMessage msg1 = new SzInfoMessage("DS1", "ID", null, null, null);
        SzInfoMessage msg2 = new SzInfoMessage("DS2", "ID", null, null, null);
        assertNotEquals(msg1, msg2);
    }

    @Test
    public void notEqualsDifferentRecordIdTest() {
        SzInfoMessage msg1 = new SzInfoMessage("DS", "ID1", null, null, null);
        SzInfoMessage msg2 = new SzInfoMessage("DS", "ID2", null, null, null);
        assertNotEquals(msg1, msg2);
    }

    @Test
    public void notEqualsDifferentAffectedEntitiesTest() {
        SzInfoMessage msg1 = new SzInfoMessage("DS", "ID",
                Arrays.asList(100L), null, null);
        SzInfoMessage msg2 = new SzInfoMessage("DS", "ID",
                Arrays.asList(200L), null, null);
        assertNotEquals(msg1, msg2);
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void hashCodeConsistencyTest(String dataSource, String recordId,
                                        List<Long> affectedEntities,
                                        List<SzInterestingEntity> interestingEntities,
                                        List<SzNotice> notices) {
        SzInfoMessage msg1 = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);
        SzInfoMessage msg2 = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);

        if (msg1.equals(msg2)) {
            assertEquals(msg1.hashCode(), msg2.hashCode(),
                    "Equal objects must have equal hash codes");
        }
    }

    @ParameterizedTest
    @MethodSource("getPropertyParameters")
    public void toStringTest(String dataSource, String recordId,
                             List<Long> affectedEntities,
                             List<SzInterestingEntity> interestingEntities,
                             List<SzNotice> notices) {
        SzInfoMessage msg = new SzInfoMessage(dataSource, recordId,
                affectedEntities, interestingEntities, notices);
        assertDoesNotThrow(() -> msg.toString(),
                "toString should not throw exception");
        assertNotNull(msg.toString(), "toString should not return null");
    }

    @Test
    public void toJsonTextTest() {
        SzInfoMessage msg = new SzInfoMessage("CUSTOMERS", "REC001",
                Arrays.asList(100L, 200L),
                Arrays.asList(createInterestingEntity(100L)),
                Arrays.asList(createNotice("CODE")));

        String jsonText = msg.toJsonText();
        assertNotNull(jsonText, "JSON text should not be null");

        JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
        assertNotNull(jsonObject, "Parsed JSON should not be null");
    }

    @Test
    public void toJsonTextPrettyPrintTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null, null);

        String jsonText = msg.toJsonText(true);
        assertNotNull(jsonText, "Pretty-printed JSON text should not be null");
    }

    @Test
    public void toJsonTextWithNoNoticesTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID",
                Arrays.asList(100L),
                Arrays.asList(createInterestingEntity(100L)),
                Collections.emptyList());

        String jsonText = msg.toJsonText();
        assertNotNull(jsonText);

        // Verify notices are not included when empty
        JsonObject jsonObject = JsonUtilities.parseJsonObject(jsonText);
        JsonObject interestingEntities = jsonObject.getJsonObject(SzInfoMessage.INTERESTING_ENTITIES_KEY);
        assertFalse(interestingEntities.containsKey(SzInfoMessage.NOTICES_KEY),
                "Notices should not be included when empty");
    }

    @Test
    public void toJsonObjectTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID",
                Arrays.asList(100L), null, null);

        JsonObject jsonObject = msg.toJsonObject();
        assertNotNull(jsonObject, "JSON object should not be null");
    }

    @Test
    public void toJsonObjectBuilderWithNullTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null, null);

        JsonObjectBuilder builder = msg.toJsonObjectBuilder(null);
        assertNotNull(builder, "Builder should not be null");
        assertNotNull(builder.build(), "Built JSON should not be null");
    }

    @Test
    public void toJsonObjectBuilderWithExistingTest() {
        SzInfoMessage msg = new SzInfoMessage("DS", "ID", null, null, null);

        JsonObjectBuilder existingBuilder = Json.createObjectBuilder();
        existingBuilder.add("existingKey", "existingValue");

        JsonObjectBuilder builder = msg.toJsonObjectBuilder(existingBuilder);
        JsonObject result = builder.build();

        assertTrue(result.containsKey("existingKey"), "Should retain existing key");
    }

    @Test
    public void fromJsonNullTextTest() {
        SzInfoMessage msg = SzInfoMessage.fromJson((String) null);
        assertNull(msg, "fromJson with null text should return null");
    }

    @Test
    public void fromJsonNullObjectTest() {
        SzInfoMessage msg = SzInfoMessage.fromJson((JsonObject) null);
        assertNull(msg, "fromJson with null object should return null");
    }

    @Test
    public void fromRawJsonNullTextTest() {
        SzInfoMessage msg = SzInfoMessage.fromRawJson((String) null);
        assertNull(msg, "fromRawJson with null text should return null");
    }

    @Test
    public void fromRawJsonNullObjectTest() {
        SzInfoMessage msg = SzInfoMessage.fromRawJson((JsonObject) null);
        assertNull(msg, "fromRawJson with null object should return null");
    }

    @Test
    public void fromJsonMissingPropertiesTest() {
        JsonObject jsonObject = Json.createObjectBuilder()
                .add("someOtherKey", "value")
                .build();

        assertThrows(IllegalArgumentException.class,
                () -> SzInfoMessage.fromJson(jsonObject),
                "Should throw when required properties missing");
    }

    /**
     * Note: Round-trip tests for SzInfoMessage are not included because the class
     * has an intentional asymmetry - toJsonText() serializes affectedEntities as
     * plain Long values, while fromJson() expects them as JsonObjects with ENTITY_ID
     * keys (designed to parse raw Senzing INFO messages).
     */

    @Test
    public void fromJsonWithValidStructureTest() {
        // Build JSON in the format that fromJson expects
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInfoMessage.DATA_SOURCE_KEY, "CUSTOMERS");
        JsonUtilities.add(job, SzInfoMessage.RECORD_ID_KEY, "REC001");

        // Build affected entities array with objects containing entityId
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        JsonObjectBuilder entityObj1 = Json.createObjectBuilder();
        entityObj1.add(SzInfoMessage.RAW_ENTITY_ID_KEY, 100L);
        affectedBuilder.add(entityObj1);
        JsonObjectBuilder entityObj2 = Json.createObjectBuilder();
        entityObj2.add(SzInfoMessage.RAW_ENTITY_ID_KEY, 200L);
        affectedBuilder.add(entityObj2);
        job.add(SzInfoMessage.AFFECTED_ENTITIES_KEY, affectedBuilder);

        // Build interesting entities object
        JsonObjectBuilder interestingObj = Json.createObjectBuilder();
        JsonArrayBuilder entitiesBuilder = Json.createArrayBuilder();
        entitiesBuilder.add(createInterestingEntity(100L).toJsonObjectBuilder());
        interestingObj.add(SzInfoMessage.ENTITIES_KEY, entitiesBuilder);

        // Add notices
        JsonArrayBuilder noticesBuilder = Json.createArrayBuilder();
        noticesBuilder.add(createNotice("CODE").toJsonObjectBuilder());
        interestingObj.add(SzInfoMessage.NOTICES_KEY, noticesBuilder);

        job.add(SzInfoMessage.INTERESTING_ENTITIES_KEY, interestingObj);

        String jsonText = JsonUtilities.toJsonText(job.build());
        SzInfoMessage parsed = SzInfoMessage.fromJson(jsonText);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("REC001", parsed.getRecordId());
        assertEquals(2, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(100L));
        assertTrue(parsed.getAffectedEntities().contains(200L));
    }

    @Test
    public void fromJsonObjectWithValidStructureTest() {
        // Build JSON in the format that fromJson expects
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInfoMessage.DATA_SOURCE_KEY, "EMPLOYEES");
        JsonUtilities.add(job, SzInfoMessage.RECORD_ID_KEY, "EMP001");

        // Build affected entities array with objects containing entityId
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        JsonObjectBuilder entityObj = Json.createObjectBuilder();
        entityObj.add(SzInfoMessage.RAW_ENTITY_ID_KEY, 300L);
        affectedBuilder.add(entityObj);
        job.add(SzInfoMessage.AFFECTED_ENTITIES_KEY, affectedBuilder);

        // Build interesting entities object with empty entities array
        JsonObjectBuilder interestingObj = Json.createObjectBuilder();
        interestingObj.add(SzInfoMessage.ENTITIES_KEY, Json.createArrayBuilder());
        job.add(SzInfoMessage.INTERESTING_ENTITIES_KEY, interestingObj);

        SzInfoMessage parsed = SzInfoMessage.fromJson(job.build());

        assertEquals("EMPLOYEES", parsed.getDataSource());
        assertEquals("EMP001", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(300L));
    }

    @Test
    public void fromJsonWithEmptyAffectedEntitiesTest() {
        // Build JSON with empty affected entities
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInfoMessage.DATA_SOURCE_KEY, "DS");
        JsonUtilities.add(job, SzInfoMessage.RECORD_ID_KEY, "ID");
        job.add(SzInfoMessage.AFFECTED_ENTITIES_KEY, Json.createArrayBuilder());

        // Build interesting entities object
        JsonObjectBuilder interestingObj = Json.createObjectBuilder();
        interestingObj.add(SzInfoMessage.ENTITIES_KEY, Json.createArrayBuilder());
        job.add(SzInfoMessage.INTERESTING_ENTITIES_KEY, interestingObj);

        SzInfoMessage parsed = SzInfoMessage.fromJson(job.build());

        assertEquals("DS", parsed.getDataSource());
        assertEquals("ID", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message JSON in the format returned by
     * Senzing SDK methods like addRecordWithInfo().
     *
     * Example raw INFO message format:
     * <pre>
     * {
     *   "DATA_SOURCE": "CUSTOMERS",
     *   "RECORD_ID": "1001",
     *   "AFFECTED_ENTITIES": [
     *     {"ENTITY_ID": 1}
     *   ],
     *   "INTERESTING_ENTITIES": {
     *     "ENTITIES": []
     *   }
     * }
     * </pre>
     */
    @Test
    public void fromRawJsonBasicInfoMessageTest() {
        // Build a raw Senzing INFO message JSON (as returned by addRecordWithInfo)
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "1001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 100}
                ],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": []
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("1001", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(100L));
        assertTrue(parsed.getInterestingEntities().isEmpty());
        assertTrue(parsed.getNotices().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with multiple affected entities.
     */
    @Test
    public void fromRawJsonMultipleAffectedEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "EMPLOYEES",
                "RECORD_ID": "EMP-2024-001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 100},
                    {"ENTITY_ID": 200},
                    {"ENTITY_ID": 300}
                ],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": []
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("EMPLOYEES", parsed.getDataSource());
        assertEquals("EMP-2024-001", parsed.getRecordId());
        assertEquals(3, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(100L));
        assertTrue(parsed.getAffectedEntities().contains(200L));
        assertTrue(parsed.getAffectedEntities().contains(300L));
    }

    /**
     * Tests parsing a comprehensive raw Senzing INFO message with interesting
     * entities containing flags, degrees, and sample records.
     */
    @Test
    public void fromRawJsonWithInterestingEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "CUST-001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 1}
                ],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": [
                        {
                            "ENTITY_ID": 1,
                            "DEGREES": 0,
                            "FLAGS": ["RESOLVED"],
                            "SAMPLE_RECORDS": [
                                {
                                    "DATA_SOURCE": "CUSTOMERS",
                                    "RECORD_ID": "CUST-001",
                                    "FLAGS": ["RESOLVED"]
                                }
                            ]
                        },
                        {
                            "ENTITY_ID": 2,
                            "DEGREES": 1,
                            "FLAGS": ["RELATED"],
                            "SAMPLE_RECORDS": [
                                {
                                    "DATA_SOURCE": "EMPLOYEES",
                                    "RECORD_ID": "EMP-001",
                                    "FLAGS": ["RELATED"]
                                }
                            ]
                        }
                    ]
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("CUST-001", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(1L));

        // Verify interesting entities
        assertEquals(2, parsed.getInterestingEntities().size());

        SzInterestingEntity entity1 = parsed.getInterestingEntities().get(0);
        assertEquals(Long.valueOf(1L), entity1.getEntityId());
        assertEquals(Integer.valueOf(0), entity1.getDegrees());
        assertTrue(entity1.getFlags().contains("RESOLVED"));
        assertEquals(1, entity1.getSampleRecords().size());
        assertEquals("CUSTOMERS", entity1.getSampleRecords().get(0).getDataSource());
        assertEquals("CUST-001", entity1.getSampleRecords().get(0).getRecordId());

        SzInterestingEntity entity2 = parsed.getInterestingEntities().get(1);
        assertEquals(Long.valueOf(2L), entity2.getEntityId());
        assertEquals(Integer.valueOf(1), entity2.getDegrees());
        assertTrue(entity2.getFlags().contains("RELATED"));
        assertEquals(1, entity2.getSampleRecords().size());
        assertEquals("EMPLOYEES", entity2.getSampleRecords().get(0).getDataSource());
    }

    /**
     * Tests parsing a raw Senzing INFO message with notices.
     */
    @Test
    public void fromRawJsonWithNoticesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "WATCHLIST",
                "RECORD_ID": "WL-001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 500}
                ],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": [
                        {
                            "ENTITY_ID": 500,
                            "DEGREES": 0,
                            "FLAGS": ["RESOLVED"],
                            "SAMPLE_RECORDS": [
                                {
                                    "DATA_SOURCE": "WATCHLIST",
                                    "RECORD_ID": "WL-001",
                                    "FLAGS": ["RESOLVED"]
                                }
                            ]
                        }
                    ],
                    "NOTICES": [
                        {
                            "CODE": "ENTITY_SIZE_WARNING",
                            "DESCRIPTION": "Entity 500 has grown to a large size"
                        },
                        {
                            "CODE": "DATA_QUALITY_ISSUE",
                            "DESCRIPTION": "Missing required field"
                        }
                    ]
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("WATCHLIST", parsed.getDataSource());
        assertEquals("WL-001", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());

        // Verify interesting entities
        assertEquals(1, parsed.getInterestingEntities().size());
        assertEquals(Long.valueOf(500L), parsed.getInterestingEntities().get(0).getEntityId());

        // Verify notices
        assertEquals(2, parsed.getNotices().size());

        SzNotice notice1 = parsed.getNotices().get(0);
        assertEquals("ENTITY_SIZE_WARNING", notice1.getCode());
        assertEquals("Entity 500 has grown to a large size", notice1.getDescription());

        SzNotice notice2 = parsed.getNotices().get(1);
        assertEquals("DATA_QUALITY_ISSUE", notice2.getCode());
        assertEquals("Missing required field", notice2.getDescription());
    }

    /**
     * Tests parsing a raw Senzing INFO message with empty affected entities
     * (edge case when a record is deleted).
     */
    @Test
    public void fromRawJsonEmptyAffectedEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "DELETED-001",
                "AFFECTED_ENTITIES": [],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": []
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("DELETED-001", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty());
        assertTrue(parsed.getInterestingEntities().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message using fromRawJson with JsonObject.
     */
    @Test
    public void fromRawJsonObjectTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("DATA_SOURCE", "VENDORS");
        job.add("RECORD_ID", "V-100");

        // Build affected entities
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        JsonObjectBuilder entityObj = Json.createObjectBuilder();
        entityObj.add("ENTITY_ID", 999L);
        affectedBuilder.add(entityObj);
        job.add("AFFECTED_ENTITIES", affectedBuilder);

        // Build interesting entities with sample records
        JsonObjectBuilder interestingObj = Json.createObjectBuilder();
        JsonArrayBuilder entitiesBuilder = Json.createArrayBuilder();

        JsonObjectBuilder entityBuilder = Json.createObjectBuilder();
        entityBuilder.add("ENTITY_ID", 999L);
        entityBuilder.add("DEGREES", 0);
        entityBuilder.add("FLAGS", Json.createArrayBuilder().add("RESOLVED"));

        JsonArrayBuilder sampleRecordsBuilder = Json.createArrayBuilder();
        JsonObjectBuilder sampleRecord = Json.createObjectBuilder();
        sampleRecord.add("DATA_SOURCE", "VENDORS");
        sampleRecord.add("RECORD_ID", "V-100");
        sampleRecord.add("FLAGS", Json.createArrayBuilder().add("RESOLVED"));
        sampleRecordsBuilder.add(sampleRecord);
        entityBuilder.add("SAMPLE_RECORDS", sampleRecordsBuilder);

        entitiesBuilder.add(entityBuilder);
        interestingObj.add("ENTITIES", entitiesBuilder);
        job.add("INTERESTING_ENTITIES", interestingObj);

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(job.build());

        assertEquals("VENDORS", parsed.getDataSource());
        assertEquals("V-100", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(999L));

        assertEquals(1, parsed.getInterestingEntities().size());
        SzInterestingEntity entity = parsed.getInterestingEntities().get(0);
        assertEquals(Long.valueOf(999L), entity.getEntityId());
        assertEquals(Integer.valueOf(0), entity.getDegrees());
        assertTrue(entity.getFlags().contains("RESOLVED"));
        assertEquals(1, entity.getSampleRecords().size());
        assertEquals("VENDORS", entity.getSampleRecords().get(0).getDataSource());
    }

    /**
     * Tests parsing a raw Senzing INFO message with INTERESTING_ENTITIES key absent.
     * This can occur when there are no interesting entities to report.
     */
    @Test
    public void fromRawJsonWithInterestingEntitiesAbsentTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "CUST-001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 100},
                    {"ENTITY_ID": 200}
                ]
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("CUST-001", parsed.getRecordId());
        assertEquals(2, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(100L));
        assertTrue(parsed.getAffectedEntities().contains(200L));
        assertTrue(parsed.getInterestingEntities().isEmpty(),
                "Interesting entities should be empty when key is absent");
        assertTrue(parsed.getNotices().isEmpty(),
                "Notices should be empty when INTERESTING_ENTITIES is absent");
    }

    /**
     * Tests parsing a raw Senzing INFO message with INTERESTING_ENTITIES key absent
     * and a single affected entity.
     */
    @Test
    public void fromRawJsonSingleAffectedEntityNoInterestingEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "EMPLOYEES",
                "RECORD_ID": "EMP-001",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 500}
                ]
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("EMPLOYEES", parsed.getDataSource());
        assertEquals("EMP-001", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getAffectedEntities().contains(500L));
        assertTrue(parsed.getInterestingEntities().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with INTERESTING_ENTITIES key absent
     * and empty affected entities array.
     */
    @Test
    public void fromRawJsonEmptyAffectedEntitiesNoInterestingEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "WATCHLIST",
                "RECORD_ID": "WL-DELETED",
                "AFFECTED_ENTITIES": []
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("WATCHLIST", parsed.getDataSource());
        assertEquals("WL-DELETED", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty());
        assertTrue(parsed.getInterestingEntities().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with AFFECTED_ENTITIES key absent.
     * The absent key should be treated as an empty array.
     */
    @Test
    public void fromRawJsonWithAffectedEntitiesAbsentTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "CUST-001",
                "INTERESTING_ENTITIES": {
                    "ENTITIES": []
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("CUST-001", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty(),
                "Affected entities should be empty when key is absent");
        assertTrue(parsed.getInterestingEntities().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with both AFFECTED_ENTITIES and
     * INTERESTING_ENTITIES keys absent. Both should be treated as empty.
     */
    @Test
    public void fromRawJsonWithBothAffectedAndInterestingAbsentTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "EMPLOYEES",
                "RECORD_ID": "EMP-001"
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("EMPLOYEES", parsed.getDataSource());
        assertEquals("EMP-001", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty(),
                "Affected entities should be empty when key is absent");
        assertTrue(parsed.getInterestingEntities().isEmpty(),
                "Interesting entities should be empty when key is absent");
        assertTrue(parsed.getNotices().isEmpty(),
                "Notices should be empty when INTERESTING_ENTITIES is absent");
    }

    /**
     * Tests parsing camelCase JSON with absent affectedEntities key.
     */
    @Test
    public void fromJsonWithAffectedEntitiesAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInfoMessage.DATA_SOURCE_KEY, "CUSTOMERS");
        JsonUtilities.add(job, SzInfoMessage.RECORD_ID_KEY, "CUST-001");

        // Build interesting entities object with empty entities array
        JsonObjectBuilder interestingObj = Json.createObjectBuilder();
        interestingObj.add(SzInfoMessage.ENTITIES_KEY, Json.createArrayBuilder());
        job.add(SzInfoMessage.INTERESTING_ENTITIES_KEY, interestingObj);

        SzInfoMessage parsed = SzInfoMessage.fromJson(job.build());

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("CUST-001", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty(),
                "Affected entities should be empty when key is absent");
    }

    /**
     * Tests parsing camelCase JSON with both affectedEntities and interestingEntities absent.
     */
    @Test
    public void fromJsonWithBothAffectedAndInterestingAbsentTest() {
        JsonObjectBuilder job = Json.createObjectBuilder();
        JsonUtilities.add(job, SzInfoMessage.DATA_SOURCE_KEY, "VENDORS");
        JsonUtilities.add(job, SzInfoMessage.RECORD_ID_KEY, "V-001");

        SzInfoMessage parsed = SzInfoMessage.fromJson(job.build());

        assertEquals("VENDORS", parsed.getDataSource());
        assertEquals("V-001", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty());
        assertTrue(parsed.getInterestingEntities().isEmpty());
        assertTrue(parsed.getNotices().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with both AFFECTED_ENTITIES and
     * INTERESTING_ENTITIES present but with empty arrays/content.
     */
    @Test
    public void fromRawJsonWithBothKeysEmptyTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "VENDORS",
                "RECORD_ID": "V-EMPTY",
                "AFFECTED_ENTITIES": [],
                "INTERESTING_ENTITIES": {
                    "ENTITIES": [],
                    "NOTICES": []
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("VENDORS", parsed.getDataSource());
        assertEquals("V-EMPTY", parsed.getRecordId());
        assertTrue(parsed.getAffectedEntities().isEmpty());
        assertTrue(parsed.getInterestingEntities().isEmpty());
        assertTrue(parsed.getNotices().isEmpty());
    }

    /**
     * Tests parsing a raw Senzing INFO message with INTERESTING_ENTITIES present
     * but ENTITIES array absent (only NOTICES present).
     */
    @Test
    public void fromRawJsonWithOnlyNoticesInInterestingEntitiesTest() {
        String rawJson = """
            {
                "DATA_SOURCE": "CUSTOMERS",
                "RECORD_ID": "CUST-002",
                "AFFECTED_ENTITIES": [
                    {"ENTITY_ID": 100}
                ],
                "INTERESTING_ENTITIES": {
                    "NOTICES": [
                        {
                            "CODE": "DATA_WARNING",
                            "DESCRIPTION": "Potential data quality issue"
                        }
                    ]
                }
            }
            """;

        SzInfoMessage parsed = SzInfoMessage.fromRawJson(rawJson);

        assertEquals("CUSTOMERS", parsed.getDataSource());
        assertEquals("CUST-002", parsed.getRecordId());
        assertEquals(1, parsed.getAffectedEntities().size());
        assertTrue(parsed.getInterestingEntities().isEmpty(),
                "Interesting entities should be empty when ENTITIES key is absent");
        assertEquals(1, parsed.getNotices().size());
        assertEquals("DATA_WARNING", parsed.getNotices().get(0).getCode());
    }
}
