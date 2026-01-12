package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SzEntityTest {

    @Test
    void testDefaultConstructor() {
        SzEntity entity = new SzEntity();

        assertEquals(0L, entity.getEntityId());
        assertNull(entity.getEntityName());
        assertNotNull(entity.getSourceSummary());
        assertTrue(entity.getSourceSummary().isEmpty());
        assertNotNull(entity.getRecords());
        assertTrue(entity.getRecords().isEmpty());
    }

    @Test
    void testSetAndGetEntityId() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(12345L);

        assertEquals(12345L, entity.getEntityId());
    }

    @Test
    void testSetAndGetEntityName() {
        SzEntity entity = new SzEntity();
        entity.setEntityName("John Doe");

        assertEquals("John Doe", entity.getEntityName());
    }

    @Test
    void testSetEntityNameToNull() {
        SzEntity entity = new SzEntity();
        entity.setEntityName("John Doe");
        entity.setEntityName(null);

        assertNull(entity.getEntityName());
    }

    @Test
    void testAddRecord() {
        SzEntity entity = new SzEntity();
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", null, null);

        entity.addRecord(record);

        Map<SzRecordKey, SzRecord> records = entity.getRecords();
        assertEquals(1, records.size());
        assertTrue(records.containsKey(record.getRecordKey()));
        assertEquals(record, records.get(record.getRecordKey()));
    }

    @Test
    void testAddDuplicateRecordIsIgnored() {
        SzEntity entity = new SzEntity();
        SzRecord record1 = new SzRecord("CUSTOMERS", "REC-001", null, null);
        SzRecord record2 = new SzRecord("CUSTOMERS", "REC-001", "DIFFERENT", "RULE");

        entity.addRecord(record1);
        entity.addRecord(record2);

        assertEquals(1, entity.getRecords().size());
    }

    @Test
    void testSourceSummaryUpdatedOnAddRecord() {
        SzEntity entity = new SzEntity();
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-002", null, null));
        entity.addRecord(new SzRecord("VENDORS", "REC-003", null, null));

        Map<String, Integer> summary = entity.getSourceSummary();
        assertEquals(2, summary.get("CUSTOMERS"));
        assertEquals(1, summary.get("VENDORS"));
    }

    @Test
    void testSetRecords() {
        SzEntity entity = new SzEntity();
        entity.addRecord(new SzRecord("OLD", "OLD-001", null, null));

        List<SzRecord> newRecords = Arrays.asList(
            new SzRecord("CUSTOMERS", "REC-001", null, null),
            new SzRecord("VENDORS", "REC-002", null, null)
        );

        entity.setRecords(newRecords);

        assertEquals(2, entity.getRecords().size());
        assertFalse(entity.getSourceSummary().containsKey("OLD"));
        assertTrue(entity.getSourceSummary().containsKey("CUSTOMERS"));
        assertTrue(entity.getSourceSummary().containsKey("VENDORS"));
    }

    @Test
    void testClearRecords() {
        SzEntity entity = new SzEntity();
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        entity.clearRecords();

        assertTrue(entity.getRecords().isEmpty());
        assertTrue(entity.getSourceSummary().isEmpty());
    }

    @Test
    void testGetSourceSummaryIsUnmodifiable() {
        SzEntity entity = new SzEntity();
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        Map<String, Integer> summary = entity.getSourceSummary();
        assertThrows(UnsupportedOperationException.class, () -> summary.put("NEW", 1));
    }

    @Test
    void testGetRecordsIsUnmodifiable() {
        SzEntity entity = new SzEntity();
        SzRecord record = new SzRecord("CUSTOMERS", "REC-001", null, null);
        entity.addRecord(record);

        Map<SzRecordKey, SzRecord> records = entity.getRecords();
        assertThrows(UnsupportedOperationException.class, () ->
            records.put(new SzRecordKey("NEW", "NEW-001"), record));
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntity entity = new SzEntity();
        assertEquals(entity, entity);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntity entity1 = new SzEntity();
        entity1.setEntityId(100L);
        entity1.setEntityName("Test");
        entity1.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        SzEntity entity2 = new SzEntity();
        entity2.setEntityId(100L);
        entity2.setEntityName("Test");
        entity2.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        assertEquals(entity1, entity2);
        assertEquals(entity2, entity1);
    }

    @Test
    void testEqualsWithDifferentEntityId() {
        SzEntity entity1 = new SzEntity();
        entity1.setEntityId(100L);

        SzEntity entity2 = new SzEntity();
        entity2.setEntityId(200L);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentEntityName() {
        SzEntity entity1 = new SzEntity();
        entity1.setEntityId(100L);
        entity1.setEntityName("Name1");

        SzEntity entity2 = new SzEntity();
        entity2.setEntityId(100L);
        entity2.setEntityName("Name2");

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentRecords() {
        SzEntity entity1 = new SzEntity();
        entity1.setEntityId(100L);
        entity1.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        SzEntity entity2 = new SzEntity();
        entity2.setEntityId(100L);
        entity2.addRecord(new SzRecord("VENDORS", "REC-001", null, null));

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntity entity = new SzEntity();
        assertNotEquals(null, entity);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntity entity = new SzEntity();
        assertNotEquals(entity, "not an entity");
    }

    @Test
    void testEqualsWithSubclass() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        SzResolvedEntity resolved = new SzResolvedEntity();
        resolved.setEntityId(100L);

        // Different classes should not be equal
        assertNotEquals(entity, resolved);
    }

    @Test
    void testHashCodeConsistency() {
        SzEntity entity1 = new SzEntity();
        entity1.setEntityId(100L);
        entity1.setEntityName("Test");

        SzEntity entity2 = new SzEntity();
        entity2.setEntityId(100L);
        entity2.setEntityName("Test");

        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    void testBuildJson() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);
        entity.setEntityName("John Doe");
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        JsonObjectBuilder builder = Json.createObjectBuilder();
        entity.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals(100L, json.getJsonNumber("id").longValue());
        assertEquals("John Doe", json.getString("name"));
        assertTrue(json.containsKey("records"));
    }

    @Test
    void testBuildJsonWithNullEntityName() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        entity.buildJson(builder);
        JsonObject json = builder.build();

        assertFalse(json.containsKey("name"));
    }

    @Test
    void testBuildJsonWithNoRecords() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        entity.buildJson(builder);
        JsonObject json = builder.build();

        assertFalse(json.containsKey("records"));
    }

    @Test
    void testToJsonObject() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);
        entity.setEntityName("Test");

        JsonObject json = entity.toJsonObject();

        assertNotNull(json);
        assertEquals(100L, json.getJsonNumber("id").longValue());
    }

    @Test
    void testToJsonText() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        String jsonText = entity.toJsonText();

        assertNotNull(jsonText);
        assertTrue(jsonText.contains("100"));
    }

    @Test
    void testToJsonTextPrettyPrint() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        String jsonTextPretty = entity.toJsonText(true);
        String jsonTextCompact = entity.toJsonText(false);

        assertNotNull(jsonTextPretty);
        assertNotNull(jsonTextCompact);
    }

    @Test
    void testToString() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);

        String result = entity.toString();

        assertEquals(entity.toJsonText(), result);
    }

    @Test
    void testToStringWithNullEntityName() {
        SzEntity entity = new SzEntity();
        entity.setEntityId(100L);
        entity.setEntityName(null);

        assertDoesNotThrow(() -> entity.toString());
    }

    @Test
    void testParseWithJsonObject() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("name", "John Doe");
        JsonObject json = builder.build();

        SzEntity entity = SzEntity.parse(json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("John Doe", entity.getEntityName());
    }

    @Test
    void testParseWithAlternateFieldNames() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("ENTITY_ID", 100L);
        builder.add("ENTITY_NAME", "John Doe");
        JsonObject json = builder.build();

        SzEntity entity = SzEntity.parse(json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("John Doe", entity.getEntityName());
    }

    @Test
    void testRoundTripThroughJson() {
        SzEntity original = new SzEntity();
        original.setEntityId(100L);
        original.setEntityName("Test Entity");
        original.addRecord(new SzRecord("CUSTOMERS", "REC-001", "MKEY", "RULE"));

        JsonObject json = original.toJsonObject();
        SzEntity parsed = SzEntity.parse(json);

        assertEquals(original, parsed);
    }
}
