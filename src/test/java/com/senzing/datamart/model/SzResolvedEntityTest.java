package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SzResolvedEntityTest {

    @Test
    void testDefaultConstructor() {
        SzResolvedEntity entity = new SzResolvedEntity();

        assertEquals(0L, entity.getEntityId());
        assertNull(entity.getEntityName());
        assertNotNull(entity.getRelatedEntities());
        assertTrue(entity.getRelatedEntities().isEmpty());
    }

    @Test
    void testGetRelatedEntitiesIsUnmodifiable() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(1L);

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(2L);
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("NAME");
        related.setPrinciple("MFF");
        entity.addRelatedEntity(related);

        Map<Long, SzRelatedEntity> relatedEntities = entity.getRelatedEntities();
        assertThrows(UnsupportedOperationException.class, () ->
            relatedEntities.put(3L, new SzRelatedEntity()));
    }

    @Test
    void testAddRelatedEntity() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(1L);

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(2L);
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("NAME");
        related.setPrinciple("MFF");

        entity.addRelatedEntity(related);

        assertEquals(1, entity.getRelatedEntities().size());
        assertTrue(entity.getRelatedEntities().containsKey(2L));
        assertEquals(related, entity.getRelatedEntities().get(2L));
    }

    @Test
    void testAddRelatedEntityWithSameIdThrows() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(100L);

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(100L);
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("KEY");
        related.setPrinciple("RULE");

        assertThrows(IllegalArgumentException.class, () -> entity.addRelatedEntity(related));
    }

    @Test
    void testSetRelatedEntities() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(1L);

        SzRelatedEntity related1 = new SzRelatedEntity();
        related1.setEntityId(2L);
        related1.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related1.setMatchKey("NAME");
        related1.setPrinciple("MFF");

        SzRelatedEntity related2 = new SzRelatedEntity();
        related2.setEntityId(3L);
        related2.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related2.setMatchKey("ADDRESS");
        related2.setPrinciple("MFS");

        entity.setRelatedEntities(Arrays.asList(related1, related2));

        assertEquals(2, entity.getRelatedEntities().size());
        assertTrue(entity.getRelatedEntities().containsKey(2L));
        assertTrue(entity.getRelatedEntities().containsKey(3L));
    }

    @Test
    void testSetRelatedEntitiesWithSameIdThrows() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(100L);

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(100L);
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("KEY");
        related.setPrinciple("RULE");

        assertThrows(IllegalArgumentException.class, () ->
            entity.setRelatedEntities(Collections.singletonList(related)));
    }

    @Test
    void testClearRelatedEntities() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(1L);

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(2L);
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("NAME");
        related.setPrinciple("MFF");
        entity.addRelatedEntity(related);

        entity.clearRelatedEntities();

        assertTrue(entity.getRelatedEntities().isEmpty());
    }

    @Test
    void testEqualsWithSameReference() {
        SzResolvedEntity entity = new SzResolvedEntity();
        assertEquals(entity, entity);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzResolvedEntity entity1 = createTestResolvedEntity();
        SzResolvedEntity entity2 = createTestResolvedEntity();

        assertEquals(entity1, entity2);
        assertEquals(entity2, entity1);
    }

    @Test
    void testEqualsWithDifferentRelatedEntities() {
        SzResolvedEntity entity1 = new SzResolvedEntity();
        entity1.setEntityId(1L);
        SzRelatedEntity related1 = new SzRelatedEntity();
        related1.setEntityId(2L);
        related1.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related1.setMatchKey("NAME");
        related1.setPrinciple("MFF");
        entity1.addRelatedEntity(related1);

        SzResolvedEntity entity2 = new SzResolvedEntity();
        entity2.setEntityId(1L);
        SzRelatedEntity related2 = new SzRelatedEntity();
        related2.setEntityId(3L);
        related2.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related2.setMatchKey("NAME");
        related2.setPrinciple("MFF");
        entity2.addRelatedEntity(related2);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithNull() {
        SzResolvedEntity entity = new SzResolvedEntity();
        assertNotEquals(null, entity);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(100L);

        SzEntity baseEntity = new SzEntity();
        baseEntity.setEntityId(100L);

        assertNotEquals(entity, baseEntity);
    }

    @Test
    void testHashCodeConsistency() {
        SzResolvedEntity entity1 = createTestResolvedEntity();
        SzResolvedEntity entity2 = createTestResolvedEntity();

        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    void testBuildJson() {
        SzResolvedEntity entity = createTestResolvedEntity();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        entity.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals(1L, json.getJsonNumber("id").longValue());
        assertTrue(json.containsKey("related"));
        assertEquals(1, json.getJsonArray("related").size());
    }

    @Test
    void testToJsonObject() {
        SzResolvedEntity entity = createTestResolvedEntity();
        JsonObject json = entity.toJsonObject();

        assertNotNull(json);
        assertEquals(1L, json.getJsonNumber("id").longValue());
    }

    @Test
    void testToString() {
        SzResolvedEntity entity = createTestResolvedEntity();
        String result = entity.toString();

        assertNotNull(result);
        assertEquals(entity.toJsonText(), result);
    }

    @Test
    void testToStringWithNullProperties() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(100L);
        entity.setEntityName(null);

        assertDoesNotThrow(() -> entity.toString());
    }

    @Test
    void testParseWithJsonObject() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("name", "Test Entity");

        JsonArrayBuilder relatedArray = Json.createArrayBuilder();
        JsonObjectBuilder relatedBuilder = Json.createObjectBuilder();
        relatedBuilder.add("id", 200L);
        relatedBuilder.add("matchType", "PM");
        relatedBuilder.add("matchKey", "NAME");
        relatedBuilder.add("principle", "MFF");
        relatedArray.add(relatedBuilder);
        builder.add("related", relatedArray);

        JsonObject json = builder.build();
        SzResolvedEntity entity = SzResolvedEntity.parse(json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("Test Entity", entity.getEntityName());
        assertEquals(1, entity.getRelatedEntities().size());
    }

    @Test
    void testParseWithAlternateFieldNames() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("ENTITY_ID", 100L);
        builder.add("ENTITY_NAME", "Test Entity");

        JsonArrayBuilder relatedArray = Json.createArrayBuilder();
        builder.add("RELATED_ENTITIES", relatedArray);

        JsonObject json = builder.build();
        SzResolvedEntity entity = SzResolvedEntity.parse(json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("Test Entity", entity.getEntityName());
    }

    @Test
    void testParseWithResolvedEntityWrapper() {
        JsonObjectBuilder resolvedBuilder = Json.createObjectBuilder();
        resolvedBuilder.add("ENTITY_ID", 100L);
        resolvedBuilder.add("ENTITY_NAME", "Test Entity");

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("RESOLVED_ENTITY", resolvedBuilder);
        builder.add("RELATED_ENTITIES", Json.createArrayBuilder());

        JsonObject json = builder.build();
        SzResolvedEntity entity = SzResolvedEntity.parse(json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("Test Entity", entity.getEntityName());
    }

    @Test
    void testParseWithNull() {
        assertNull(SzResolvedEntity.parse((JsonObject) null));
    }

    @Test
    void testParseStringWithNull() {
        assertNull(SzResolvedEntity.parse((String) null));
    }

    @Test
    void testParseHashWithNull() {
        assertNull(SzResolvedEntity.parseHash(null));
    }

    @Test
    void testToHashAndParseHash() {
        SzResolvedEntity original = createTestResolvedEntity();

        String hash = original.toHash();
        assertNotNull(hash);
        assertTrue(hash.length() > 0);

        SzResolvedEntity parsed = SzResolvedEntity.parseHash(hash);
        assertEquals(original, parsed);
    }

    @Test
    void testRoundTripThroughJson() {
        SzResolvedEntity original = createTestResolvedEntity();

        JsonObject json = original.toJsonObject();
        SzResolvedEntity parsed = SzResolvedEntity.parse(json);

        assertEquals(original, parsed);
    }

    private SzResolvedEntity createTestResolvedEntity() {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(1L);
        entity.setEntityName("Test Entity");
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));

        SzRelatedEntity related = new SzRelatedEntity();
        related.setEntityId(2L);
        related.setEntityName("Related Entity");
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("NAME+DOB");
        related.setPrinciple("MFF");
        related.addRecord(new SzRecord("VENDORS", "REC-002", null, null));

        entity.addRelatedEntity(related);

        return entity;
    }
}
