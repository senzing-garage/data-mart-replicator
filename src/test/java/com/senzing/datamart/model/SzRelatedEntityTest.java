package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import static org.junit.jupiter.api.Assertions.*;

class SzRelatedEntityTest {

    @Test
    void testDefaultConstructor() {
        SzRelatedEntity entity = new SzRelatedEntity();

        assertEquals(0L, entity.getEntityId());
        assertNull(entity.getEntityName());
        assertNull(entity.getMatchType());
        assertNull(entity.getMatchKey());
        assertNull(entity.getPrinciple());
    }

    @Test
    void testSetAndGetMatchType() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setMatchType(SzMatchType.POSSIBLE_MATCH);

        assertEquals(SzMatchType.POSSIBLE_MATCH, entity.getMatchType());
    }

    @Test
    void testSetMatchTypeToNull() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setMatchType(SzMatchType.POSSIBLE_MATCH);
        entity.setMatchType(null);

        assertNull(entity.getMatchType());
    }

    @Test
    void testSetAndGetMatchKey() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setMatchKey("NAME+DOB");

        assertEquals("NAME+DOB", entity.getMatchKey());
    }

    @Test
    void testSetMatchKeyToNull() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setMatchKey("NAME+DOB");
        entity.setMatchKey(null);

        assertNull(entity.getMatchKey());
    }

    @Test
    void testSetAndGetPrinciple() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setPrinciple("MFF");

        assertEquals("MFF", entity.getPrinciple());
    }

    @Test
    void testSetPrincipleToNull() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setPrinciple("MFF");
        entity.setPrinciple(null);

        assertNull(entity.getPrinciple());
    }

    @Test
    void testEqualsWithSameReference() {
        SzRelatedEntity entity = new SzRelatedEntity();
        assertEquals(entity, entity);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();

        assertEquals(entity1, entity2);
        assertEquals(entity2, entity1);
    }

    @Test
    void testEqualsWithDifferentMatchType() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();
        entity2.setMatchType(SzMatchType.DISCLOSED_RELATION);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();
        entity2.setMatchKey("ADDRESS");

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();
        entity2.setPrinciple("MFS");

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithDifferentBaseProperties() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();
        entity2.setEntityId(999L);

        assertNotEquals(entity1, entity2);
    }

    @Test
    void testEqualsWithNull() {
        SzRelatedEntity entity = new SzRelatedEntity();
        assertNotEquals(null, entity);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setEntityId(100L);

        SzEntity baseEntity = new SzEntity();
        baseEntity.setEntityId(100L);

        assertNotEquals(entity, baseEntity);
    }

    @Test
    void testHashCodeConsistency() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();

        assertEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    void testHashCodeDifferentForDifferentMatchType() {
        SzRelatedEntity entity1 = createTestRelatedEntity();
        SzRelatedEntity entity2 = createTestRelatedEntity();
        entity2.setMatchType(SzMatchType.DISCLOSED_RELATION);

        assertNotEquals(entity1.hashCode(), entity2.hashCode());
    }

    @Test
    void testBuildJson() {
        SzRelatedEntity entity = createTestRelatedEntity();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        entity.buildJson(builder);
        JsonObject json = builder.build();

        assertEquals(100L, json.getJsonNumber("id").longValue());
        assertEquals("Test Related", json.getString("name"));
        assertEquals("PM", json.getString("matchType"));
        assertEquals("NAME+DOB", json.getString("matchKey"));
        assertEquals("MFF", json.getString("principle"));
    }

    @Test
    void testToJsonObject() {
        SzRelatedEntity entity = createTestRelatedEntity();
        JsonObject json = entity.toJsonObject();

        assertNotNull(json);
        assertEquals(100L, json.getJsonNumber("id").longValue());
        assertEquals("PM", json.getString("matchType"));
    }

    @Test
    void testToString() {
        SzRelatedEntity entity = createTestRelatedEntity();
        String result = entity.toString();

        assertNotNull(result);
        assertEquals(entity.toJsonText(), result);
    }

    @Test
    void testToStringWithNullProperties() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setEntityId(100L);
        entity.setMatchType(SzMatchType.POSSIBLE_MATCH);
        entity.setMatchKey("KEY");
        entity.setPrinciple("RULE");

        assertDoesNotThrow(() -> entity.toString());
    }

    @Test
    void testParseWithMatchTypeCode() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("name", "Test");
        builder.add("matchType", "PM");
        builder.add("matchKey", "NAME");
        builder.add("principle", "MFF");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

        assertEquals(100L, entity.getEntityId());
        assertEquals(SzMatchType.POSSIBLE_MATCH, entity.getMatchType());
        assertEquals("NAME", entity.getMatchKey());
        assertEquals("MFF", entity.getPrinciple());
    }

    @Test
    void testParseWithMatchTypeEnum() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("matchType", "POSSIBLE_MATCH");
        builder.add("matchKey", "NAME");
        builder.add("principle", "MFF");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

        assertEquals(SzMatchType.POSSIBLE_MATCH, entity.getMatchType());
    }

    @Test
    void testParseWithAlternateFieldNames() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("ENTITY_ID", 100L);
        builder.add("ENTITY_NAME", "Test");
        builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
        builder.add("MATCH_KEY", "NAME");
        builder.add("ERRULE_CODE", "MFF");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

        assertEquals(100L, entity.getEntityId());
        assertEquals("Test", entity.getEntityName());
        assertEquals(SzMatchType.POSSIBLE_MATCH, entity.getMatchType());
        assertEquals("NAME", entity.getMatchKey());
        assertEquals("MFF", entity.getPrinciple());
    }

    @Test
    void testParseWithExistingEntity() {
        SzRelatedEntity existing = new SzRelatedEntity();
        existing.setEntityId(999L);

        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("matchType", "PM");
        builder.add("matchKey", "NAME");
        builder.add("principle", "MFF");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(existing, json);

        assertSame(existing, entity);
        assertEquals(100L, entity.getEntityId());
    }

    @Test
    void testParseDetectsAmbiguousMatch() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("IS_AMBIGUOUS", 1);
        builder.add("MATCH_KEY", "NAME");
        builder.add("ERRULE_CODE", "MFF");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

        assertEquals(SzMatchType.AMBIGUOUS_MATCH, entity.getMatchType());
    }

    @Test
    void testParseDetectsDisclosedRelation() {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("id", 100L);
        builder.add("IS_DISCLOSED", 1);
        builder.add("MATCH_KEY", "REL");
        builder.add("ERRULE_CODE", "DISCLOSED");

        JsonObject json = builder.build();
        SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

        assertEquals(SzMatchType.DISCLOSED_RELATION, entity.getMatchType());
    }

    @Test
    void testRoundTripThroughJson() {
        SzRelatedEntity original = createTestRelatedEntity();

        JsonObject json = original.toJsonObject();
        SzRelatedEntity parsed = SzRelatedEntity.parse(null, json);

        assertEquals(original, parsed);
    }

    @Test
    void testParseWithEmptyMatchKeyLogsError() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            JsonObjectBuilder builder = Json.createObjectBuilder();
            builder.add("ENTITY_ID", 100L);
            builder.add("ENTITY_NAME", "Test Entity");
            builder.add("MATCH_LEVEL_CODE", "POSSIBLY_SAME");
            builder.add("MATCH_KEY", "");  // Empty match key
            builder.add("ERRULE_CODE", "MFF");

            JsonObject json = builder.build();
            SzRelatedEntity entity = SzRelatedEntity.parse(null, json);

            // Verify entity was still parsed
            assertEquals(100L, entity.getEntityId());
            assertNull(entity.getMatchKey());  // Empty string normalized to null
        });

        // Verify the expected error was logged
        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Encountered empty match key"),
                "Should log error about empty match key. Actual output: " + errOutput);
        assertTrue(errOutput.contains("---------------------"),
                "Should include separator lines in error output");
    }

    private SzRelatedEntity createTestRelatedEntity() {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setEntityId(100L);
        entity.setEntityName("Test Related");
        entity.setMatchType(SzMatchType.POSSIBLE_MATCH);
        entity.setMatchKey("NAME+DOB");
        entity.setPrinciple("MFF");
        entity.addRecord(new SzRecord("CUSTOMERS", "REC-001", null, null));
        return entity;
    }
}
