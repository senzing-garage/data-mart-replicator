package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;

import javax.json.JsonObject;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SzRelationshipTest {

    @Test
    void testConstructorWithEntities() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        assertEquals(1L, relationship.getEntityId());
        assertEquals(2L, relationship.getRelatedEntityId());
        assertEquals(SzMatchType.POSSIBLE_MATCH, relationship.getMatchType());
        assertEquals("NAME+DOB", relationship.getMatchKey());
        assertEquals("MFF", relationship.getPrinciple());
    }

    @Test
    void testConstructorFlipsIdsWhenResolvedIsGreater() {
        SzResolvedEntity resolved = createResolvedEntity(100L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(50L, "VENDORS");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        assertEquals(50L, relationship.getEntityId());
        assertEquals(100L, relationship.getRelatedEntityId());
    }

    @Test
    void testConstructorWithNullResolvedEntityThrows() {
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(null, related));
    }

    @Test
    void testConstructorWithNullRelatedEntityThrows() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(resolved, null));
    }

    @Test
    void testConstructorWithUnrelatedEntityThrows() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        // Don't add related to resolved

        assertThrows(IllegalArgumentException.class, () ->
            new SzRelationship(resolved, related));
    }

    @Test
    void testConstructorWithSameEntityIdThrows() {
        SzResolvedEntity resolved = createResolvedEntity(100L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(100L, "VENDORS");
        // Manually add to bypass validation in addRelatedEntity
        try {
            resolved.addRelatedEntity(related);
            fail("Should have thrown");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    void testConstructorWithExplicitParameters() {
        Map<String, Integer> summary1 = new LinkedHashMap<>();
        summary1.put("CUSTOMERS", 2);

        Map<String, Integer> summary2 = new LinkedHashMap<>();
        summary2.put("VENDORS", 3);

        SzRelationship relationship = new SzRelationship(
            1L, 2L,
            SzMatchType.POSSIBLE_MATCH,
            "NAME+DOB",
            "MFF",
            summary1,
            summary2
        );

        assertEquals(1L, relationship.getEntityId());
        assertEquals(2L, relationship.getRelatedEntityId());
        assertEquals(SzMatchType.POSSIBLE_MATCH, relationship.getMatchType());
        assertEquals("NAME+DOB", relationship.getMatchKey());
        assertEquals("MFF", relationship.getPrinciple());
        assertEquals(summary1, relationship.getSourceSummary());
        assertEquals(summary2, relationship.getRelatedSourceSummary());
    }

    @Test
    void testConstructorWithExplicitParametersFlipsIds() {
        Map<String, Integer> summary1 = new LinkedHashMap<>();
        summary1.put("CUSTOMERS", 2);

        Map<String, Integer> summary2 = new LinkedHashMap<>();
        summary2.put("VENDORS", 3);

        SzRelationship relationship = new SzRelationship(
            100L, 50L,
            SzMatchType.POSSIBLE_MATCH,
            "NAME+DOB",
            "MFF",
            summary1,
            summary2
        );

        assertEquals(50L, relationship.getEntityId());
        assertEquals(100L, relationship.getRelatedEntityId());
        assertEquals(summary2, relationship.getSourceSummary());
        assertEquals(summary1, relationship.getRelatedSourceSummary());
    }

    @Test
    void testConstructorWithNullMatchKeyThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, null, "MFF", summary, summary));
    }

    @Test
    void testConstructorWithNullPrincipleThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", null, summary, summary));
    }

    @Test
    void testConstructorWithNullMatchTypeThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, null, "KEY", "MFF", summary, summary));
    }

    @Test
    void testConstructorWithNullSummary1Throws() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", null, summary));
    }

    @Test
    void testConstructorWithNullSummary2Throws() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary, null));
    }

    @Test
    void testGetSourceSummaryIsUnmodifiable() {
        SzRelationship relationship = createTestRelationship();

        Map<String, Integer> summary = relationship.getSourceSummary();
        assertThrows(UnsupportedOperationException.class, () -> summary.put("NEW", 1));
    }

    @Test
    void testGetRelatedSourceSummaryIsUnmodifiable() {
        SzRelationship relationship = createTestRelationship();

        Map<String, Integer> summary = relationship.getRelatedSourceSummary();
        assertThrows(UnsupportedOperationException.class, () -> summary.put("NEW", 1));
    }

    @Test
    void testEqualsWithSameReference() {
        SzRelationship relationship = createTestRelationship();
        assertEquals(relationship, relationship);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRelationship rel1 = createTestRelationship();
        SzRelationship rel2 = createTestRelationship();

        assertEquals(rel1, rel2);
        assertEquals(rel2, rel1);
    }

    @Test
    void testEqualsWithDifferentEntityId() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(2L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentRelatedId() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 20L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentMatchType() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.DISCLOSED_RELATION, "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY1", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY2", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFS", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentSourceSummary() {
        Map<String, Integer> summary1a = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary1b = Collections.singletonMap("DS1", 2);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1a, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "MFF", summary1b, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithNull() {
        SzRelationship relationship = createTestRelationship();
        assertNotEquals(null, relationship);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRelationship relationship = createTestRelationship();
        assertNotEquals(relationship, "not a relationship");
    }

    @Test
    void testHashCodeConsistency() {
        SzRelationship rel1 = createTestRelationship();
        SzRelationship rel2 = createTestRelationship();

        assertEquals(rel1.hashCode(), rel2.hashCode());
    }

    @Test
    void testBuildJson() {
        SzRelationship relationship = createTestRelationship();

        JsonObject json = relationship.toJsonObject();

        assertEquals(1L, json.getJsonNumber("entityId").longValue());
        assertEquals(2L, json.getJsonNumber("relatedId").longValue());
        assertEquals("POSSIBLE_MATCH", json.getString("matchType"));
        assertEquals("NAME+DOB", json.getString("matchKey"));
        assertEquals("MFF", json.getString("principle"));
        assertTrue(json.containsKey("sourceSummary"));
        assertTrue(json.containsKey("relatedSummary"));
    }

    @Test
    void testToJsonText() {
        SzRelationship relationship = createTestRelationship();
        String jsonText = relationship.toJsonText();

        assertNotNull(jsonText);
        assertTrue(jsonText.contains("entityId"));
        assertTrue(jsonText.contains("relatedId"));
    }

    @Test
    void testToString() {
        SzRelationship relationship = createTestRelationship();
        String result = relationship.toString();

        assertEquals(relationship.toJsonText(), result);
    }

    @Test
    void testParseJsonObject() {
        SzRelationship original = createTestRelationship();
        JsonObject json = original.toJsonObject();

        SzRelationship parsed = SzRelationship.parse(json);

        assertEquals(original, parsed);
    }

    @Test
    void testParseJsonObjectNull() {
        assertNull(SzRelationship.parse((JsonObject) null));
    }

    @Test
    void testParseString() {
        SzRelationship original = createTestRelationship();
        String jsonText = original.toJsonText();

        SzRelationship parsed = SzRelationship.parse(jsonText);

        assertEquals(original, parsed);
    }

    @Test
    void testParseStringNull() {
        assertNull(SzRelationship.parse((String) null));
    }

    @Test
    void testToHashAndParseHash() {
        SzRelationship original = createTestRelationship();

        String hash = original.toHash();
        assertNotNull(hash);
        assertTrue(hash.length() > 0);

        SzRelationship parsed = SzRelationship.parseHash(hash);
        assertEquals(original, parsed);
    }

    @Test
    void testParseHashNull() {
        assertNull(SzRelationship.parseHash(null));
    }

    @Test
    void testRoundTripThroughJson() {
        SzRelationship original = createTestRelationship();
        JsonObject json = original.toJsonObject();
        SzRelationship parsed = SzRelationship.parse(json);

        assertEquals(original, parsed);
    }

    private SzResolvedEntity createResolvedEntity(long entityId, String dataSource) {
        SzResolvedEntity entity = new SzResolvedEntity();
        entity.setEntityId(entityId);
        entity.setEntityName("Entity " + entityId);
        entity.addRecord(new SzRecord(dataSource, "REC-" + entityId, null, null));
        return entity;
    }

    private SzRelatedEntity createRelatedEntity(long entityId, String dataSource) {
        SzRelatedEntity entity = new SzRelatedEntity();
        entity.setEntityId(entityId);
        entity.setEntityName("Related " + entityId);
        entity.setMatchType(SzMatchType.POSSIBLE_MATCH);
        entity.setMatchKey("NAME+DOB");
        entity.setPrinciple("MFF");
        entity.addRecord(new SzRecord(dataSource, "REC-" + entityId, null, null));
        return entity;
    }

    private SzRelationship createTestRelationship() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        resolved.addRelatedEntity(related);
        return new SzRelationship(resolved, related);
    }
}
