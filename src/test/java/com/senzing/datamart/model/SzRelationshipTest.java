package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.stream.SystemErr;

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
            "NAME+DOB",
            "MFF",
            summary1,
            summary2
        );

        assertEquals(1L, relationship.getEntityId());
        assertEquals(2L, relationship.getRelatedEntityId());
        assertEquals(SzMatchType.POSSIBLE_MATCH, relationship.getMatchType());
        assertEquals("NAME+DOB", relationship.getMatchKey());
        assertEquals("NAME+DOB", relationship.getReverseMatchKey());
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
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, null, "KEY", "MFF", summary, summary));
    }

    @Test
    void testConstructorWithNullReverseMatchKeyThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", null, "MFF", summary, summary));
    }

    @Test
    void testConstructorWithNullPrincipleThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", null, summary, summary));
    }

    @Test
    void testConstructorWithNullMatchTypeThrows() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, null, "KEY", "KEY", "MFF", summary, summary));
    }

    @Test
    void testConstructorWithNullSummary1Throws() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", null, summary));
    }

    @Test
    void testConstructorWithNullSummary2Throws() {
        Map<String, Integer> summary = new LinkedHashMap<>();

        assertThrows(NullPointerException.class, () ->
            new SzRelationship(1L, 2L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary, null));
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

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(2L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentRelatedId() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 20L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentMatchType() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.DISCLOSED_RELATION, "KEY", "KEY", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY1", "KEY1", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY2", "KEY2", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentReverseMatchKey() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.DISCLOSED_RELATION, "+REL_POINTER(A:)", "+REL_POINTER(:A)", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.DISCLOSED_RELATION, "+REL_POINTER(B:)", "+REL_POINTER(:B)", "MFF", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        Map<String, Integer> summary1 = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFS", summary1, summary2);

        assertNotEquals(rel1, rel2);
    }

    @Test
    void testEqualsWithDifferentSourceSummary() {
        Map<String, Integer> summary1a = Collections.singletonMap("DS1", 1);
        Map<String, Integer> summary1b = Collections.singletonMap("DS1", 2);
        Map<String, Integer> summary2 = Collections.singletonMap("DS2", 1);

        SzRelationship rel1 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1a, summary2);
        SzRelationship rel2 = new SzRelationship(1L, 10L, SzMatchType.POSSIBLE_MATCH, "KEY", "KEY", "MFF", summary1b, summary2);

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
        assertEquals("NAME+DOB", json.getString("reverseMatchKey"));
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

    // ========================================================================
    // getReverseMatchKey() tests
    // ========================================================================

    @Test
    void testGetReverseMatchKeyForNonDisclosedRelation() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        related.setMatchType(SzMatchType.POSSIBLE_MATCH);
        related.setMatchKey("NAME+DOB");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        assertEquals("NAME+DOB", relationship.getMatchKey());
        assertEquals("NAME+DOB", relationship.getReverseMatchKey());
    }

    @Test
    void testGetReverseMatchKeyForDisclosedRelationNonRelPointer() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        related.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related.setMatchKey("EMPLOYER");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        assertEquals("EMPLOYER", relationship.getMatchKey());
        assertEquals("EMPLOYER", relationship.getReverseMatchKey());
    }

    @Test
    void testGetReverseMatchKeyForRelPointer() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        related.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related.setMatchKey("+REL_POINTER(ABC:)");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        assertEquals("+REL_POINTER(ABC:)", relationship.getMatchKey());
        assertEquals("+REL_POINTER(:ABC)", relationship.getReverseMatchKey());
    }

    @Test
    void testGetReverseMatchKeyForRelPointerReversed() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        related.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related.setMatchKey("+REL_POINTER(:XYZ)");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        // No flip since 1 < 2, so matchKey stays original, reverseMatchKey is calculated
        assertEquals("+REL_POINTER(:XYZ)", relationship.getMatchKey());
        assertEquals("+REL_POINTER(XYZ:)", relationship.getReverseMatchKey());
    }

    @Test
    void testGetReverseMatchKeyForRelPointerBothSides() {
        SzResolvedEntity resolved = createResolvedEntity(1L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(2L, "VENDORS");
        related.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related.setMatchKey("+REL_POINTER(FOO:BAR)");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        // No flip since 1 < 2, so matchKey stays original, reverseMatchKey is calculated
        assertEquals("+REL_POINTER(FOO:BAR)", relationship.getMatchKey());
        assertEquals("+REL_POINTER(BAR:FOO)", relationship.getReverseMatchKey());
    }

    @Test
    void testGetReverseMatchKeyFlipsWhenEntityIdFlips() {
        SzResolvedEntity resolved = createResolvedEntity(100L, "CUSTOMERS");
        SzRelatedEntity related = createRelatedEntity(50L, "VENDORS");
        related.setMatchType(SzMatchType.DISCLOSED_RELATION);
        related.setMatchKey("+REL_POINTER(ABC:)");
        resolved.addRelatedEntity(related);

        SzRelationship relationship = new SzRelationship(resolved, related);

        // Entity IDs are flipped (50 < 100), so match keys should also flip
        assertEquals(50L, relationship.getEntityId());
        assertEquals(100L, relationship.getRelatedEntityId());
        assertEquals("+REL_POINTER(:ABC)", relationship.getMatchKey());
        assertEquals("+REL_POINTER(ABC:)", relationship.getReverseMatchKey());
    }

    // ========================================================================
    // Static getReverseMatchKey() method tests
    // ========================================================================

    @Test
    void testStaticGetReverseMatchKeyWithNull() {
        assertNull(SzRelationship.getReverseMatchKey(null));
    }

    @Test
    void testStaticGetReverseMatchKeyWithNonRelPointer() {
        String matchKey = "NAME+DOB";
        assertEquals(matchKey, SzRelationship.getReverseMatchKey(matchKey));
    }

    @Test
    void testStaticGetReverseMatchKeyWithRelPointerLeftSide() {
        String matchKey = "+REL_POINTER(ABC:)";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        assertEquals("+REL_POINTER(:ABC)", reversed);
    }

    @Test
    void testStaticGetReverseMatchKeyWithRelPointerRightSide() {
        String matchKey = "+REL_POINTER(:XYZ)";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        assertEquals("+REL_POINTER(XYZ:)", reversed);
    }

    @Test
    void testStaticGetReverseMatchKeyWithRelPointerBothSides() {
        String matchKey = "+REL_POINTER(FOO:BAR)";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        assertEquals("+REL_POINTER(BAR:FOO)", reversed);
    }

    @Test
    void testStaticGetReverseMatchKeyWithRelPointerEmptyBothSides() {
        String matchKey = "+REL_POINTER(:)";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        assertEquals("+REL_POINTER(:)", reversed);
    }

    @Test
    void testStaticGetReverseMatchKeyWithRelPointerMultipleChars() {
        String matchKey = "+REL_POINTER(EMPLOYER:EMPLOYEE)";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        assertEquals("+REL_POINTER(EMPLOYEE:EMPLOYER)", reversed);
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void testStaticGetReverseMatchKeyWithRelPointerTooShort() throws Exception {
        // Less than prefix + 3 chars (colon + parens + 1 other)
        String matchKey = "+REL_POINTER()";

        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            String reversed = SzRelationship.getReverseMatchKey(matchKey);
            // Should return original due to bad format and log warning
            assertEquals(matchKey, reversed);
        });

        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Badly formatted REL_POINTER match key"),
                "Expected warning about badly formatted match key");
        assertTrue(errOutput.contains(matchKey),
                "Warning should include the problematic match key");
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void testStaticGetReverseMatchKeyWithRelPointerNoColon() throws Exception {
        String matchKey = "+REL_POINTER(ABC)";

        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            String reversed = SzRelationship.getReverseMatchKey(matchKey);
            // Should return original due to missing colon and log warning
            assertEquals(matchKey, reversed);
        });

        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Failed to find unescaped colon in REL_POINTER match key"),
                "Expected warning about missing colon");
        assertTrue(errOutput.contains(matchKey),
                "Warning should include the problematic match key");
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void testStaticGetReverseMatchKeyWithRelPointerMultipleColons() throws Exception {
        String matchKey = "+REL_POINTER(A:B:C)";

        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            String reversed = SzRelationship.getReverseMatchKey(matchKey);
            // Should return original due to multiple colons and log warning
            assertEquals(matchKey, reversed);
        });

        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Found more than one colon in REL_POINTER match key"),
                "Expected warning about multiple colons");
        assertTrue(errOutput.contains(matchKey),
                "Warning should include the problematic match key");
    }

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void testStaticGetReverseMatchKeyWithRelPointerNoClosingParen() throws Exception {
        String matchKey = "+REL_POINTER(ABC:";

        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            String reversed = SzRelationship.getReverseMatchKey(matchKey);
            // Should return original due to missing closing paren and log warning
            assertEquals(matchKey, reversed);
        });

        String errOutput = systemErr.getText();
        assertTrue(errOutput.contains("Failed to find closing parentheses in REL_POINTER match key"),
                "Expected warning about missing closing parentheses");
        assertTrue(errOutput.contains(matchKey),
                "Warning should include the problematic match key");
    }

    @Test
    void testStaticGetReverseMatchKeyWithWhitespace() {
        String matchKey = "  +REL_POINTER(ABC:)  ";
        String reversed = SzRelationship.getReverseMatchKey(matchKey);
        // Trims input, trailing whitespace is removed in result
        assertEquals("+REL_POINTER(:ABC)", reversed);
    }

    @Test
    void testStaticGetReverseMatchKeyDoubleReverse() {
        String matchKey = "+REL_POINTER(ABC:)";
        String reversed1 = SzRelationship.getReverseMatchKey(matchKey);
        String reversed2 = SzRelationship.getReverseMatchKey(reversed1);

        assertEquals("+REL_POINTER(:ABC)", reversed1);
        assertEquals("+REL_POINTER(ABC:)", reversed2);
        assertEquals(matchKey, reversed2);
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
