package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzReportRelationTest {

    @Test
    void testDefaultConstructor() {
        SzReportRelation relation = new SzReportRelation();

        assertNull(relation.getEntity());
        assertNull(relation.getRelatedEntity());
        assertNull(relation.getRelationType());
        assertNull(relation.getMatchKey());
        assertNull(relation.getPrinciple());
    }

    @Test
    void testSetAndGetEntity() {
        SzReportRelation relation = new SzReportRelation();
        SzReportEntity entity = new SzReportEntity(100L, "Test");

        relation.setEntity(entity);

        assertNotNull(relation.getEntity());
        assertEquals(100L, relation.getEntity().getEntityId());
        assertEquals("Test", relation.getEntity().getEntityName());
    }

    @Test
    void testSetAndGetRelatedEntity() {
        SzReportRelation relation = new SzReportRelation();
        SzReportEntity related = new SzReportEntity(200L, "Related");

        relation.setRelatedEntity(related);

        assertNotNull(relation.getRelatedEntity());
        assertEquals(200L, relation.getRelatedEntity().getEntityId());
        assertEquals("Related", relation.getRelatedEntity().getEntityName());
    }

    @Test
    void testSetAndGetRelationType() {
        SzReportRelation relation = new SzReportRelation();
        relation.setRelationType(SzRelationType.POSSIBLE_MATCH);

        assertEquals(SzRelationType.POSSIBLE_MATCH, relation.getRelationType());
    }

    @Test
    void testSetAndGetMatchKey() {
        SzReportRelation relation = new SzReportRelation();
        relation.setMatchKey("NAME+DOB");

        assertEquals("NAME+DOB", relation.getMatchKey());
    }

    @Test
    void testSetAndGetPrinciple() {
        SzReportRelation relation = new SzReportRelation();
        relation.setPrinciple("MFF");

        assertEquals("MFF", relation.getPrinciple());
    }

    @Test
    void testEqualsWithSameReference() {
        SzReportRelation relation = createTestRelation();
        assertEquals(relation, relation);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();

        assertEquals(relation1, relation2);
        assertEquals(relation2, relation1);
    }

    @Test
    void testEqualsWithDifferentEntity() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();
        relation2.setEntity(new SzReportEntity(999L, "Different"));

        assertNotEquals(relation1, relation2);
    }

    @Test
    void testEqualsWithDifferentRelatedEntity() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();
        relation2.setRelatedEntity(new SzReportEntity(999L, "Different"));

        assertNotEquals(relation1, relation2);
    }

    @Test
    void testEqualsWithDifferentRelationType() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();
        relation2.setRelationType(SzRelationType.DISCLOSED_RELATION);

        assertNotEquals(relation1, relation2);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();
        relation2.setMatchKey("ADDRESS");

        assertNotEquals(relation1, relation2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();
        relation2.setPrinciple("MFS");

        assertNotEquals(relation1, relation2);
    }

    @Test
    void testEqualsWithNull() {
        SzReportRelation relation = createTestRelation();
        assertNotEquals(null, relation);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzReportRelation relation = createTestRelation();
        assertNotEquals(relation, "not a relation");
    }

    @Test
    void testHashCodeConsistency() {
        SzReportRelation relation1 = createTestRelation();
        SzReportRelation relation2 = createTestRelation();

        assertEquals(relation1.hashCode(), relation2.hashCode());
    }

    @Test
    void testToString() {
        SzReportRelation relation = createTestRelation();
        String result = relation.toString();

        assertNotNull(result);
        assertTrue(result.contains("POSSIBLE_MATCH"));
        assertTrue(result.contains("NAME+DOB"));
        assertTrue(result.contains("MFF"));
    }

    @Test
    void testToStringWithNullValues() {
        SzReportRelation relation = new SzReportRelation();

        assertDoesNotThrow(() -> relation.toString());
    }

    @Test
    void testSerializable() {
        SzReportRelation relation = new SzReportRelation();
        assertTrue(relation instanceof java.io.Serializable);
    }

    private SzReportRelation createTestRelation() {
        SzReportRelation relation = new SzReportRelation();
        relation.setEntity(new SzReportEntity(100L, "Entity1"));
        relation.setRelatedEntity(new SzReportEntity(200L, "Entity2"));
        relation.setRelationType(SzRelationType.POSSIBLE_MATCH);
        relation.setMatchKey("NAME+DOB");
        relation.setPrinciple("MFF");
        return relation;
    }
}
