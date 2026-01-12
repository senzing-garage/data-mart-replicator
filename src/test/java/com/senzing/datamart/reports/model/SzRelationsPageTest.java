package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzRelationsPageTest {

    @Test
    void testDefaultConstructor() {
        SzRelationsPage page = new SzRelationsPage();

        assertEquals("0:0", page.getBound());
        assertNull(page.getBoundType());
        assertEquals(0, page.getPageSize());
        assertNull(page.getSampleSize());
        assertNull(page.getMinimumValue());
        assertNull(page.getMaximumValue());
        assertEquals(0L, page.getTotalRelationCount());
        assertEquals(0L, page.getBeforePageCount());
        assertEquals(0L, page.getAfterPageCount());
        assertNotNull(page.getRelations());
        assertTrue(page.getRelations().isEmpty());
    }

    @Test
    void testSetAndGetBound() {
        SzRelationsPage page = new SzRelationsPage();
        page.setBound("100:200");

        assertEquals("100:200", page.getBound());
    }

    @Test
    void testSetAndGetBoundType() {
        SzRelationsPage page = new SzRelationsPage();
        page.setBoundType(SzBoundType.EXCLUSIVE_LOWER);

        assertEquals(SzBoundType.EXCLUSIVE_LOWER, page.getBoundType());
    }

    @Test
    void testSetAndGetPageSize() {
        SzRelationsPage page = new SzRelationsPage();
        page.setPageSize(25);

        assertEquals(25, page.getPageSize());
    }

    @Test
    void testSetAndGetSampleSize() {
        SzRelationsPage page = new SzRelationsPage();
        page.setSampleSize(10);

        assertEquals(Integer.valueOf(10), page.getSampleSize());
    }

    @Test
    void testSetAndGetTotalRelationCount() {
        SzRelationsPage page = new SzRelationsPage();
        page.setTotalRelationCount(1000L);

        assertEquals(1000L, page.getTotalRelationCount());
    }

    @Test
    void testSetAndGetBeforePageCount() {
        SzRelationsPage page = new SzRelationsPage();
        page.setBeforePageCount(500L);

        assertEquals(500L, page.getBeforePageCount());
    }

    @Test
    void testSetAndGetAfterPageCount() {
        SzRelationsPage page = new SzRelationsPage();
        page.setAfterPageCount(300L);

        assertEquals(300L, page.getAfterPageCount());
    }

    @Test
    void testSetAndGetPageMinimumValue() {
        SzRelationsPage page = new SzRelationsPage();
        page.setPageMinimumValue("100:200");

        assertEquals("100:200", page.getPageMinimumValue());
    }

    @Test
    void testSetAndGetPageMaximumValue() {
        SzRelationsPage page = new SzRelationsPage();
        page.setPageMaximumValue("500:600");

        assertEquals("500:600", page.getPageMaximumValue());
    }

    @Test
    void testAddRelation() {
        SzRelationsPage page = new SzRelationsPage();
        SzReportRelation relation = createTestRelation(100L, 200L);

        page.addRelation(relation);

        List<SzReportRelation> relations = page.getRelations();
        assertEquals(1, relations.size());
        assertEquals(100L, relations.get(0).getEntity().getEntityId());
        assertEquals(200L, relations.get(0).getRelatedEntity().getEntityId());
    }

    @Test
    void testAddRelationReplacesExisting() {
        SzRelationsPage page = new SzRelationsPage();

        SzReportRelation relation1 = createTestRelation(100L, 200L);
        relation1.setMatchKey("OLD");
        page.addRelation(relation1);

        SzReportRelation relation2 = createTestRelation(100L, 200L);
        relation2.setMatchKey("NEW");
        page.addRelation(relation2);

        List<SzReportRelation> relations = page.getRelations();
        assertEquals(1, relations.size());
        assertEquals("NEW", relations.get(0).getMatchKey());
    }

    @Test
    void testSetRelations() {
        SzRelationsPage page = new SzRelationsPage();

        SzReportRelation relation1 = createTestRelation(100L, 200L);
        SzReportRelation relation2 = createTestRelation(300L, 400L);

        page.setRelations(Arrays.asList(relation1, relation2));

        assertEquals(2, page.getRelations().size());
    }

    @Test
    void testSetRelationsWithNull() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(100L, 200L));

        page.setRelations(null);

        assertTrue(page.getRelations().isEmpty());
    }

    @Test
    void testGetMinimumValueWithRelations() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(100L, 200L));
        page.addRelation(createTestRelation(50L, 150L));
        page.addRelation(createTestRelation(200L, 300L));

        assertEquals("50:150", page.getMinimumValue());
    }

    @Test
    void testGetMaximumValueWithRelations() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(100L, 200L));
        page.addRelation(createTestRelation(50L, 150L));
        page.addRelation(createTestRelation(200L, 300L));

        assertEquals("200:300", page.getMaximumValue());
    }

    @Test
    void testGetPageMinimumValueFallsBackToMinimumValue() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(50L, 100L));
        page.addRelation(createTestRelation(100L, 200L));

        assertEquals("50:100", page.getPageMinimumValue());
    }

    @Test
    void testGetPageMaximumValueFallsBackToMaximumValue() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(50L, 100L));
        page.addRelation(createTestRelation(100L, 200L));

        assertEquals("100:200", page.getPageMaximumValue());
    }

    @Test
    void testGetRelationsSortedAscending() {
        SzRelationsPage page = new SzRelationsPage();
        page.addRelation(createTestRelation(200L, 300L));
        page.addRelation(createTestRelation(100L, 200L));
        page.addRelation(createTestRelation(150L, 250L));

        List<SzReportRelation> relations = page.getRelations();
        assertEquals(100L, relations.get(0).getEntity().getEntityId());
        assertEquals(150L, relations.get(1).getEntity().getEntityId());
        assertEquals(200L, relations.get(2).getEntity().getEntityId());
    }

    @Test
    void testEqualsWithSameReference() {
        SzRelationsPage page = new SzRelationsPage();
        assertEquals(page, page);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRelationsPage page1 = createTestPage();
        SzRelationsPage page2 = createTestPage();

        assertEquals(page1, page2);
        assertEquals(page2, page1);
    }

    @Test
    void testEqualsWithDifferentBound() {
        SzRelationsPage page1 = createTestPage();
        SzRelationsPage page2 = createTestPage();
        page2.setBound("999:999");

        assertNotEquals(page1, page2);
    }

    @Test
    void testEqualsWithDifferentBoundType() {
        SzRelationsPage page1 = createTestPage();
        SzRelationsPage page2 = createTestPage();
        page2.setBoundType(SzBoundType.INCLUSIVE_LOWER);

        assertNotEquals(page1, page2);
    }

    @Test
    void testEqualsWithNull() {
        SzRelationsPage page = createTestPage();
        assertNotEquals(null, page);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRelationsPage page = createTestPage();
        assertNotEquals(page, "not a page");
    }

    @Test
    void testHashCodeConsistency() {
        SzRelationsPage page1 = createTestPage();
        SzRelationsPage page2 = createTestPage();

        assertEquals(page1.hashCode(), page2.hashCode());
    }

    @Test
    void testToString() {
        SzRelationsPage page = createTestPage();
        String result = page.toString();

        assertNotNull(result);
        assertTrue(result.contains("100:200"));
        assertTrue(result.contains("EXCLUSIVE_LOWER"));
    }

    @Test
    void testToStringWithNullValues() {
        SzRelationsPage page = new SzRelationsPage();

        assertDoesNotThrow(() -> page.toString());
    }

    @Test
    void testSerializable() {
        SzRelationsPage page = new SzRelationsPage();
        assertTrue(page instanceof java.io.Serializable);
    }

    private SzReportRelation createTestRelation(long entityId, long relatedId) {
        SzReportRelation relation = new SzReportRelation();
        relation.setEntity(new SzReportEntity(entityId, "Entity " + entityId));
        relation.setRelatedEntity(new SzReportEntity(relatedId, "Related " + relatedId));
        relation.setRelationType(SzRelationType.POSSIBLE_MATCH);
        relation.setMatchKey("NAME+DOB");
        relation.setPrinciple("MFF");
        return relation;
    }

    private SzRelationsPage createTestPage() {
        SzRelationsPage page = new SzRelationsPage();
        page.setBound("100:200");
        page.setBoundType(SzBoundType.EXCLUSIVE_LOWER);
        page.setPageSize(25);
        page.setTotalRelationCount(500L);
        page.setBeforePageCount(100L);
        page.setAfterPageCount(375L);
        page.addRelation(createTestRelation(100L, 200L));
        return page;
    }
}
