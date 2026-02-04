package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzEntitiesPageTest {

    @Test
    void testDefaultConstructor() {
        SzEntitiesPage page = new SzEntitiesPage();

        assertNull(page.getBound());
        assertNull(page.getBoundType());
        assertEquals(0, page.getPageSize());
        assertNull(page.getSampleSize());
        assertNull(page.getMinimumValue());
        assertNull(page.getMaximumValue());
        assertEquals(0L, page.getTotalEntityCount());
        assertEquals(0L, page.getBeforePageCount());
        assertEquals(0L, page.getAfterPageCount());
        assertNotNull(page.getEntities());
        assertTrue(page.getEntities().isEmpty());
    }

    @Test
    void testSetAndGetBound() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setBound("1000");

        assertEquals("1000", page.getBound());
    }

    @Test
    void testSetAndGetBoundType() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setBoundType(SzBoundType.EXCLUSIVE_LOWER);

        assertEquals(SzBoundType.EXCLUSIVE_LOWER, page.getBoundType());
    }

    @Test
    void testSetAndGetPageSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setPageSize(25);

        assertEquals(25, page.getPageSize());
    }

    @Test
    void testSetAndGetSampleSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setSampleSize(10);

        assertEquals(Integer.valueOf(10), page.getSampleSize());
    }

    @Test
    void testSetAndGetTotalEntityCount() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setTotalEntityCount(1000L);

        assertEquals(1000L, page.getTotalEntityCount());
    }

    @Test
    void testSetAndGetBeforePageCount() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setBeforePageCount(500L);

        assertEquals(500L, page.getBeforePageCount());
    }

    @Test
    void testSetAndGetAfterPageCount() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setAfterPageCount(300L);

        assertEquals(300L, page.getAfterPageCount());
    }

    @Test
    void testSetAndGetPageMinimumValueWithSampleSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setSampleSize(10);
        page.setPageMinimumValue(100L);

        assertEquals(Long.valueOf(100L), page.getPageMinimumValue());
    }

    @Test
    void testSetAndGetPageMinimumValueWithoutSampleSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setPageMinimumValue(100L);

        assertNull(page.getPageMinimumValue(), "Page min should be null if no sample size");
    }

    @Test
    void testSetAndGetPageMaximumValueWithSampleSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setSampleSize(10);
        page.setPageMaximumValue(200L);

        assertEquals(Long.valueOf(200L), page.getPageMaximumValue());
    }


    @Test
    void testSetAndGetPageMaximumValueWithoutSampleSize() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setPageMaximumValue(200L);

        assertNull(page.getPageMaximumValue(), "Page max should be null if no sample size");
    }

    @Test
    void testAddEntity() {
        SzEntitiesPage page = new SzEntitiesPage();
        SzReportEntity entity = new SzReportEntity(100L, "Test");

        page.addEntity(entity);

        List<SzReportEntity> entities = page.getEntities();
        assertEquals(1, entities.size());
        assertEquals(100L, entities.get(0).getEntityId());
    }

    @Test
    void testAddEntityReplacesExisting() {
        SzEntitiesPage page = new SzEntitiesPage();

        SzReportEntity entity1 = new SzReportEntity(100L, "Old Name");
        page.addEntity(entity1);

        SzReportEntity entity2 = new SzReportEntity(100L, "New Name");
        page.addEntity(entity2);

        List<SzReportEntity> entities = page.getEntities();
        assertEquals(1, entities.size());
        assertEquals("New Name", entities.get(0).getEntityName());
    }

    @Test
    void testSetEntities() {
        SzEntitiesPage page = new SzEntitiesPage();

        SzReportEntity entity1 = new SzReportEntity(100L, "Entity1");
        SzReportEntity entity2 = new SzReportEntity(200L, "Entity2");

        page.setEntities(Arrays.asList(entity1, entity2));

        assertEquals(2, page.getEntities().size());
    }

    @Test
    void testSetEntitiesWithNull() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(100L));

        page.setEntities(null);

        assertTrue(page.getEntities().isEmpty());
    }

    @Test
    void testRemoveEntity() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(100L, "Entity1"));
        page.addEntity(new SzReportEntity(200L, "Entity2"));

        page.removeEntity(100L);

        List<SzReportEntity> entities = page.getEntities();
        assertEquals(1, entities.size());
        assertEquals(200L, entities.get(0).getEntityId());
    }

    @Test
    void testRemoveAllEntities() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(100L));
        page.addEntity(new SzReportEntity(200L));

        page.removeAllEntities();

        assertTrue(page.getEntities().isEmpty());
    }

    @Test
    void testGetMinimumValueWithEntities() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(100L));
        page.addEntity(new SzReportEntity(50L));
        page.addEntity(new SzReportEntity(200L));

        assertEquals(Long.valueOf(50L), page.getMinimumValue());
    }

    @Test
    void testGetMaximumValueWithEntities() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(100L));
        page.addEntity(new SzReportEntity(50L));
        page.addEntity(new SzReportEntity(200L));

        assertEquals(Long.valueOf(200L), page.getMaximumValue());
    }

    @Test
    void testGetPageMinimumValueFallsBackToMinimumValue() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(50L));
        page.addEntity(new SzReportEntity(100L));

        assertEquals(Long.valueOf(50L), page.getPageMinimumValue());
    }

    @Test
    void testGetPageMaximumValueFallsBackToMaximumValue() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(50L));
        page.addEntity(new SzReportEntity(100L));

        assertEquals(Long.valueOf(100L), page.getPageMaximumValue());
    }

    @Test
    void testGetEntitiesSortedAscending() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.addEntity(new SzReportEntity(200L));
        page.addEntity(new SzReportEntity(100L));
        page.addEntity(new SzReportEntity(150L));

        List<SzReportEntity> entities = page.getEntities();
        assertEquals(100L, entities.get(0).getEntityId());
        assertEquals(150L, entities.get(1).getEntityId());
        assertEquals(200L, entities.get(2).getEntityId());
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntitiesPage page = new SzEntitiesPage();
        assertEquals(page, page);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntitiesPage page1 = createTestPage();
        SzEntitiesPage page2 = createTestPage();

        assertEquals(page1, page2);
        assertEquals(page2, page1);
    }

    @Test
    void testEqualsWithDifferentBound() {
        SzEntitiesPage page1 = createTestPage();
        SzEntitiesPage page2 = createTestPage();
        page2.setBound("999");

        assertNotEquals(page1, page2);
    }

    @Test
    void testEqualsWithDifferentBoundType() {
        SzEntitiesPage page1 = createTestPage();
        SzEntitiesPage page2 = createTestPage();
        page2.setBoundType(SzBoundType.INCLUSIVE_LOWER);

        assertNotEquals(page1, page2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntitiesPage page = createTestPage();
        assertNotEquals(null, page);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntitiesPage page = createTestPage();
        assertNotEquals(page, "not a page");
    }

    @Test
    void testHashCodeConsistency() {
        SzEntitiesPage page1 = createTestPage();
        SzEntitiesPage page2 = createTestPage();

        assertEquals(page1.hashCode(), page2.hashCode());
    }

    @Test
    void testToString() {
        SzEntitiesPage page = createTestPage();
        String result = page.toString();

        assertNotNull(result);
        assertTrue(result.contains("1000"));
        assertTrue(result.contains("EXCLUSIVE_LOWER"));
    }

    @Test
    void testToStringWithNullValues() {
        SzEntitiesPage page = new SzEntitiesPage();

        assertDoesNotThrow(() -> page.toString());
    }

    @Test
    void testSerializable() {
        SzEntitiesPage page = new SzEntitiesPage();
        assertTrue(page instanceof java.io.Serializable);
    }

    private SzEntitiesPage createTestPage() {
        SzEntitiesPage page = new SzEntitiesPage();
        page.setBound("1000");
        page.setBoundType(SzBoundType.EXCLUSIVE_LOWER);
        page.setPageSize(25);
        page.setTotalEntityCount(500L);
        page.setBeforePageCount(100L);
        page.setAfterPageCount(375L);
        page.addEntity(new SzReportEntity(100L, "Test Entity"));
        return page;
    }
}
