package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzSourceLoadedStatsTest {

    @Test
    void testConstructorWithDataSource() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");

        assertEquals("CUSTOMERS", stats.getDataSource());
        assertEquals(0L, stats.getRecordCount());
        assertEquals(0L, stats.getEntityCount());
        assertEquals(0L, stats.getUnmatchedRecordCount());
    }

    @Test
    void testConstructorWithNullDataSourceThrows() {
        assertThrows(NullPointerException.class, () -> new SzSourceLoadedStats(null));
    }

    @Test
    void testSetAndGetDataSource() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        stats.setDataSource("VENDORS");

        assertEquals("VENDORS", stats.getDataSource());
    }

    @Test
    void testSetAndGetRecordCount() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        stats.setRecordCount(1000L);

        assertEquals(1000L, stats.getRecordCount());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        stats.setEntityCount(500L);

        assertEquals(500L, stats.getEntityCount());
    }

    @Test
    void testSetAndGetUnmatchedRecordCount() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        stats.setUnmatchedRecordCount(200L);

        assertEquals(200L, stats.getUnmatchedRecordCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        assertEquals(stats, stats);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        stats1.setRecordCount(1000L);
        stats1.setEntityCount(500L);
        stats1.setUnmatchedRecordCount(200L);

        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("CUSTOMERS");
        stats2.setRecordCount(1000L);
        stats2.setEntityCount(500L);
        stats2.setUnmatchedRecordCount(200L);

        assertEquals(stats1, stats2);
        assertEquals(stats2, stats1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("VENDORS");

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentRecordCount() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        stats1.setRecordCount(1000L);

        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("CUSTOMERS");
        stats2.setRecordCount(2000L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentEntityCount() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        stats1.setEntityCount(500L);

        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("CUSTOMERS");
        stats2.setEntityCount(600L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentUnmatchedRecordCount() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        stats1.setUnmatchedRecordCount(200L);

        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("CUSTOMERS");
        stats2.setUnmatchedRecordCount(300L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithNull() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        assertNotEquals(null, stats);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        assertNotEquals(stats, "not stats");
    }

    @Test
    void testHashCodeConsistency() {
        SzSourceLoadedStats stats1 = new SzSourceLoadedStats("CUSTOMERS");
        stats1.setRecordCount(1000L);
        stats1.setEntityCount(500L);
        stats1.setUnmatchedRecordCount(200L);

        SzSourceLoadedStats stats2 = new SzSourceLoadedStats("CUSTOMERS");
        stats2.setRecordCount(1000L);
        stats2.setEntityCount(500L);
        stats2.setUnmatchedRecordCount(200L);

        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    @Test
    void testToString() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        stats.setRecordCount(1000L);
        stats.setEntityCount(500L);
        stats.setUnmatchedRecordCount(200L);

        String result = stats.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("1000"));
        assertTrue(result.contains("500"));
        assertTrue(result.contains("200"));
    }

    @Test
    void testSerializable() {
        SzSourceLoadedStats stats = new SzSourceLoadedStats("CUSTOMERS");
        assertTrue(stats instanceof java.io.Serializable);
    }
}
