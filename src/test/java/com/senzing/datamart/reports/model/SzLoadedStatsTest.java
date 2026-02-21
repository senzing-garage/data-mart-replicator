package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzLoadedStatsTest {

    @Test
    void testDefaultConstructor() {
        SzLoadedStats stats = new SzLoadedStats();

        assertEquals(0L, stats.getTotalRecordCount());
        assertEquals(0L, stats.getTotalEntityCount());
        assertEquals(0L, stats.getTotalUnmatchedRecordCount());
        assertNotNull(stats.getDataSourceCounts());
        assertTrue(stats.getDataSourceCounts().isEmpty());
    }

    @Test
    void testSetAndGetTotalRecordCount() {
        SzLoadedStats stats = new SzLoadedStats();
        stats.setTotalRecordCount(1000L);

        assertEquals(1000L, stats.getTotalRecordCount());
    }

    @Test
    void testSetAndGetTotalEntityCount() {
        SzLoadedStats stats = new SzLoadedStats();
        stats.setTotalEntityCount(500L);

        assertEquals(500L, stats.getTotalEntityCount());
    }

    @Test
    void testSetAndGetTotalUnmatchedRecordCount() {
        SzLoadedStats stats = new SzLoadedStats();
        stats.setTotalUnmatchedRecordCount(200L);

        assertEquals(200L, stats.getTotalUnmatchedRecordCount());
    }

    @Test
    void testAddDataSourceCount() {
        SzLoadedStats stats = new SzLoadedStats();
        SzSourceLoadedStats sourceStats = new SzSourceLoadedStats("CUSTOMERS");
        sourceStats.setRecordCount(100L);

        stats.addDataSourceCount(sourceStats);

        List<SzSourceLoadedStats> result = stats.getDataSourceCounts();
        assertEquals(1, result.size());
        assertEquals("CUSTOMERS", result.get(0).getDataSource());
    }

    @Test
    void testAddDataSourceCountWithNull() {
        SzLoadedStats stats = new SzLoadedStats();

        stats.addDataSourceCount(null);

        assertTrue(stats.getDataSourceCounts().isEmpty());
    }

    @Test
    void testAddDataSourceCountReplacesExisting() {
        SzLoadedStats stats = new SzLoadedStats();

        SzSourceLoadedStats sourceStats1 = new SzSourceLoadedStats("CUSTOMERS");
        sourceStats1.setRecordCount(100L);
        stats.addDataSourceCount(sourceStats1);

        SzSourceLoadedStats sourceStats2 = new SzSourceLoadedStats("CUSTOMERS");
        sourceStats2.setRecordCount(200L);
        stats.addDataSourceCount(sourceStats2);

        List<SzSourceLoadedStats> result = stats.getDataSourceCounts();
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getRecordCount());
    }

    @Test
    void testSetDataSourceCounts() {
        SzLoadedStats stats = new SzLoadedStats();

        SzSourceLoadedStats sourceStats1 = new SzSourceLoadedStats("CUSTOMERS");
        SzSourceLoadedStats sourceStats2 = new SzSourceLoadedStats("VENDORS");

        stats.setDataSourceCounts(Arrays.asList(sourceStats1, sourceStats2));

        assertEquals(2, stats.getDataSourceCounts().size());
    }

    @Test
    void testSetDataSourceCountsWithNull() {
        SzLoadedStats stats = new SzLoadedStats();
        stats.addDataSourceCount(new SzSourceLoadedStats("CUSTOMERS"));

        stats.setDataSourceCounts(null);

        assertTrue(stats.getDataSourceCounts().isEmpty());
    }

    @Test
    void testSetDataSourceCountsWithNullElementsIgnored() {
        SzLoadedStats stats = new SzLoadedStats();

        SzSourceLoadedStats sourceStats1 = new SzSourceLoadedStats("CUSTOMERS");

        stats.setDataSourceCounts(Arrays.asList(sourceStats1, null));

        assertEquals(1, stats.getDataSourceCounts().size());
    }

    @Test
    void testEqualsWithSameReference() {
        SzLoadedStats stats = createTestLoadedStats();
        assertEquals(stats, stats);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();

        assertEquals(stats1, stats2);
        assertEquals(stats2, stats1);
    }

    @Test
    void testEqualsWithDifferentTotalRecordCount() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();
        stats2.setTotalRecordCount(9999L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentTotalEntityCount() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();
        stats2.setTotalEntityCount(9999L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentTotalUnmatchedRecordCount() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();
        stats2.setTotalUnmatchedRecordCount(9999L);

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithDifferentDataSourceCounts() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();
        stats2.addDataSourceCount(new SzSourceLoadedStats("EXTRA"));

        assertNotEquals(stats1, stats2);
    }

    @Test
    void testEqualsWithNull() {
        SzLoadedStats stats = createTestLoadedStats();
        assertNotEquals(null, stats);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzLoadedStats stats = createTestLoadedStats();
        assertNotEquals(stats, "not stats");
    }

    @Test
    void testHashCodeConsistency() {
        SzLoadedStats stats1 = createTestLoadedStats();
        SzLoadedStats stats2 = createTestLoadedStats();

        assertEquals(stats1.hashCode(), stats2.hashCode());
    }

    @Test
    void testToString() {
        SzLoadedStats stats = createTestLoadedStats();
        String result = stats.toString();

        assertNotNull(result);
        assertTrue(result.contains("1000"));
        assertTrue(result.contains("500"));
        assertTrue(result.contains("200"));
    }

    @Test
    void testToStringWithNullValues() {
        SzLoadedStats stats = new SzLoadedStats();

        assertDoesNotThrow(() -> stats.toString());
    }

    @Test
    void testSerializable() {
        SzLoadedStats stats = new SzLoadedStats();
        assertTrue(stats instanceof java.io.Serializable);
    }

    private SzLoadedStats createTestLoadedStats() {
        SzLoadedStats stats = new SzLoadedStats();
        stats.setTotalRecordCount(1000L);
        stats.setTotalEntityCount(500L);
        stats.setTotalUnmatchedRecordCount(200L);

        SzSourceLoadedStats sourceStats = new SzSourceLoadedStats("CUSTOMERS");
        sourceStats.setRecordCount(100L);
        sourceStats.setEntityCount(50L);
        sourceStats.setUnmatchedRecordCount(20L);
        stats.addDataSourceCount(sourceStats);

        return stats;
    }
}
