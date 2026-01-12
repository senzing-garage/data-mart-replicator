package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzSummaryStatsTest {

    @Test
    void testDefaultConstructor() {
        SzSummaryStats stats = new SzSummaryStats();

        assertNotNull(stats.getSourceSummaries());
        assertTrue(stats.getSourceSummaries().isEmpty());
    }

    @Test
    void testAddSourceSummary() {
        SzSummaryStats stats = new SzSummaryStats();
        SzSourceSummary sourceSummary = new SzSourceSummary("CUSTOMERS");

        stats.addSourceSummary(sourceSummary);

        List<SzSourceSummary> result = stats.getSourceSummaries();
        assertEquals(1, result.size());
        assertEquals("CUSTOMERS", result.get(0).getDataSource());
    }

    @Test
    void testAddSourceSummaryWithNull() {
        SzSummaryStats stats = new SzSummaryStats();

        stats.addSourceSummary(null);

        assertTrue(stats.getSourceSummaries().isEmpty());
    }

    @Test
    void testAddSourceSummaryReplacesExisting() {
        SzSummaryStats stats = new SzSummaryStats();

        SzSourceSummary sourceSummary1 = new SzSourceSummary("CUSTOMERS");
        sourceSummary1.setRecordCount(100L);
        stats.addSourceSummary(sourceSummary1);

        SzSourceSummary sourceSummary2 = new SzSourceSummary("CUSTOMERS");
        sourceSummary2.setRecordCount(200L);
        stats.addSourceSummary(sourceSummary2);

        List<SzSourceSummary> result = stats.getSourceSummaries();
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getRecordCount());
    }

    @Test
    void testSetSourceSummaries() {
        SzSummaryStats stats = new SzSummaryStats();

        SzSourceSummary sourceSummary1 = new SzSourceSummary("CUSTOMERS");
        SzSourceSummary sourceSummary2 = new SzSourceSummary("VENDORS");

        stats.setSourceSummaries(Arrays.asList(sourceSummary1, sourceSummary2));

        assertEquals(2, stats.getSourceSummaries().size());
    }

    @Test
    void testSetSourceSummariesWithNull() {
        SzSummaryStats stats = new SzSummaryStats();
        stats.addSourceSummary(new SzSourceSummary("CUSTOMERS"));

        stats.setSourceSummaries(null);

        assertTrue(stats.getSourceSummaries().isEmpty());
    }

    @Test
    void testSetSourceSummariesWithNullElementsIgnored() {
        SzSummaryStats stats = new SzSummaryStats();

        SzSourceSummary sourceSummary = new SzSourceSummary("CUSTOMERS");

        stats.setSourceSummaries(Arrays.asList(sourceSummary, null));

        assertEquals(1, stats.getSourceSummaries().size());
    }

    @Test
    void testRemoveSourceSummary() {
        SzSummaryStats stats = new SzSummaryStats();
        stats.addSourceSummary(new SzSourceSummary("CUSTOMERS"));
        stats.addSourceSummary(new SzSourceSummary("VENDORS"));

        stats.removeSourceSummary("CUSTOMERS");

        List<SzSourceSummary> result = stats.getSourceSummaries();
        assertEquals(1, result.size());
        assertEquals("VENDORS", result.get(0).getDataSource());
    }

    @Test
    void testRemoveAllSourceSummaries() {
        SzSummaryStats stats = new SzSummaryStats();
        stats.addSourceSummary(new SzSourceSummary("CUSTOMERS"));
        stats.addSourceSummary(new SzSourceSummary("VENDORS"));

        stats.removeAllSourceSummaries();

        assertTrue(stats.getSourceSummaries().isEmpty());
    }

    @Test
    void testGetSourceSummariesReturnsSortedList() {
        SzSummaryStats stats = new SzSummaryStats();
        stats.addSourceSummary(new SzSourceSummary("VENDORS"));
        stats.addSourceSummary(new SzSourceSummary("CUSTOMERS"));
        stats.addSourceSummary(new SzSourceSummary("EMPLOYEES"));

        List<SzSourceSummary> result = stats.getSourceSummaries();
        assertEquals(3, result.size());
        assertEquals("CUSTOMERS", result.get(0).getDataSource());
        assertEquals("EMPLOYEES", result.get(1).getDataSource());
        assertEquals("VENDORS", result.get(2).getDataSource());
    }

    @Test
    void testToString() {
        SzSummaryStats stats = createTestSummaryStats();
        String result = stats.toString();

        assertNotNull(result);
        assertTrue(result.contains("sourceSummaries"));
    }

    @Test
    void testToStringWithNullValues() {
        SzSummaryStats stats = new SzSummaryStats();

        assertDoesNotThrow(() -> stats.toString());
    }

    @Test
    void testSerializable() {
        SzSummaryStats stats = new SzSummaryStats();
        assertTrue(stats instanceof java.io.Serializable);
    }

    private SzSummaryStats createTestSummaryStats() {
        SzSummaryStats stats = new SzSummaryStats();

        SzSourceSummary sourceSummary = new SzSourceSummary("CUSTOMERS");
        sourceSummary.setRecordCount(1000L);
        sourceSummary.setEntityCount(500L);
        sourceSummary.setUnmatchedRecordCount(200L);
        stats.addSourceSummary(sourceSummary);

        return stats;
    }
}
