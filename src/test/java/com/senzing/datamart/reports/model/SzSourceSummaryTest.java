package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzSourceSummaryTest {

    @Test
    void testDefaultConstructor() {
        SzSourceSummary summary = new SzSourceSummary();

        assertNull(summary.getDataSource());
        assertEquals(0L, summary.getRecordCount());
        assertEquals(0L, summary.getEntityCount());
        assertEquals(0L, summary.getUnmatchedRecordCount());
        assertNotNull(summary.getCrossSourceSummaries());
        assertTrue(summary.getCrossSourceSummaries().isEmpty());
    }

    @Test
    void testConstructorWithDataSource() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");

        assertEquals("CUSTOMERS", summary.getDataSource());
        assertEquals(0L, summary.getRecordCount());
        assertEquals(0L, summary.getEntityCount());
        assertEquals(0L, summary.getUnmatchedRecordCount());
    }

    @Test
    void testSetAndGetDataSource() {
        SzSourceSummary summary = new SzSourceSummary();
        summary.setDataSource("VENDORS");

        assertEquals("VENDORS", summary.getDataSource());
    }

    @Test
    void testSetAndGetRecordCount() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.setRecordCount(1000L);

        assertEquals(1000L, summary.getRecordCount());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.setEntityCount(500L);

        assertEquals(500L, summary.getEntityCount());
    }

    @Test
    void testSetAndGetUnmatchedRecordCount() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.setUnmatchedRecordCount(200L);

        assertEquals(200L, summary.getUnmatchedRecordCount());
    }

    @Test
    void testAddCrossSourceSummary() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        SzCrossSourceSummary crossSummary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.addCrossSourceSummary(crossSummary);

        List<SzCrossSourceSummary> result = summary.getCrossSourceSummaries();
        assertEquals(1, result.size());
        assertEquals("VENDORS", result.get(0).getVersusDataSource());
    }

    @Test
    void testAddCrossSourceSummaryWithNull() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");

        summary.addCrossSourceSummary(null);

        assertTrue(summary.getCrossSourceSummaries().isEmpty());
    }

    @Test
    void testAddCrossSourceSummaryReplacesExisting() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");

        SzCrossSourceSummary crossSummary1 = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        crossSummary1.addMatches(createTestMatchCounts("NAME+DOB", "MFF"));
        summary.addCrossSourceSummary(crossSummary1);

        SzCrossSourceSummary crossSummary2 = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        crossSummary2.addMatches(createTestMatchCounts("ADDRESS", "MFS"));
        summary.addCrossSourceSummary(crossSummary2);

        List<SzCrossSourceSummary> result = summary.getCrossSourceSummaries();
        assertEquals(1, result.size());
        assertEquals("ADDRESS", result.get(0).getMatches().get(0).getMatchKey());
    }

    @Test
    void testSetCrossSourceSummaries() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");

        SzCrossSourceSummary crossSummary1 = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");
        SzCrossSourceSummary crossSummary2 = new SzCrossSourceSummary("CUSTOMERS", "EMPLOYEES");

        summary.setCrossSourceSummaries(Arrays.asList(crossSummary1, crossSummary2));

        assertEquals(2, summary.getCrossSourceSummaries().size());
    }

    @Test
    void testSetCrossSourceSummariesWithNull() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "VENDORS"));

        summary.setCrossSourceSummaries(null);

        assertTrue(summary.getCrossSourceSummaries().isEmpty());
    }

    @Test
    void testSetCrossSourceSummariesWithNullElementsIgnored() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");

        SzCrossSourceSummary crossSummary = new SzCrossSourceSummary("CUSTOMERS", "VENDORS");

        summary.setCrossSourceSummaries(Arrays.asList(crossSummary, null));

        assertEquals(1, summary.getCrossSourceSummaries().size());
    }

    @Test
    void testRemoveCrossSourceSummary() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "VENDORS"));
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "EMPLOYEES"));

        summary.removeCrossSourceSummary("VENDORS");

        List<SzCrossSourceSummary> result = summary.getCrossSourceSummaries();
        assertEquals(1, result.size());
        assertEquals("EMPLOYEES", result.get(0).getVersusDataSource());
    }

    @Test
    void testRemoveAllCrossSourceSummaries() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "VENDORS"));
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "EMPLOYEES"));

        summary.removeAllCrossSourceSummaries();

        assertTrue(summary.getCrossSourceSummaries().isEmpty());
    }

    @Test
    void testToString() {
        SzSourceSummary summary = createTestSourceSummary();
        String result = summary.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("1000"));
        assertTrue(result.contains("500"));
        assertTrue(result.contains("200"));
    }

    @Test
    void testToStringWithNullValues() {
        SzSourceSummary summary = new SzSourceSummary();

        assertDoesNotThrow(() -> summary.toString());
    }

    @Test
    void testSerializable() {
        SzSourceSummary summary = new SzSourceSummary();
        assertTrue(summary instanceof java.io.Serializable);
    }

    private SzMatchCounts createTestMatchCounts(String matchKey, String principle) {
        SzMatchCounts counts = new SzMatchCounts(matchKey, principle);
        counts.setEntityCount(100L);
        counts.setRecordCount(200L);
        return counts;
    }

    private SzSourceSummary createTestSourceSummary() {
        SzSourceSummary summary = new SzSourceSummary("CUSTOMERS");
        summary.setRecordCount(1000L);
        summary.setEntityCount(500L);
        summary.setUnmatchedRecordCount(200L);
        summary.addCrossSourceSummary(new SzCrossSourceSummary("CUSTOMERS", "VENDORS"));
        return summary;
    }
}
