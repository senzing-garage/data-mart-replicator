package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzCrossSourceMatchCountsTest {

    @Test
    void testDefaultConstructor() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts();

        assertNull(counts.getDataSource());
        assertNull(counts.getVersusDataSource());
        assertNotNull(counts.getCounts());
        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testConstructorWithDataSources() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");

        assertEquals("CUSTOMERS", counts.getDataSource());
        assertEquals("VENDORS", counts.getVersusDataSource());
        assertNotNull(counts.getCounts());
        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testSetAndGetDataSource() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts();
        counts.setDataSource("CUSTOMERS");

        assertEquals("CUSTOMERS", counts.getDataSource());
    }

    @Test
    void testSetAndGetVersusDataSource() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts();
        counts.setVersusDataSource("VENDORS");

        assertEquals("VENDORS", counts.getVersusDataSource());
    }

    @Test
    void testAddCounts() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");
        SzMatchCounts matchCounts = createTestMatchCounts("NAME+DOB", "MFF");

        counts.addCounts(matchCounts);

        List<SzMatchCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
        assertEquals("MFF", result.get(0).getPrinciple());
    }

    @Test
    void testAddCountsWithNull() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");

        counts.addCounts(null);

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testAddCountsReplacesExisting() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");

        SzMatchCounts matchCounts1 = createTestMatchCounts("NAME+DOB", "MFF");
        matchCounts1.setEntityCount(100L);
        counts.addCounts(matchCounts1);

        SzMatchCounts matchCounts2 = createTestMatchCounts("NAME+DOB", "MFF");
        matchCounts2.setEntityCount(200L);
        counts.addCounts(matchCounts2);

        List<SzMatchCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getEntityCount());
    }

    @Test
    void testSetCounts() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");

        SzMatchCounts matchCounts1 = createTestMatchCounts("NAME+DOB", "MFF");
        SzMatchCounts matchCounts2 = createTestMatchCounts("ADDRESS", "MFS");

        counts.setCounts(Arrays.asList(matchCounts1, matchCounts2));

        assertEquals(2, counts.getCounts().size());
    }

    @Test
    void testSetCountsWithNull() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");
        counts.addCounts(createTestMatchCounts("NAME+DOB", "MFF"));

        counts.setCounts(null);

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testRemoveCounts() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");
        counts.addCounts(createTestMatchCounts("NAME+DOB", "MFF"));
        counts.addCounts(createTestMatchCounts("ADDRESS", "MFS"));

        counts.removeCounts("NAME+DOB", "MFF");

        List<SzMatchCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals("ADDRESS", result.get(0).getMatchKey());
    }

    @Test
    void testRemoveAllCounts() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");
        counts.addCounts(createTestMatchCounts("NAME+DOB", "MFF"));
        counts.addCounts(createTestMatchCounts("ADDRESS", "MFS"));

        counts.removeAllCounts();

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testEqualsWithSameReference() {
        SzCrossSourceMatchCounts counts = createTestCrossSourceMatchCounts();
        assertEquals(counts, counts);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzCrossSourceMatchCounts counts1 = createTestCrossSourceMatchCounts();
        SzCrossSourceMatchCounts counts2 = createTestCrossSourceMatchCounts();

        assertEquals(counts1, counts2);
        assertEquals(counts2, counts1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzCrossSourceMatchCounts counts1 = createTestCrossSourceMatchCounts();
        SzCrossSourceMatchCounts counts2 = createTestCrossSourceMatchCounts();
        counts2.setDataSource("DIFFERENT");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentVersusDataSource() {
        SzCrossSourceMatchCounts counts1 = createTestCrossSourceMatchCounts();
        SzCrossSourceMatchCounts counts2 = createTestCrossSourceMatchCounts();
        counts2.setVersusDataSource("DIFFERENT");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentCounts() {
        SzCrossSourceMatchCounts counts1 = createTestCrossSourceMatchCounts();
        SzCrossSourceMatchCounts counts2 = createTestCrossSourceMatchCounts();
        counts2.addCounts(createTestMatchCounts("EXTRA", "RULE"));

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithNull() {
        SzCrossSourceMatchCounts counts = createTestCrossSourceMatchCounts();
        assertNotEquals(null, counts);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzCrossSourceMatchCounts counts = createTestCrossSourceMatchCounts();
        assertNotEquals(counts, "not counts");
    }

    @Test
    void testHashCodeConsistency() {
        SzCrossSourceMatchCounts counts1 = createTestCrossSourceMatchCounts();
        SzCrossSourceMatchCounts counts2 = createTestCrossSourceMatchCounts();

        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    @Test
    void testToString() {
        SzCrossSourceMatchCounts counts = createTestCrossSourceMatchCounts();
        String result = counts.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("VENDORS"));
    }

    @Test
    void testToStringWithNullValues() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts();

        assertDoesNotThrow(() -> counts.toString());
    }

    @Test
    void testSerializable() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts();
        assertTrue(counts instanceof java.io.Serializable);
    }

    private SzMatchCounts createTestMatchCounts(String matchKey, String principle) {
        SzMatchCounts counts = new SzMatchCounts(matchKey, principle);
        counts.setEntityCount(100L);
        counts.setRecordCount(200L);
        return counts;
    }

    private SzCrossSourceMatchCounts createTestCrossSourceMatchCounts() {
        SzCrossSourceMatchCounts counts = new SzCrossSourceMatchCounts("CUSTOMERS", "VENDORS");
        counts.addCounts(createTestMatchCounts("NAME+DOB", "MFF"));
        return counts;
    }
}
