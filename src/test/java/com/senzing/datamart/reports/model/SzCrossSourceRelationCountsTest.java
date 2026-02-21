package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzCrossSourceRelationCountsTest {

    @Test
    void testDefaultConstructor() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();

        assertNull(counts.getDataSource());
        assertNull(counts.getVersusDataSource());
        assertNull(counts.getRelationType());
        assertNotNull(counts.getCounts());
        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testConstructorWithParameters() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);

        assertEquals("CUSTOMERS", counts.getDataSource());
        assertEquals("VENDORS", counts.getVersusDataSource());
        assertEquals(SzRelationType.POSSIBLE_MATCH, counts.getRelationType());
        assertNotNull(counts.getCounts());
        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testSetAndGetDataSource() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();
        counts.setDataSource("CUSTOMERS");

        assertEquals("CUSTOMERS", counts.getDataSource());
    }

    @Test
    void testSetAndGetVersusDataSource() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();
        counts.setVersusDataSource("VENDORS");

        assertEquals("VENDORS", counts.getVersusDataSource());
    }

    @Test
    void testSetAndGetRelationType() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();
        counts.setRelationType(SzRelationType.AMBIGUOUS_MATCH);

        assertEquals(SzRelationType.AMBIGUOUS_MATCH, counts.getRelationType());
    }

    @Test
    void testAddCounts() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);
        SzRelationCounts relationCounts = createTestRelationCounts("NAME+DOB", "MFF");

        counts.addCounts(relationCounts);

        List<SzRelationCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals("NAME+DOB", result.get(0).getMatchKey());
        assertEquals("MFF", result.get(0).getPrinciple());
    }

    @Test
    void testAddCountsWithNull() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);

        counts.addCounts(null);

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testAddCountsReplacesExisting() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);

        SzRelationCounts relationCounts1 = createTestRelationCounts("NAME+DOB", "MFF");
        relationCounts1.setEntityCount(100L);
        counts.addCounts(relationCounts1);

        SzRelationCounts relationCounts2 = createTestRelationCounts("NAME+DOB", "MFF");
        relationCounts2.setEntityCount(200L);
        counts.addCounts(relationCounts2);

        List<SzRelationCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals(200L, result.get(0).getEntityCount());
    }

    @Test
    void testSetCounts() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);

        SzRelationCounts relationCounts1 = createTestRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts relationCounts2 = createTestRelationCounts("ADDRESS", "MFS");

        counts.setCounts(Arrays.asList(relationCounts1, relationCounts2));

        assertEquals(2, counts.getCounts().size());
    }

    @Test
    void testSetCountsWithNull() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);
        counts.addCounts(createTestRelationCounts("NAME+DOB", "MFF"));

        counts.setCounts(null);

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testRemoveCounts() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);
        counts.addCounts(createTestRelationCounts("NAME+DOB", "MFF"));
        counts.addCounts(createTestRelationCounts("ADDRESS", "MFS"));

        counts.removeCounts("NAME+DOB", "MFF");

        List<SzRelationCounts> result = counts.getCounts();
        assertEquals(1, result.size());
        assertEquals("ADDRESS", result.get(0).getMatchKey());
    }

    @Test
    void testRemoveAllCounts() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);
        counts.addCounts(createTestRelationCounts("NAME+DOB", "MFF"));
        counts.addCounts(createTestRelationCounts("ADDRESS", "MFS"));

        counts.removeAllCounts();

        assertTrue(counts.getCounts().isEmpty());
    }

    @Test
    void testEqualsWithSameReference() {
        SzCrossSourceRelationCounts counts = createTestCrossSourceRelationCounts();
        assertEquals(counts, counts);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();

        assertEquals(counts1, counts2);
        assertEquals(counts2, counts1);
    }

    @Test
    void testEqualsWithDifferentDataSource() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();
        counts2.setDataSource("DIFFERENT");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentVersusDataSource() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();
        counts2.setVersusDataSource("DIFFERENT");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentRelationType() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();
        counts2.setRelationType(SzRelationType.DISCLOSED_RELATION);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentCounts() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();
        counts2.addCounts(createTestRelationCounts("EXTRA", "RULE"));

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithNull() {
        SzCrossSourceRelationCounts counts = createTestCrossSourceRelationCounts();
        assertNotEquals(null, counts);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzCrossSourceRelationCounts counts = createTestCrossSourceRelationCounts();
        assertNotEquals(counts, "not counts");
    }

    @Test
    void testHashCodeConsistency() {
        SzCrossSourceRelationCounts counts1 = createTestCrossSourceRelationCounts();
        SzCrossSourceRelationCounts counts2 = createTestCrossSourceRelationCounts();

        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    @Test
    void testToString() {
        SzCrossSourceRelationCounts counts = createTestCrossSourceRelationCounts();
        String result = counts.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("VENDORS"));
        assertTrue(result.contains("POSSIBLE_MATCH"));
    }

    @Test
    void testToStringWithNullValues() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();

        assertDoesNotThrow(() -> counts.toString());
    }

    @Test
    void testSerializable() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts();
        assertTrue(counts instanceof java.io.Serializable);
    }

    private SzRelationCounts createTestRelationCounts(String matchKey, String principle) {
        SzRelationCounts counts = new SzRelationCounts(matchKey, principle);
        counts.setEntityCount(100L);
        counts.setRecordCount(200L);
        counts.setRelationCount(50L);
        return counts;
    }

    private SzCrossSourceRelationCounts createTestCrossSourceRelationCounts() {
        SzCrossSourceRelationCounts counts = new SzCrossSourceRelationCounts(
                "CUSTOMERS", "VENDORS", SzRelationType.POSSIBLE_MATCH);
        counts.addCounts(createTestRelationCounts("NAME+DOB", "MFF"));
        return counts;
    }
}
