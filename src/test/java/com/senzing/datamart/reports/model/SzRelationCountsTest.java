package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzRelationCountsTest {

    @Test
    void testDefaultConstructor() {
        SzRelationCounts counts = new SzRelationCounts();

        assertNull(counts.getMatchKey());
        assertNull(counts.getPrinciple());
        assertEquals(0L, counts.getEntityCount());
        assertEquals(0L, counts.getRecordCount());
        assertEquals(0L, counts.getRelationCount());
    }

    @Test
    void testConstructorWithMatchKeyAndPrinciple() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");

        assertEquals("NAME+DOB", counts.getMatchKey());
        assertEquals("MFF", counts.getPrinciple());
        assertEquals(0L, counts.getEntityCount());
        assertEquals(0L, counts.getRecordCount());
        assertEquals(0L, counts.getRelationCount());
    }

    @Test
    void testConstructorWithNullMatchKey() {
        SzRelationCounts counts = new SzRelationCounts(null, "MFF");

        assertNull(counts.getMatchKey());
        assertEquals("MFF", counts.getPrinciple());
    }

    @Test
    void testConstructorWithNullPrinciple() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", null);

        assertEquals("NAME+DOB", counts.getMatchKey());
        assertNull(counts.getPrinciple());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzRelationCounts counts = new SzRelationCounts();
        counts.setEntityCount(100L);

        assertEquals(100L, counts.getEntityCount());
    }

    @Test
    void testSetAndGetRecordCount() {
        SzRelationCounts counts = new SzRelationCounts();
        counts.setRecordCount(500L);

        assertEquals(500L, counts.getRecordCount());
    }

    @Test
    void testSetAndGetRelationCount() {
        SzRelationCounts counts = new SzRelationCounts();
        counts.setRelationCount(250L);

        assertEquals(250L, counts.getRelationCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");
        assertEquals(counts, counts);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);
        counts1.setRecordCount(500L);
        counts1.setRelationCount(250L);

        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(100L);
        counts2.setRecordCount(500L);
        counts2.setRelationCount(250L);

        assertEquals(counts1, counts2);
        assertEquals(counts2, counts1);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = new SzRelationCounts("ADDRESS", "MFF");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFS");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentEntityCount() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);

        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(200L);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentRecordCount() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        counts1.setRecordCount(100L);

        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFF");
        counts2.setRecordCount(200L);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentRelationCount() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        counts1.setRelationCount(100L);

        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFF");
        counts2.setRelationCount(200L);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithNull() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");
        assertNotEquals(null, counts);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");
        assertNotEquals(counts, "not counts");
    }

    @Test
    void testHashCodeConsistency() {
        SzRelationCounts counts1 = new SzRelationCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);
        counts1.setRecordCount(500L);
        counts1.setRelationCount(250L);

        SzRelationCounts counts2 = new SzRelationCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(100L);
        counts2.setRecordCount(500L);
        counts2.setRelationCount(250L);

        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    @Test
    void testToString() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");
        counts.setEntityCount(100L);
        counts.setRecordCount(500L);
        counts.setRelationCount(250L);

        String result = counts.toString();

        assertNotNull(result);
        assertTrue(result.contains("NAME+DOB"));
        assertTrue(result.contains("MFF"));
        assertTrue(result.contains("100"));
        assertTrue(result.contains("500"));
        assertTrue(result.contains("250"));
    }

    @Test
    void testToStringWithNullValues() {
        SzRelationCounts counts = new SzRelationCounts(null, null);

        assertDoesNotThrow(() -> counts.toString());
    }

    @Test
    void testSerializable() {
        SzRelationCounts counts = new SzRelationCounts("NAME+DOB", "MFF");
        assertTrue(counts instanceof java.io.Serializable);
    }
}
