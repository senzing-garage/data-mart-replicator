package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzMatchCountsTest {

    @Test
    void testDefaultConstructor() {
        SzMatchCounts counts = new SzMatchCounts();

        assertNull(counts.getMatchKey());
        assertNull(counts.getPrinciple());
        assertEquals(0L, counts.getEntityCount());
        assertEquals(0L, counts.getRecordCount());
    }

    @Test
    void testConstructorWithMatchKeyAndPrinciple() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");

        assertEquals("NAME+DOB", counts.getMatchKey());
        assertEquals("MFF", counts.getPrinciple());
        assertEquals(0L, counts.getEntityCount());
        assertEquals(0L, counts.getRecordCount());
    }

    @Test
    void testConstructorWithNullMatchKey() {
        SzMatchCounts counts = new SzMatchCounts(null, "MFF");

        assertNull(counts.getMatchKey());
        assertEquals("MFF", counts.getPrinciple());
    }

    @Test
    void testConstructorWithNullPrinciple() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", null);

        assertEquals("NAME+DOB", counts.getMatchKey());
        assertNull(counts.getPrinciple());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzMatchCounts counts = new SzMatchCounts();
        counts.setEntityCount(100L);

        assertEquals(100L, counts.getEntityCount());
    }

    @Test
    void testSetAndGetRecordCount() {
        SzMatchCounts counts = new SzMatchCounts();
        counts.setRecordCount(500L);

        assertEquals(500L, counts.getRecordCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");
        assertEquals(counts, counts);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);
        counts1.setRecordCount(500L);

        SzMatchCounts counts2 = new SzMatchCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(100L);
        counts2.setRecordCount(500L);

        assertEquals(counts1, counts2);
        assertEquals(counts2, counts1);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        SzMatchCounts counts2 = new SzMatchCounts("ADDRESS", "MFF");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        SzMatchCounts counts2 = new SzMatchCounts("NAME+DOB", "MFS");

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentEntityCount() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);

        SzMatchCounts counts2 = new SzMatchCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(200L);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithDifferentRecordCount() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        counts1.setRecordCount(100L);

        SzMatchCounts counts2 = new SzMatchCounts("NAME+DOB", "MFF");
        counts2.setRecordCount(200L);

        assertNotEquals(counts1, counts2);
    }

    @Test
    void testEqualsWithNull() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");
        assertNotEquals(null, counts);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");
        assertNotEquals(counts, "not counts");
    }

    @Test
    void testHashCodeConsistency() {
        SzMatchCounts counts1 = new SzMatchCounts("NAME+DOB", "MFF");
        counts1.setEntityCount(100L);
        counts1.setRecordCount(500L);

        SzMatchCounts counts2 = new SzMatchCounts("NAME+DOB", "MFF");
        counts2.setEntityCount(100L);
        counts2.setRecordCount(500L);

        assertEquals(counts1.hashCode(), counts2.hashCode());
    }

    @Test
    void testToString() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");
        counts.setEntityCount(100L);
        counts.setRecordCount(500L);

        String result = counts.toString();

        assertNotNull(result);
        assertTrue(result.contains("NAME+DOB"));
        assertTrue(result.contains("MFF"));
        assertTrue(result.contains("100"));
        assertTrue(result.contains("500"));
    }

    @Test
    void testToStringWithNullValues() {
        SzMatchCounts counts = new SzMatchCounts(null, null);

        assertDoesNotThrow(() -> counts.toString());
    }

    @Test
    void testSerializable() {
        SzMatchCounts counts = new SzMatchCounts("NAME+DOB", "MFF");
        assertTrue(counts instanceof java.io.Serializable);
    }
}
