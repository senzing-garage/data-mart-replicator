package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzCountsKeyTest {

    @Test
    void testConstructorWithMatchKeyAndPrinciple() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");

        assertEquals("NAME+DOB", key.getMatchKey());
        assertEquals("MFF", key.getPrinciple());
    }

    @Test
    void testConstructorWithNullMatchKey() {
        SzCountsKey key = new SzCountsKey(null, "MFF");

        assertNull(key.getMatchKey());
        assertEquals("MFF", key.getPrinciple());
    }

    @Test
    void testConstructorWithNullPrinciple() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", null);

        assertEquals("NAME+DOB", key.getMatchKey());
        assertNull(key.getPrinciple());
    }

    @Test
    void testConstructorWithBothNull() {
        SzCountsKey key = new SzCountsKey(null, null);

        assertNull(key.getMatchKey());
        assertNull(key.getPrinciple());
    }

    @Test
    void testEqualsWithSameReference() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        assertEquals(key, key);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    @Test
    void testEqualsWithDifferentMatchKey() {
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("ADDRESS", "MFF");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentPrinciple() {
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFS");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithNullMatchKeys() {
        SzCountsKey key1 = new SzCountsKey(null, "MFF");
        SzCountsKey key2 = new SzCountsKey(null, "MFF");

        assertEquals(key1, key2);
    }

    @Test
    void testEqualsWithNullPrinciples() {
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", null);
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", null);

        assertEquals(key1, key2);
    }

    @Test
    void testEqualsWithNull() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        assertNotEquals(null, key);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        assertNotEquals(key, "not a key");
    }

    @Test
    void testHashCodeConsistency() {
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testHashCodeWithNullValues() {
        SzCountsKey key1 = new SzCountsKey(null, null);
        SzCountsKey key2 = new SzCountsKey(null, null);

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testToString() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        String result = key.toString();

        assertNotNull(result);
        assertTrue(result.contains("NAME+DOB"));
        assertTrue(result.contains("MFF"));
    }

    @Test
    void testToStringWithNullValues() {
        SzCountsKey key = new SzCountsKey(null, null);
        String result = key.toString();

        assertNotNull(result);
        assertDoesNotThrow(() -> key.toString());
    }

    @Test
    void testSerializable() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        assertTrue(key instanceof java.io.Serializable);
    }

    // ========================================================================
    // equals(null) test - direct call to exercise line 67-68
    // ========================================================================

    @Test
    void testEqualsWithNullParameter() {
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        // Direct call to equals(null) to exercise the null check branch
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    // ========================================================================
    // compareTo() tests - exercise all conditional branches
    // ========================================================================

    @Test
    void testCompareToWithNullParameter() {
        // Line 107-108: comparing to null should return positive (this > null)
        SzCountsKey key = new SzCountsKey("NAME+DOB", "MFF");
        assertTrue(key.compareTo(null) > 0, "compareTo(null) should return positive");
    }

    @Test
    void testCompareToWithEqualKeys() {
        // Keys are equal - should return 0
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");
        assertEquals(0, key1.compareTo(key2), "Equal keys should compare to 0");
    }

    @Test
    void testCompareToWithDifferentMatchKeys() {
        // Line 119: Both match keys non-null and different - compare strings
        SzCountsKey key1 = new SzCountsKey("ADDRESS", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");
        assertTrue(key1.compareTo(key2) < 0, "ADDRESS should sort before NAME+DOB");
        assertTrue(key2.compareTo(key1) > 0, "NAME+DOB should sort after ADDRESS");
    }

    @Test
    void testCompareToWithNullMatchKeyFirst() {
        // Line 113-114: this.matchKey is null, other is not - return -1
        SzCountsKey key1 = new SzCountsKey(null, "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");
        assertTrue(key1.compareTo(key2) < 0, "null matchKey should sort before non-null");
    }

    @Test
    void testCompareToWithNullMatchKeySecond() {
        // Line 116-117: other.matchKey is null, this is not - return 1
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey(null, "MFF");
        assertTrue(key1.compareTo(key2) > 0, "non-null matchKey should sort after null");
    }

    @Test
    void testCompareToWithBothNullMatchKeys() {
        // Both match keys null - proceed to compare principles
        SzCountsKey key1 = new SzCountsKey(null, "MFF");
        SzCountsKey key2 = new SzCountsKey(null, "MFS");
        assertTrue(key1.compareTo(key2) < 0, "MFF should sort before MFS");
        assertTrue(key2.compareTo(key1) > 0, "MFS should sort after MFF");
    }

    @Test
    void testCompareToWithSameMatchKeyDifferentPrinciples() {
        // Line 132: Same match key, different principles - compare principle strings
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFS");
        assertTrue(key1.compareTo(key2) < 0, "MFF should sort before MFS");
        assertTrue(key2.compareTo(key1) > 0, "MFS should sort after MFF");
    }

    @Test
    void testCompareToWithNullPrincipleFirst() {
        // Line 126-127: this.principle is null, other is not - return -1
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", null);
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");
        assertTrue(key1.compareTo(key2) < 0, "null principle should sort before non-null");
    }

    @Test
    void testCompareToWithNullPrincipleSecond() {
        // Line 129-130: other.principle is null, this is not - return 1
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", null);
        assertTrue(key1.compareTo(key2) > 0, "non-null principle should sort after null");
    }

    @Test
    void testCompareToWithBothNullPrinciples() {
        // Line 123-124: Both principles null (and equal) - return 0
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", null);
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", null);
        assertEquals(0, key1.compareTo(key2), "Both null principles should compare to 0");
    }

    @Test
    void testCompareToWithAllNullValues() {
        // Both matchKey and principle are null in both keys
        SzCountsKey key1 = new SzCountsKey(null, null);
        SzCountsKey key2 = new SzCountsKey(null, null);
        assertEquals(0, key1.compareTo(key2), "Completely null keys should compare to 0");
    }

    @Test
    void testCompareToConsistentWithEquals() {
        // Verify that compareTo returns 0 when equals returns true
        SzCountsKey key1 = new SzCountsKey("NAME+DOB", "MFF");
        SzCountsKey key2 = new SzCountsKey("NAME+DOB", "MFF");
        assertEquals(key1, key2);
        assertEquals(0, key1.compareTo(key2), "compareTo should return 0 for equal objects");
    }
}
