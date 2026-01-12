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
}
