package com.senzing.datamart.handlers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CrossSourceKeyTest {

    @Test
    void testConstructorWithSingleSource() {
        String source = "CUSTOMERS";
        CrossSourceKey key = new CrossSourceKey(source);

        assertEquals(source, key.getSource1());
        assertEquals(source, key.getSource2());
    }

    @Test
    void testConstructorWithTwoSources() {
        String source1 = "CUSTOMERS";
        String source2 = "WATCHLIST";
        CrossSourceKey key = new CrossSourceKey(source1, source2);

        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
    }

    @Test
    void testConstructorWithNullSingleSource() {
        assertThrows(NullPointerException.class, () -> {
            new CrossSourceKey(null);
        });
    }

    @Test
    void testConstructorWithNullSource1() {
        assertThrows(NullPointerException.class, () -> {
            new CrossSourceKey(null, "WATCHLIST");
        });
    }

    @Test
    void testConstructorWithNullSource2() {
        assertThrows(NullPointerException.class, () -> {
            new CrossSourceKey("CUSTOMERS", null);
        });
    }

    @Test
    void testConstructorWithBothSourcesNull() {
        assertThrows(NullPointerException.class, () -> {
            new CrossSourceKey(null, null);
        });
    }

    @Test
    void testEqualsWithSameReference() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        assertEquals(key, key);
    }

    @Test
    void testEqualsWithEqualObjects() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    @Test
    void testEqualsWithEqualObjectsSingleSource() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "CUSTOMERS");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    @Test
    void testEqualsWithDifferentSource1() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("REFERENCE", "WATCHLIST");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithDifferentSource2() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "REFERENCE");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithBothSourcesDifferent() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("REFERENCE", "VENDORS");

        assertNotEquals(key1, key2);
    }

    @Test
    void testEqualsWithNull() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        assertNotEquals(null, key);
    }

    @Test
    void testEqualsWithDifferentClass() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        String other = "CUSTOMERS:WATCHLIST";
        assertNotEquals(key, other);
    }

    @Test
    void testHashCodeConsistency() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testHashCodeDifferentForDifferentObjects() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("REFERENCE", "VENDORS");

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void testCompareToWithNull() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        assertTrue(key.compareTo(null) > 0);
    }

    @Test
    void testCompareToWithSameReference() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        assertEquals(0, key.compareTo(key));
    }

    @Test
    void testCompareToEqual() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "WATCHLIST");

        assertEquals(0, key1.compareTo(key2));
        assertEquals(0, key2.compareTo(key1));
    }

    @Test
    void testCompareToDifferentSource1() {
        CrossSourceKey key1 = new CrossSourceKey("AAA", "WATCHLIST");
        CrossSourceKey key2 = new CrossSourceKey("ZZZ", "WATCHLIST");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    void testCompareToDifferentSource2() {
        CrossSourceKey key1 = new CrossSourceKey("CUSTOMERS", "AAA");
        CrossSourceKey key2 = new CrossSourceKey("CUSTOMERS", "ZZZ");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    void testCompareToSource1TakesPrecedence() {
        CrossSourceKey key1 = new CrossSourceKey("AAA", "ZZZ");
        CrossSourceKey key2 = new CrossSourceKey("BBB", "AAA");

        // Even though key1's source2 is greater, source1 comparison takes precedence
        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    void testToString() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS", "WATCHLIST");
        String result = key.toString();

        assertNotNull(result);
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("WATCHLIST"));
        assertEquals("CUSTOMERS:WATCHLIST", result);
    }

    @Test
    void testToStringWithSameSource() {
        CrossSourceKey key = new CrossSourceKey("CUSTOMERS");
        String result = key.toString();

        assertNotNull(result);
        assertEquals("CUSTOMERS:CUSTOMERS", result);
    }
}
