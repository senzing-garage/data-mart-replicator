package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzEntitySizeCountTest {

    @Test
    void testDefaultConstructor() {
        SzEntitySizeCount count = new SzEntitySizeCount();

        assertEquals(0, count.getEntitySize());
        assertEquals(0L, count.getEntityCount());
    }

    @Test
    void testSetAndGetEntitySize() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);

        assertEquals(5, count.getEntitySize());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntityCount(100L);

        assertEquals(100L, count.getEntityCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(100L);

        assertEquals(count, count);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(100L);

        assertEquals(count1, count2);
        assertEquals(count2, count1);
    }

    @Test
    void testEqualsWithDifferentEntitySize() {
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(10);

        assertNotEquals(count1, count2);
    }

    @Test
    void testEqualsWithDifferentEntityCount() {
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(200L);

        assertNotEquals(count1, count2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertNotEquals(null, count);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertNotEquals(count, "not a count");
    }

    @Test
    void testHashCodeConsistency() {
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(100L);

        assertEquals(count1.hashCode(), count2.hashCode());
    }

    @Test
    void testToString() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(100L);

        String result = count.toString();

        assertNotNull(result);
        assertTrue(result.contains("5"));
        assertTrue(result.contains("100"));
    }

    @Test
    void testSerializable() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertTrue(count instanceof java.io.Serializable);
    }

    @Test
    void testSetEntitySizeWithZero() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntitySize(0);
        });
    }

    @Test
    void testSetEntitySizeWithNegative() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntitySize(-1);
        });
    }

    @Test
    void testSetEntitySizeWithLargeNegative() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntitySize(-100);
        });
    }

    @Test
    void testSetEntityCountWithNegative() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntityCount(-1L);
        });
    }

    @Test
    void testSetEntityCountWithLargeNegative() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntityCount(-1000L);
        });
    }

    @Test
    void testSetEntityCountWithZeroIsValid() {
        SzEntitySizeCount count = new SzEntitySizeCount();
        // Zero entity count should be valid (no entities of this size)
        assertDoesNotThrow(() -> {
            count.setEntityCount(0L);
        });
        assertEquals(0L, count.getEntityCount());
    }
}
