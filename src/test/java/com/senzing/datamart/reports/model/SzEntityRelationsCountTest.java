package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SzEntityRelationsCountTest {

    @Test
    void testDefaultConstructor() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();

        assertEquals(0, count.getRelationsCount());
        assertEquals(0L, count.getEntityCount());
    }

    @Test
    void testSetAndGetRelationsCount() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);

        assertEquals(5, count.getRelationsCount());
    }

    @Test
    void testSetAndGetEntityCount() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setEntityCount(100L);

        assertEquals(100L, count.getEntityCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(100L);

        assertEquals(count, count);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(100L);

        assertEquals(count1, count2);
        assertEquals(count2, count1);
    }

    @Test
    void testEqualsWithDifferentRelationsCount() {
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(10);

        assertNotEquals(count1, count2);
    }

    @Test
    void testEqualsWithDifferentEntityCount() {
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(200L);

        assertNotEquals(count1, count2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertNotEquals(null, count);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertNotEquals(count, "not a count");
    }

    @Test
    void testHashCodeConsistency() {
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(100L);

        assertEquals(count1.hashCode(), count2.hashCode());
    }

    @Test
    void testToString() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(100L);

        String result = count.toString();

        assertNotNull(result);
        assertTrue(result.contains("5"));
        assertTrue(result.contains("100"));
    }

    @Test
    void testSerializable() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertTrue(count instanceof java.io.Serializable);
    }

    @Test
    void testSetRelationsCountWithZeroIsValid() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        // Zero relations count should be valid (entities with no relations)
        assertDoesNotThrow(() -> {
            count.setRelationsCount(0);
        });
        assertEquals(0, count.getRelationsCount());
    }

    @Test
    void testSetRelationsCountWithNegative() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setRelationsCount(-1);
        });
    }

    @Test
    void testSetRelationsCountWithLargeNegative() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setRelationsCount(-100);
        });
    }

    @Test
    void testSetEntityCountWithZeroIsValid() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        // Zero entity count should be valid (no entities with this relation count)
        assertDoesNotThrow(() -> {
            count.setEntityCount(0L);
        });
        assertEquals(0L, count.getEntityCount());
    }

    @Test
    void testSetEntityCountWithNegative() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntityCount(-1L);
        });
    }

    @Test
    void testSetEntityCountWithLargeNegative() {
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        assertThrows(IllegalArgumentException.class, () -> {
            count.setEntityCount(-1000L);
        });
    }
}
