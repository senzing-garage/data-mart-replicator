package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzEntitySizeBreakdownTest {

    @Test
    void testDefaultConstructor() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        assertNotNull(breakdown.getEntitySizeCounts());
        assertTrue(breakdown.getEntitySizeCounts().isEmpty());
    }

    @Test
    void testAddEntitySizeCount() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(100L);

        breakdown.addEntitySizeCount(count);

        List<SzEntitySizeCount> counts = breakdown.getEntitySizeCounts();
        assertEquals(1, counts.size());
        assertEquals(5, counts.get(0).getEntitySize());
        assertEquals(100L, counts.get(0).getEntityCount());
    }

    @Test
    void testAddEntitySizeCountWithNull() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        breakdown.addEntitySizeCount(null);

        assertTrue(breakdown.getEntitySizeCounts().isEmpty());
    }

    @Test
    void testAddEntitySizeCountWithZeroEntityCount() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(0L);

        breakdown.addEntitySizeCount(count);

        assertTrue(breakdown.getEntitySizeCounts().isEmpty());
    }

    @Test
    void testAddEntitySizeCountRemovesExistingWithZeroCount() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);
        breakdown.addEntitySizeCount(count1);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(0L);
        breakdown.addEntitySizeCount(count2);

        assertTrue(breakdown.getEntitySizeCounts().isEmpty());
    }

    @Test
    void testSetEntitySizeCounts() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(10);
        count2.setEntityCount(50L);

        breakdown.setEntitySizeCounts(Arrays.asList(count1, count2));

        List<SzEntitySizeCount> counts = breakdown.getEntitySizeCounts();
        assertEquals(2, counts.size());
    }

    @Test
    void testSetEntitySizeCountsWithNull() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(100L);
        breakdown.addEntitySizeCount(count);

        breakdown.setEntitySizeCounts(null);

        assertTrue(breakdown.getEntitySizeCounts().isEmpty());
    }

    @Test
    void testGetEntitySizeCountsReturnsSortedDescending() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();

        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);

        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(10);
        count2.setEntityCount(50L);

        SzEntitySizeCount count3 = new SzEntitySizeCount();
        count3.setEntitySize(1);
        count3.setEntityCount(200L);

        breakdown.setEntitySizeCounts(Arrays.asList(count1, count2, count3));

        List<SzEntitySizeCount> counts = breakdown.getEntitySizeCounts();
        assertEquals(10, counts.get(0).getEntitySize());
        assertEquals(5, counts.get(1).getEntitySize());
        assertEquals(1, counts.get(2).getEntitySize());
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
        assertEquals(breakdown, breakdown);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntitySizeBreakdown breakdown1 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);
        breakdown1.addEntitySizeCount(count1);

        SzEntitySizeBreakdown breakdown2 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(100L);
        breakdown2.addEntitySizeCount(count2);

        assertEquals(breakdown1, breakdown2);
        assertEquals(breakdown2, breakdown1);
    }

    @Test
    void testEqualsWithDifferentCounts() {
        SzEntitySizeBreakdown breakdown1 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);
        breakdown1.addEntitySizeCount(count1);

        SzEntitySizeBreakdown breakdown2 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(10);
        count2.setEntityCount(100L);
        breakdown2.addEntitySizeCount(count2);

        assertNotEquals(breakdown1, breakdown2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
        assertNotEquals(null, breakdown);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
        assertNotEquals(breakdown, "not a breakdown");
    }

    @Test
    void testHashCodeConsistency() {
        SzEntitySizeBreakdown breakdown1 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count1 = new SzEntitySizeCount();
        count1.setEntitySize(5);
        count1.setEntityCount(100L);
        breakdown1.addEntitySizeCount(count1);

        SzEntitySizeBreakdown breakdown2 = new SzEntitySizeBreakdown();
        SzEntitySizeCount count2 = new SzEntitySizeCount();
        count2.setEntitySize(5);
        count2.setEntityCount(100L);
        breakdown2.addEntitySizeCount(count2);

        assertEquals(breakdown1.hashCode(), breakdown2.hashCode());
    }

    @Test
    void testToString() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
        SzEntitySizeCount count = new SzEntitySizeCount();
        count.setEntitySize(5);
        count.setEntityCount(100L);
        breakdown.addEntitySizeCount(count);

        String result = breakdown.toString();

        assertNotNull(result);
    }

    @Test
    void testSerializable() {
        SzEntitySizeBreakdown breakdown = new SzEntitySizeBreakdown();
        assertTrue(breakdown instanceof java.io.Serializable);
    }
}
