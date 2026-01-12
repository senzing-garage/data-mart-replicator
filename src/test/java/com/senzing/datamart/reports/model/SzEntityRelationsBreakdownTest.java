package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SzEntityRelationsBreakdownTest {

    @Test
    void testDefaultConstructor() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        assertNotNull(breakdown.getEntityRelationsCounts());
        assertTrue(breakdown.getEntityRelationsCounts().isEmpty());
    }

    @Test
    void testAddEntityRelationsCount() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(100L);

        breakdown.addEntityRelationsCount(count);

        List<SzEntityRelationsCount> counts = breakdown.getEntityRelationsCounts();
        assertEquals(1, counts.size());
        assertEquals(5, counts.get(0).getRelationsCount());
        assertEquals(100L, counts.get(0).getEntityCount());
    }

    @Test
    void testAddEntityRelationsCountWithNull() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        breakdown.addEntityRelationsCount(null);

        assertTrue(breakdown.getEntityRelationsCounts().isEmpty());
    }

    @Test
    void testAddEntityRelationsCountWithZeroEntityCount() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(0L);

        breakdown.addEntityRelationsCount(count);

        assertTrue(breakdown.getEntityRelationsCounts().isEmpty());
    }

    @Test
    void testAddEntityRelationsCountRemovesExistingWithZeroCount() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);
        breakdown.addEntityRelationsCount(count1);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(0L);
        breakdown.addEntityRelationsCount(count2);

        assertTrue(breakdown.getEntityRelationsCounts().isEmpty());
    }

    @Test
    void testSetEntityRelationsCounts() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(10);
        count2.setEntityCount(50L);

        breakdown.setEntityRelationsCounts(Arrays.asList(count1, count2));

        List<SzEntityRelationsCount> counts = breakdown.getEntityRelationsCounts();
        assertEquals(2, counts.size());
    }

    @Test
    void testSetEntityRelationsCountsWithNull() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(100L);
        breakdown.addEntityRelationsCount(count);

        breakdown.setEntityRelationsCounts(null);

        assertTrue(breakdown.getEntityRelationsCounts().isEmpty());
    }

    @Test
    void testGetEntityRelationsCountsReturnsSortedDescending() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();

        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);

        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(10);
        count2.setEntityCount(50L);

        SzEntityRelationsCount count3 = new SzEntityRelationsCount();
        count3.setRelationsCount(1);
        count3.setEntityCount(200L);

        breakdown.setEntityRelationsCounts(Arrays.asList(count1, count2, count3));

        List<SzEntityRelationsCount> counts = breakdown.getEntityRelationsCounts();
        assertEquals(10, counts.get(0).getRelationsCount());
        assertEquals(5, counts.get(1).getRelationsCount());
        assertEquals(1, counts.get(2).getRelationsCount());
    }

    @Test
    void testEqualsWithSameReference() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
        assertEquals(breakdown, breakdown);
    }

    @Test
    void testEqualsWithEqualObjects() {
        SzEntityRelationsBreakdown breakdown1 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);
        breakdown1.addEntityRelationsCount(count1);

        SzEntityRelationsBreakdown breakdown2 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(100L);
        breakdown2.addEntityRelationsCount(count2);

        assertEquals(breakdown1, breakdown2);
        assertEquals(breakdown2, breakdown1);
    }

    @Test
    void testEqualsWithDifferentCounts() {
        SzEntityRelationsBreakdown breakdown1 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);
        breakdown1.addEntityRelationsCount(count1);

        SzEntityRelationsBreakdown breakdown2 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(10);
        count2.setEntityCount(100L);
        breakdown2.addEntityRelationsCount(count2);

        assertNotEquals(breakdown1, breakdown2);
    }

    @Test
    void testEqualsWithNull() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
        assertNotEquals(null, breakdown);
    }

    @Test
    void testEqualsWithDifferentClass() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
        assertNotEquals(breakdown, "not a breakdown");
    }

    @Test
    void testHashCodeConsistency() {
        SzEntityRelationsBreakdown breakdown1 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count1 = new SzEntityRelationsCount();
        count1.setRelationsCount(5);
        count1.setEntityCount(100L);
        breakdown1.addEntityRelationsCount(count1);

        SzEntityRelationsBreakdown breakdown2 = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count2 = new SzEntityRelationsCount();
        count2.setRelationsCount(5);
        count2.setEntityCount(100L);
        breakdown2.addEntityRelationsCount(count2);

        assertEquals(breakdown1.hashCode(), breakdown2.hashCode());
    }

    @Test
    void testToString() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
        SzEntityRelationsCount count = new SzEntityRelationsCount();
        count.setRelationsCount(5);
        count.setEntityCount(100L);
        breakdown.addEntityRelationsCount(count);

        String result = breakdown.toString();

        assertNotNull(result);
    }

    @Test
    void testSerializable() {
        SzEntityRelationsBreakdown breakdown = new SzEntityRelationsBreakdown();
        assertTrue(breakdown instanceof java.io.Serializable);
    }
}
