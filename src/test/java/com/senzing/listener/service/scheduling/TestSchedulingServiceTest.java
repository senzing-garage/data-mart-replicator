package com.senzing.listener.service.scheduling;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link TestSchedulingService} test utility class.
 */
class TestSchedulingServiceTest {

    @Test
    void testGetPendingTasksCountReturnsSetValue() {
        TestSchedulingService service = new TestSchedulingService();

        // Initially null
        assertNull(service.getRemainingTasksCount());

        // Set a value
        service.setPendingTaskCount(42L);
        assertEquals(42L, service.getRemainingTasksCount().longValue());

        // Set to null
        service.setPendingTaskCount(null);
        assertNull(service.getRemainingTasksCount());
    }

    @Test
    void testGetPendingFollowUpTasksCountReturnsSetValue() {
        TestSchedulingService service = new TestSchedulingService();

        // Initially null
        assertNull(service.getRemainingFollowUpTasksCount());

        // Set a value
        service.setPendingFollowUpCount(100L);
        assertEquals(100L, service.getRemainingFollowUpTasksCount().longValue());

        // Set to null
        service.setPendingFollowUpCount(null);
        assertNull(service.getRemainingFollowUpTasksCount());
    }

    @Test
    void testGetLastTaskScheduledNanoTimeInitiallyNegativeOne() {
        TestSchedulingService service = new TestSchedulingService();

        assertEquals(-1L, service.getLastTaskActivityNanoTime());
    }

    @Test
    void testUpdateLastTaskScheduledNanoTime() {
        TestSchedulingService service = new TestSchedulingService();

        assertEquals(-1L, service.getLastTaskActivityNanoTime());

        long before = System.nanoTime();
        service.updateLastTaskScheduledNanoTime();
        long after = System.nanoTime();

        long nanoTime = service.getLastTaskActivityNanoTime();
        assertTrue(nanoTime >= before);
        assertTrue(nanoTime <= after);
    }

    @Test
    void testGetAllPendingTasksCountWithBothNull() {
        TestSchedulingService service = new TestSchedulingService();

        // Both null should return null
        assertNull(service.getAllRemainingTasksCount());
    }

    @Test
    void testGetAllPendingTasksCountWithOnlyTasksSet() {
        TestSchedulingService service = new TestSchedulingService();

        service.setPendingTaskCount(50L);
        // follow-up is still null

        Long count = service.getAllRemainingTasksCount();
        assertEquals(50L, count.longValue());
    }

    @Test
    void testGetAllPendingTasksCountWithOnlyFollowUpSet() {
        TestSchedulingService service = new TestSchedulingService();

        service.setPendingFollowUpCount(30L);
        // tasks is still null

        Long count = service.getAllRemainingTasksCount();
        assertEquals(30L, count.longValue());
    }

    @Test
    void testGetAllPendingTasksCountWithBothSet() {
        TestSchedulingService service = new TestSchedulingService();

        service.setPendingTaskCount(20L);
        service.setPendingFollowUpCount(15L);

        Long count = service.getAllRemainingTasksCount();
        assertEquals(35L, count.longValue());
    }
}
