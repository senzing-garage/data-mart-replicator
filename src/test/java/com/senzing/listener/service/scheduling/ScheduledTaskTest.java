package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.locking.ResourceKey;

import org.checkerframework.checker.units.qual.s;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AbstractSchedulingService.ScheduledTask}.
 * Tests inner class functionality including toString, collapseWith, and follow-up expiration.
 */
class ScheduledTaskTest {

    @Test
    void testScheduledTaskToString() {
        Task task = new Task("TEST_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        String str = scheduledTask.toString();

        assertNotNull(str);
        assertTrue(str.contains("TEST_ACTION") || str.contains("task"),
            "toString should contain action or task info");
    }

    @Test
    void testCollapseWith() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("key", "value");

        Task task1 = new Task("ACTION", params, new TreeSet<>(), null, true);
        Task task2 = new Task("ACTION", new TreeMap<>(params), new TreeSet<>(), null, true);

        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task1);

        int initialMultiplicity = scheduledTask.getMultiplicity();

        // Collapse with identical task
        scheduledTask.collapseWith(task2);

        assertEquals(initialMultiplicity + 1, scheduledTask.getMultiplicity(),
            "Multiplicity should increase after collapse");
    }

    @Test
    void testCollapseWithNonCollapsibleTask() {
        Task collapsible = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        Task nonCollapsible = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, false);

        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(collapsible);

        // collapseWith throws UnsupportedOperationException for non-collapsible
        assertThrows(UnsupportedOperationException.class,
            () -> scheduledTask.collapseWith(nonCollapsible),
            "Should throw when collapsing with non-collapsible task");
    }

    @Test
    void testCollapseWithDifferentSignature() {
        SortedMap<String, Object> params1 = new TreeMap<>();
        params1.put("key", "value1");
        SortedMap<String, Object> params2 = new TreeMap<>();
        params2.put("key", "value2");

        Task task1 = new Task("ACTION", params1, new TreeSet<>(), null, true);
        Task task2 = new Task("ACTION", params2, new TreeSet<>(), null, true);

        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task1);

        // Different signatures throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class,
            () -> scheduledTask.collapseWith(task2),
            "Should throw when tasks have different signatures");
    }

    @Test
    void testSetFollowUpExpiration() throws Exception {
        Task task = new Task("FOLLOWUP_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        long expiration = System.currentTimeMillis() + 5000;

        // call setFollowUpExpiration
        scheduledTask.setFollowUpExpiration(expiration);

        // Verify it was set (check via isFollowUpExpired)
        boolean expired = scheduledTask.isFollowUpExpired();
        assertFalse(expired, "Should not be expired immediately after setting future expiration");
    }

    @Test
    void testIsFollowUpExpiredWhenNotExpired() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Set expiration in the future
        scheduledTask.setFollowUpExpiration(System.currentTimeMillis() + 10000);

        // Check if expired
        boolean expired = scheduledTask.isFollowUpExpired();
        assertFalse(expired, "Should not be expired when expiration is in future");
    }

    @Test
    void testIsFollowUpExpiredWhenExpired() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Set expiration in the past
        scheduledTask.setFollowUpExpiration(System.currentTimeMillis() - 1000);

        // Check if expired
        boolean expired = scheduledTask.isFollowUpExpired();
        assertTrue(expired, "Should be expired when expiration is in past");
    }

    @Test
    void testIsFollowUpExpiredWhenNotSet() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Don't set expiration
        boolean expired = scheduledTask.isFollowUpExpired();
        assertFalse(expired, "Should not be expired when expiration not set");
    }

    @Test
    void testGetOriginalBackingTaskId() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        long originalTaskId = task.getTaskId();

        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Use reflection to get original task ID
        long retrievedId = scheduledTask.getOriginalBackingTaskId();
        
        assertEquals(originalTaskId, retrievedId, "Original task ID should match");
    }

    @Test
    void testScheduledTaskGetters() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("key", "value");
        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "123"));

        // Task without TaskGroup (follow-up task)
        Task task = new Task("GET_ACTION", params, resources, null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Test all getters
        assertEquals("GET_ACTION", scheduledTask.getAction());
        assertNotNull(scheduledTask.getParameters());
        assertNotNull(scheduledTask.getResourceKeys());
        assertNotNull(scheduledTask.getSignature());
        assertTrue(scheduledTask.isAllowingCollapse());
        assertTrue(scheduledTask.isFollowUp()); // No TaskGroup, so it's a follow-up
        // getFollowUpId() returns the ID (may be null for newly created ScheduledTask)
    }

    @Test
    void testScheduledTaskStateTransitions() {
        Task task = new Task("STATE_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        // Test state transitions
        scheduledTask.beginHandling();
        scheduledTask.succeeded();

        assertTrue(scheduledTask.isSuccessful());
    }

    @Test
    void testScheduledTaskFailure() {
        Task task = new Task("FAIL_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        AbstractSchedulingService.ScheduledTask scheduledTask =
            new AbstractSchedulingService.ScheduledTask(task);

        scheduledTask.beginHandling();
        scheduledTask.failed(new Exception("Test failure"));

        assertFalse(scheduledTask.isSuccessful());
    }
}
