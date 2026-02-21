package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.locking.ResourceKey;
import org.junit.jupiter.api.*;

import javax.json.JsonObject;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link Task}.
 * Tests are independent and can run in parallel.
 */
class TaskTest {

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    @Test
    void testConstructorWithBasicParameters() {
        String action = "TEST_ACTION";
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("param1", "value1");
        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "123"));

        Task task = new Task(action, params, resources, null, true);

        assertNotNull(task);
        assertEquals(action, task.getAction());
        assertEquals(Task.State.UNSCHEDULED, task.getState());
        assertTrue(task.isAllowingCollapse());
        assertNull(task.getTaskGroup());
        assertFalse(task.isCompleted());
    }

    @Test
    void testConstructorWithTaskGroup() {
        TaskGroup group = new TaskGroup();
        String action = "TEST_ACTION";
        SortedMap<String, Object> params = new TreeMap<>();
        SortedSet<ResourceKey> resources = new TreeSet<>();

        Task task = new Task(action, params, resources, group, false);

        assertNotNull(task);
        assertEquals(group, task.getTaskGroup());
        assertFalse(task.isAllowingCollapse());
    }

    @Test
    void testConstructorGeneratesUniqueTaskIds() {
        SortedMap<String, Object> params = new TreeMap<>();
        SortedSet<ResourceKey> resources = new TreeSet<>();

        Task task1 = new Task("ACTION", params, resources, null, true);
        Task task2 = new Task("ACTION", params, resources, null, true);
        Task task3 = new Task("ACTION", params, resources, null, true);

        long id1 = task1.getTaskId();
        long id2 = task2.getTaskId();
        long id3 = task3.getTaskId();

        assertNotEquals(id1, id2);
        assertNotEquals(id2, id3);
        assertNotEquals(id1, id3);
    }

    @Test
    void testConstructorParametersAreUnmodifiable() {
        String action = "TEST_ACTION";
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("param1", "value1");
        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "123"));

        Task task = new Task(action, params, resources, null, true);

        // Modify original collections should not affect task
        params.put("param2", "value2");
        resources.add(new ResourceKey("RECORD", "456"));

        assertEquals(1, task.getParameters().size());
        assertEquals(1, task.getResourceKeys().size());
    }

    // ========================================================================
    // Getter Tests
    // ========================================================================

    @Test
    void testGetAction() {
        String action = "MY_ACTION";
        Task task = new Task(action, new TreeMap<>(), new TreeSet<>(), null, true);
        assertEquals(action, task.getAction());
    }

    @Test
    void testGetParameters() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("key1", "val1");
        params.put("key2", 123);
        params.put("key3", Arrays.asList("a", "b", "c"));

        Task task = new Task("ACTION", params, new TreeSet<>(), null, true);

        Map<String, Object> result = task.getParameters();
        assertEquals(3, result.size());
        assertEquals("val1", result.get("key1"));
        assertEquals(123, result.get("key2"));
        assertEquals(Arrays.asList("a", "b", "c"), result.get("key3"));
    }

    @Test
    void testGetResourceKeys() {
        SortedSet<ResourceKey> resources = new TreeSet<>();
        ResourceKey key1 = new ResourceKey("ENTITY", "100");
        ResourceKey key2 = new ResourceKey("ENTITY", "200");
        ResourceKey key3 = new ResourceKey("RECORD", "DS1", "R001");
        resources.add(key1);
        resources.add(key2);
        resources.add(key3);

        Task task = new Task("ACTION", new TreeMap<>(), resources, null, true);

        Set<ResourceKey> result = task.getResourceKeys();
        assertEquals(3, result.size());
        assertTrue(result.contains(key1));
        assertTrue(result.contains(key2));
        assertTrue(result.contains(key3));
    }

    @Test
    void testGetTaskId() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        long taskId = task.getTaskId();
        assertTrue(taskId >= 0);
    }

    // ========================================================================
    // State Transition Tests
    // ========================================================================

    @Test
    void testMarkScheduledTransition() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        assertEquals(Task.State.UNSCHEDULED, task.getState());

        task.markScheduled();
        assertEquals(Task.State.SCHEDULED, task.getState());
    }

    @Test
    void testBeginHandlingTransition() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();

        task.beginHandling();
        assertEquals(Task.State.STARTED, task.getState());
    }

    @Test
    void testSucceededTransition() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();

        task.succeeded();
        assertEquals(Task.State.SUCCESSFUL, task.getState());
        assertTrue(task.isCompleted());
    }

    @Test
    void testFailedTransition() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();

        Exception failure = new Exception("Test failure");
        task.failed(failure);
        assertEquals(Task.State.FAILED, task.getState());
        assertTrue(task.isCompleted());
        assertEquals(failure, task.getFailure());
    }

    @Test
    void testAbortedFromUnscheduled() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        task.aborted();
        assertEquals(Task.State.ABORTED, task.getState());
        assertTrue(task.isCompleted());
    }

    @Test
    void testAbortedFromScheduled() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();

        task.aborted();
        assertEquals(Task.State.ABORTED, task.getState());
        assertTrue(task.isCompleted());
    }

    @Test
    void testInvalidStateTransition() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();
        task.succeeded();

        // Cannot transition from SUCCESSFUL to any other state
        assertThrows(IllegalArgumentException.class, () -> task.markScheduled());
        assertThrows(IllegalArgumentException.class, () -> task.beginHandling());
        assertThrows(IllegalArgumentException.class, () -> task.aborted());
    }

    // ========================================================================
    // Statistics Tests
    // ========================================================================

    @Test
    void testStatisticsUnscheduledTime() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        // Wait a bit to accumulate unscheduled time
        Thread.sleep(50);

        Map<Task.Statistic, Number> stats = task.getStatistics();
        assertNotNull(stats.get(Task.Statistic.unscheduledTime));
        long unscheduledTime = stats.get(Task.Statistic.unscheduledTime).longValue();
        assertTrue(unscheduledTime >= 50, "Expected at least 50ms unscheduled time, got " + unscheduledTime);
    }

    @Test
    void testStatisticsPendingTime() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();

        // Wait a bit to accumulate pending time
        Thread.sleep(50);

        Map<Task.Statistic, Number> stats = task.getStatistics();
        assertNotNull(stats.get(Task.Statistic.pendingTime));
        long pendingTime = stats.get(Task.Statistic.pendingTime).longValue();
        assertTrue(pendingTime >= 50, "Expected at least 50ms pending time, got " + pendingTime);
    }

    @Test
    void testStatisticsHandlingTime() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();

        // Wait a bit to accumulate handling time
        Thread.sleep(50);

        task.succeeded();

        Map<Task.Statistic, Number> stats = task.getStatistics();
        assertNotNull(stats.get(Task.Statistic.handlingTime));
        long handlingTime = stats.get(Task.Statistic.handlingTime).longValue();
        assertTrue(handlingTime >= 50, "Expected at least 50ms handling time, got " + handlingTime);
    }

    @Test
    void testStatisticsLifespan() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        Thread.sleep(50);
        task.markScheduled();
        Thread.sleep(50);
        task.beginHandling();
        Thread.sleep(50);
        task.succeeded();

        Map<Task.Statistic, Number> stats = task.getStatistics();
        assertNotNull(stats.get(Task.Statistic.lifespan));
        long lifespan = stats.get(Task.Statistic.lifespan).longValue();
        assertTrue(lifespan >= 150, "Expected at least 150ms lifespan, got " + lifespan);
    }

    // ========================================================================
    // JSON Serialization/Deserialization Tests
    // ========================================================================

    @Test
    void testToJsonBasic() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("key1", "value1");
        params.put("key2", 42);

        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "123"));

        Task task = new Task("MY_ACTION", params, resources, null, true);

        String json = task.toJsonText();
        assertNotNull(json);
        assertTrue(json.contains("MY_ACTION"));
        assertTrue(json.contains("key1"));
        assertTrue(json.contains("value1"));
    }

    @Test
    void testToJsonObject() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("param1", "val1");

        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("RECORD", "DS1", "R001"));

        Task task = new Task("ACTION", params, resources, null, true);

        JsonObject jsonObj = task.toJsonObject();
        assertNotNull(jsonObj);
        assertEquals("ACTION", jsonObj.getString("action"));
        assertTrue(jsonObj.containsKey("params"));
        assertTrue(jsonObj.containsKey("resources"));
    }

    @Test
    void testDeserializeBasic() {
        String json = "{\"action\":\"TEST_ACTION\",\"params\":{\"key1\":\"value1\"},\"resources\":[\"ENTITY:123\"]}";

        Task task = Task.deserialize(json, true, null);

        assertNotNull(task);
        assertEquals("TEST_ACTION", task.getAction());
        assertEquals(1, task.getParameters().size());
        assertEquals("value1", task.getParameters().get("key1"));
        assertEquals(1, task.getResourceKeys().size());
        assertTrue(task.isAllowingCollapse());
        assertNull(task.getTaskGroup());
    }

    @Test
    void testDeserializeWithElapsedTime() {
        String json = "{\"action\":\"ACTION\",\"params\":{},\"resources\":[]}";

        Task task = Task.deserialize(json, false, 1000L);

        assertNotNull(task);
        assertFalse(task.isAllowingCollapse());
    }

    @Test
    void testSerializeDeserializeRoundTrip() {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("str", "test");
        params.put("num", 123);
        params.put("list", Arrays.asList("a", "b", "c"));

        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "100"));
        resources.add(new ResourceKey("ENTITY", "200"));

        Task original = new Task("ACTION", params, resources, null, true);

        String json = original.toJsonText();
        Task deserialized = Task.deserialize(json, true, null);

        assertEquals(original.getAction(), deserialized.getAction());
        // Parameters might differ in type but should have same values
        assertEquals(original.getParameters().get("str"), deserialized.getParameters().get("str"));
        assertEquals(original.getResourceKeys().size(), deserialized.getResourceKeys().size());
    }

    // ========================================================================
    // Message Digest Signature Tests
    // ========================================================================

    @Test
    void testGetSignature() throws Exception {
        SortedMap<String, Object> params = new TreeMap<>();
        params.put("key", "value");

        SortedSet<ResourceKey> resources = new TreeSet<>();
        resources.add(new ResourceKey("ENTITY", "123"));

        Task task = new Task("ACTION", params, resources, null, true);

        String signature = task.getSignature();
        assertNotNull(signature);
        assertFalse(signature.isEmpty());
        // SHA-256 hash should be 64 hex characters
        assertEquals(64, signature.length());
    }

    @Test
    void testStaticGetSignature() throws Exception {
        Task task1 = new Task("TEST_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        Task task2 = new Task("TEST_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        String signature1 = Task.toSignature(task1);
        String signature2 = Task.toSignature(task2);

        assertNotNull(signature1);
        assertNotNull(signature2);
        assertEquals(64, signature1.length());
        assertEquals(signature1, signature2); // Same task content should have same signature
    }

    @Test
    void testIdenticalTasksHaveSameSignature() throws Exception {
        SortedMap<String, Object> params1 = new TreeMap<>();
        params1.put("key", "value");
        SortedSet<ResourceKey> resources1 = new TreeSet<>();
        resources1.add(new ResourceKey("ENTITY", "123"));

        SortedMap<String, Object> params2 = new TreeMap<>();
        params2.put("key", "value");
        SortedSet<ResourceKey> resources2 = new TreeSet<>();
        resources2.add(new ResourceKey("ENTITY", "123"));

        Task task1 = new Task("ACTION", params1, resources1, null, true);
        Task task2 = new Task("ACTION", params2, resources2, null, true);

        assertEquals(task1.getSignature(), task2.getSignature());
    }

    @Test
    void testDifferentTasksHaveDifferentSignatures() throws Exception {
        SortedMap<String, Object> params1 = new TreeMap<>();
        params1.put("key", "value1");
        SortedSet<ResourceKey> resources1 = new TreeSet<>();

        SortedMap<String, Object> params2 = new TreeMap<>();
        params2.put("key", "value2");
        SortedSet<ResourceKey> resources2 = new TreeSet<>();

        Task task1 = new Task("ACTION", params1, resources1, null, true);
        Task task2 = new Task("ACTION", params2, resources2, null, true);

        assertNotEquals(task1.getSignature(), task2.getSignature());
    }

    // ========================================================================
    // isCompleted() Tests
    // ========================================================================

    @Test
    void testIsCompletedForUnscheduled() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        assertFalse(task.isCompleted());
    }

    @Test
    void testIsCompletedForScheduled() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        assertFalse(task.isCompleted());
    }

    @Test
    void testIsCompletedForStarted() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();
        assertFalse(task.isCompleted());
    }

    @Test
    void testIsCompletedForSuccessful() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();
        task.succeeded();
        assertTrue(task.isCompleted());
    }

    @Test
    void testIsCompletedForFailed() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();
        task.failed(new Exception("Test"));
        assertTrue(task.isCompleted());
    }

    @Test
    void testIsCompletedForAborted() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.aborted();
        assertTrue(task.isCompleted());
    }

    // ========================================================================
    // Allow Collapse Tests
    // ========================================================================

    @Test
    void testAllowCollapseTrue() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        assertTrue(task.isAllowingCollapse());
    }

    @Test
    void testAllowCollapseFalse() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, false);
        assertFalse(task.isAllowingCollapse());
    }

    // ========================================================================
    // Failure Tests
    // ========================================================================

    @Test
    void testGetFailureWhenNoFailure() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        assertNull(task.getFailure());
    }

    @Test
    void testGetFailureAfterFailed() throws Exception {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        task.markScheduled();
        task.beginHandling();

        Exception failure = new RuntimeException("Test failure");
        task.failed(failure);

        assertEquals(failure, task.getFailure());
    }

    // ========================================================================
    // toString() Tests
    // ========================================================================

    @Test
    void testToString() {
        Task task = new Task("MY_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);
        String str = task.toString();

        assertNotNull(str);
        assertTrue(str.contains("MY_ACTION"));
        assertTrue(str.contains(String.valueOf(task.getTaskId())));
    }

    // ========================================================================
    // Edge Case and Error Handling Tests
    // ========================================================================

    @Test
    void testDeserializeWithEmptyParameters() {
        String json = "{\"action\":\"ACTION\",\"params\":{},\"resources\":[]}";
        Task task = Task.deserialize(json, true, null);

        assertNotNull(task);
        assertTrue(task.getParameters().isEmpty());
        assertTrue(task.getResourceKeys().isEmpty());
    }

    @Test
    void testDeserializeWithNullParameters() {
        String json = "{\"action\":\"ACTION\",\"resources\":[]}";
        Task task = Task.deserialize(json, true, null);

        assertNotNull(task);
        assertTrue(task.getParameters().isEmpty());
    }

    @Test
    void testDeserializeWithComplexParameters() {
        String json = "{\"action\":\"ACTION\",\"params\":{" +
                      "\"str\":\"value\"," +
                      "\"num\":42," +
                      "\"list\":[1,2,3]," +
                      "\"map\":{\"nested\":\"value\"}" +
                      "},\"resources\":[]}";

        Task task = Task.deserialize(json, true, null);

        assertNotNull(task);
        Map<String, Object> params = task.getParameters();
        assertEquals("value", params.get("str"));
        // JSON deserialization may convert numbers to Long instead of Integer
        assertEquals(42L, ((Number)params.get("num")).longValue());
        assertTrue(params.get("list") instanceof List);
        assertTrue(params.get("map") instanceof Map);
    }

    @Test
    void testStatisticsWithNegativeElapsedTime() {
        // Test deserialize with negative elapsed time - should be clamped to 0
        String json = "{\"action\":\"ACTION\",\"params\":{},\"resources\":[]}";
        Task task = Task.deserialize(json, true, -500L);

        assertNotNull(task);
        // Should not throw exception
        Map<Task.Statistic, Number> stats = task.getStatistics();
        assertNotNull(stats);
    }

    @Test
    void testMultipleTasksWithDifferentGroups() {
        TaskGroup group1 = new TaskGroup();
        TaskGroup group2 = new TaskGroup();

        Task task1 = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group1, true);
        Task task2 = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group2, true);
        Task task3 = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        assertEquals(group1, task1.getTaskGroup());
        assertEquals(group2, task2.getTaskGroup());
        assertNull(task3.getTaskGroup());
        assertNotEquals(task1.getTaskGroup(), task2.getTaskGroup());
    }

    @Test
    void testStaticToJsonObjectBuilder() {
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        javax.json.JsonObjectBuilder builder = Task.toJsonObjectBuilder(task);

        assertNotNull(builder);

        javax.json.JsonObject jsonObj = builder.build();
        assertTrue(jsonObj.containsKey("action"));
    }

    @Test
    void testTaskStatisticGetUnits() {
        // Test the getUnits() method on Task.Statistic enum
        assertEquals("ms", Task.Statistic.unscheduledTime.getUnits());
        assertEquals("ms", Task.Statistic.pendingTime.getUnits());
        assertEquals("ms", Task.Statistic.handlingTime.getUnits());
        assertEquals("ms", Task.Statistic.lifespan.getUnits());
    }
}
