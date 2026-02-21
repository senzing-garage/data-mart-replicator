package com.senzing.listener.service.scheduling;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link TaskGroup}.
 * Tests are independent and can run in parallel.
 */
class TaskGroupTest {

    @Test
    void testConstructor() {
        TaskGroup group = new TaskGroup();
        assertNotNull(group);
        assertEquals(TaskGroup.State.OPEN, group.getState());
        assertEquals(0, group.getTaskCount());
        assertTrue(group.isFastFail());
    }

    @Test
    void testConstructorGeneratesUniqueGroupIds() {
        TaskGroup group1 = new TaskGroup();
        TaskGroup group2 = new TaskGroup();
        assertNotEquals(group1.getGroupId(), group2.getGroupId());
    }

    @Test
    void testCloseEmptyGroupBecomesSuccessful() {
        TaskGroup group = new TaskGroup();
        assertEquals(TaskGroup.State.OPEN, group.getState());

        group.close();
        // Empty group immediately becomes SUCCESSFUL when closed
        assertEquals(TaskGroup.State.SUCCESSFUL, group.getState());
        assertTrue(group.isCompleted());
    }

    @Test
    void testCloseIsIdempotent() {
        TaskGroup group = new TaskGroup();
        group.close();
        TaskGroup.State firstState = group.getState();

        group.close(); // Second close should have no effect
        assertEquals(firstState, group.getState());
    }

    @Test
    void testSetFastFailWhenOpen() {
        TaskGroup group = new TaskGroup();
        assertTrue(group.isFastFail());

        group.setFastFail(false);
        assertFalse(group.isFastFail());
    }

    @Test
    void testSetFastFailWhenClosedThrows() {
        TaskGroup group = new TaskGroup();
        group.close();
        assertThrows(IllegalStateException.class, () -> group.setFastFail(false));
    }

    @Test
    void testAddSingleTask() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        assertEquals(1, group.getTaskCount());
        assertEquals(1, group.getPendingCount());
    }

    @Test
    void testAddTaskWhenClosedThrows() {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.close();
        assertThrows(IllegalStateException.class, () -> group.addTask(task));
    }

    @Test
    void testSingleTaskLifecycle() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        // Schedule - markScheduled() internally calls group.taskScheduled()
        task.markScheduled();
        assertEquals(TaskGroup.State.SCHEDULED, group.getState());
        assertEquals(1, group.getScheduledCount());

        // Start - beginHandling() internally calls group.taskStarted()
        task.beginHandling();
        assertEquals(1, group.getStartedCount());

        // Complete - succeeded() internally calls group.taskSucceeded()
        task.succeeded();
        assertEquals(1, group.getSuccessCount());
        assertEquals(0, group.getPendingCount());
        assertTrue(group.isCompleted());
        assertEquals(TaskGroup.State.SUCCESSFUL, group.getState());
    }

    @Test
    void testSingleTaskFailure() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();
        task.beginHandling();
        task.failed(new Exception("Test"));

        assertEquals(1, group.getFailureCount());
        assertEquals(TaskGroup.State.FAILED, group.getState());
        assertTrue(group.isCompleted());
    }

    @Test
    void testTwoTasksSuccessful() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule first task
        task1.markScheduled();
        assertEquals(TaskGroup.State.SCHEDULING, group.getState());

        // Schedule second task - group transitions to SCHEDULED
        task2.markScheduled();
        assertEquals(TaskGroup.State.SCHEDULED, group.getState());

        // Complete first task
        task1.beginHandling();
        task1.succeeded();
        assertEquals(1, group.getSuccessCount());
        assertFalse(group.isCompleted()); // Still have task2 pending

        // Complete second task
        task2.beginHandling();
        task2.succeeded();
        assertEquals(2, group.getSuccessCount());
        assertTrue(group.isCompleted());
        assertEquals(TaskGroup.State.SUCCESSFUL, group.getState());
    }

    @Test
    void testFastFailWithFailure() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(true);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule both tasks
        task1.markScheduled();
        task2.markScheduled();

        // Fail first task
        task1.beginHandling();
        task1.failed(new Exception("Test"));

        assertEquals(TaskGroup.State.FAILED, group.getState());
        // With fast fail, group is considered complete even though task2 is pending
        assertTrue(group.isCompleted());
    }

    @Test
    void testNoFastFailWithFailure() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(false);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule both
        task1.markScheduled();
        task2.markScheduled();

        // Fail first task
        task1.beginHandling();
        task1.failed(new Exception("Test"));

        assertEquals(TaskGroup.State.FAILED, group.getState());
        // Without fast fail, not complete until all tasks done
        assertFalse(group.isCompleted());

        // Complete second task
        task2.beginHandling();
        task2.succeeded();

        assertTrue(group.isCompleted());
    }

    @Test
    void testGetStatistics() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.containsKey(TaskGroup.Stat.taskCount));
        assertEquals(1, stats.get(TaskGroup.Stat.taskCount).intValue());
    }

    @Test
    void testAddTaskWithWrongGroupThrows() {
        TaskGroup group1 = new TaskGroup();
        TaskGroup group2 = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group1, true);

        assertThrows(IllegalArgumentException.class, () -> group2.addTask(task));
    }

    @Test
    void testTaskScheduledWithWrongStateThrows() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        // Call taskScheduled directly without markScheduled - task is still UNSCHEDULED
        assertThrows(IllegalStateException.class, () -> group.taskScheduled(task));
    }

    @Test
    void testTaskStartedWithWrongStateThrows() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();

        // Call taskStarted directly without beginHandling - task is still SCHEDULED
        assertThrows(IllegalArgumentException.class, () -> group.taskStarted(task));
    }

    @Test
    void testToString() {
        TaskGroup group = new TaskGroup();
        String str = group.toString();

        assertNotNull(str);
        // TaskGroup uses default Object.toString()
        assertTrue(str.contains("TaskGroup@"));
    }

    @Test
    void testAwaitCompletionWhenComplete() throws Exception {
        TaskGroup group = new TaskGroup();
        group.close();

        // Should return immediately
        group.awaitCompletion();
        assertTrue(group.isCompleted());
    }

    @Test
    void testStatisticsOpenTime() throws Exception {
        TaskGroup group = new TaskGroup();

        Thread.sleep(50);

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number openTime = stats.get(TaskGroup.Stat.openTime);

        assertNotNull(openTime);
        assertTrue(openTime.longValue() >= 50);
    }

    @Test
    void testGetPendingCount() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        assertEquals(2, group.getPendingCount());

        // Complete first task
        task1.markScheduled();
        task1.beginHandling();
        task1.succeeded();

        assertEquals(1, group.getPendingCount());

        // Complete second task
        task2.markScheduled();
        task2.beginHandling();
        task2.succeeded();

        assertEquals(0, group.getPendingCount());
    }

    @Test
    void testIsCompletedReturnsFalseWhenOpen() {
        TaskGroup group = new TaskGroup();
        assertFalse(group.isCompleted());
    }

    @Test
    void testGetAbortedCount() {
        TaskGroup group = new TaskGroup();
        assertEquals(0, group.getAbortedCount());
    }

    @Test
    void testThreeTasksWithMixedOutcomes() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(false); // Don't fast fail so all tasks get processed

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task3 = new Task("ACTION3", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.addTask(task3);
        group.close();

        // Schedule all
        task1.markScheduled();
        task2.markScheduled();
        task3.markScheduled();

        // Task 1 succeeds
        task1.beginHandling();
        task1.succeeded();

        // Task 2 fails
        task2.beginHandling();
        task2.failed(new Exception("Test"));

        // Task 3 succeeds
        task3.beginHandling();
        task3.succeeded();

        assertEquals(2, group.getSuccessCount());
        assertEquals(1, group.getFailureCount());
        assertEquals(TaskGroup.State.FAILED, group.getState());
        assertTrue(group.isCompleted());
    }

    @Test
    void testGetTasks() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);

        List<Task> tasks = group.getTasks();
        assertEquals(2, tasks.size());
        assertTrue(tasks.contains(task1));
        assertTrue(tasks.contains(task2));
    }

    @Test
    void testGetFailedTasks() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(false);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task3 = new Task("ACTION3", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.addTask(task3);
        group.close();

        task1.markScheduled();
        task1.beginHandling();
        task1.succeeded();

        task2.markScheduled();
        task2.beginHandling();
        task2.failed(new Exception("Failure 1"));

        task3.markScheduled();
        task3.beginHandling();
        task3.failed(new Exception("Failure 2"));

        List<Task> failedTasks = group.getFailedTasks();
        assertEquals(2, failedTasks.size());
        assertTrue(failedTasks.contains(task2));
        assertTrue(failedTasks.contains(task3));
    }

    @Test
    void testGetAbortedTasks() throws Exception {
        // This test requires a scenario where tasks get aborted
        // Aborted tasks occur when group is in FAILED state with fastFail=true
        TaskGroup group = new TaskGroup();
        group.setFastFail(true);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule task1, fail it
        task1.markScheduled();
        task1.beginHandling();
        task1.failed(new Exception("Test"));

        // Now group is FAILED with fastFail=true
        assertEquals(TaskGroup.State.FAILED, group.getState());

        // Abort task2
        task2.aborted();

        List<Task> abortedTasks = group.getAbortedTasks();
        assertEquals(1, abortedTasks.size());
        assertTrue(abortedTasks.contains(task2));
    }

    @Test
    void testIsFirstFailure() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(false);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        task1.markScheduled();
        task1.beginHandling();
        task1.failed(new Exception("First failure"));

        task2.markScheduled();
        task2.beginHandling();
        task2.failed(new Exception("Second failure"));

        // task1 is the first failure
        assertTrue(group.isFirstFailure(task1));
        assertFalse(group.isFirstFailure(task2));
    }

    @Test
    void testStatisticsWithSchedulingTime() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule first task
        task1.markScheduled();

        Thread.sleep(50);

        // Schedule second task
        task2.markScheduled();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number schedulingTime = stats.get(TaskGroup.Stat.unscheduledTime);
        assertNotNull(schedulingTime);
    }

    @Test
    void testEnsureTaskPresentThrowsForNotAdded() {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        // Don't add the task
        assertThrows(IllegalStateException.class, () -> group.ensureTaskPresent(task));
    }

    @Test
    void testAwaitCompletionWithMaximumInterval() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        // Start task completion in separate thread
        new Thread(() -> {
            try {
                Thread.sleep(100);
                task.markScheduled();
                task.beginHandling();
                task.succeeded();
            } catch (Exception e) {
                // Ignore
            }
        }).start();

        // Wait with short intervals
        group.awaitCompletion(50, 500);

        assertTrue(group.isCompleted());
    }

    @Test
    void testAwaitCompletionTimeout() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        // Don't complete the task, wait should timeout
        long start = System.currentTimeMillis();
        group.awaitCompletion(100, 200);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= 100);
        assertFalse(group.isCompleted());
    }

    @Test
    void testStatisticsTotalHandlingTime() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        task1.markScheduled();
        task1.beginHandling();
        Thread.sleep(50);
        task1.succeeded();

        task2.markScheduled();
        task2.beginHandling();
        Thread.sleep(50);
        task2.succeeded();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number totalHandling = stats.get(TaskGroup.Stat.totalHandlingTime);
        assertNotNull(totalHandling);
        assertTrue(totalHandling.longValue() >= 100);
    }

    @Test
    void testStatisticsLongestHandlingTime() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        task1.markScheduled();
        task1.beginHandling();
        Thread.sleep(30);
        task1.succeeded();

        task2.markScheduled();
        task2.beginHandling();
        Thread.sleep(100); // Longer handling time
        task2.succeeded();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number longest = stats.get(TaskGroup.Stat.longestHandlingTime);
        assertNotNull(longest);
        assertTrue(longest.longValue() >= 100);
    }

    @Test
    void testStatisticsRoundTripTime() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();
        Thread.sleep(50);
        task.beginHandling();
        task.succeeded();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number roundTrip = stats.get(TaskGroup.Stat.roundTripTime);
        assertNotNull(roundTrip);
        assertTrue(roundTrip.longValue() >= 50);
    }

    @Test
    void testStatisticsLifespan() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        Thread.sleep(50);
        group.addTask(task);
        group.close();

        task.markScheduled();
        task.beginHandling();
        task.succeeded();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();
        Number lifespan = stats.get(TaskGroup.Stat.lifespan);
        assertNotNull(lifespan);
        assertTrue(lifespan.longValue() >= 50);
    }

    @Test
    void testStatisticsAllKeysPresent() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();
        Thread.sleep(50);
        task.beginHandling();
        Thread.sleep(50);
        task.succeeded();

        Map<TaskGroup.Statistic, Number> stats = group.getStatistics();

        // These should always be present
        assertTrue(stats.containsKey(TaskGroup.Stat.openTime));
        assertTrue(stats.containsKey(TaskGroup.Stat.unscheduledTime));
        assertTrue(stats.containsKey(TaskGroup.Stat.totalHandlingTime));
        assertTrue(stats.containsKey(TaskGroup.Stat.roundTripTime));
        assertTrue(stats.containsKey(TaskGroup.Stat.lifespan));
        assertTrue(stats.containsKey(TaskGroup.Stat.taskCount));
        assertTrue(stats.containsKey(TaskGroup.Stat.pendingCount));
        assertTrue(stats.containsKey(TaskGroup.Stat.successCount));
        assertTrue(stats.containsKey(TaskGroup.Stat.failureCount));

        // pendingTime and longestHandlingTime may be conditionally included
    }

    @Test
    void testIsConcludingTaskForFirstFailure() throws Exception {
        TaskGroup group = new TaskGroup();
        group.setFastFail(false);

        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        task1.markScheduled();
        task1.beginHandling();
        task1.failed(new Exception("Fail"));

        task2.markScheduled();
        task2.beginHandling();
        task2.succeeded();

        // task2 is concluding task (last one)
        assertTrue(group.isConcludingTask(task2));
        assertFalse(group.isConcludingTask(task1));
    }

    @Test
    void testGetSchedulingTime() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        task1.markScheduled();
        Thread.sleep(50);
        task2.markScheduled();

        long schedulingTime = group.getSchedulingTime();
        assertTrue(schedulingTime >= 50);
    }

    @Test
    void testAddTaskWhenTaskAlreadyInGroup() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);

        // Adding same task again should be idempotent (updates taskStateMap)
        group.addTask(task);

        assertEquals(1, group.getTaskCount());
    }

    @Test
    void testTaskSucceededWithoutBeingStartedThrows() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();
        // Call taskSucceeded directly without beginHandling

        assertThrows(IllegalArgumentException.class, () -> group.taskSucceeded(task));
    }

    @Test
    void testTaskFailedWithoutTaskBeingFailed() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        task.markScheduled();
        task.beginHandling();
        // Don't call task.failed()

        // taskFailed() expects task to be in FAILED state
        assertThrows(IllegalStateException.class, () -> group.taskFailed(task));
    }

    @Test
    void testGetSchedulingTimeBeforeAllTasksScheduled() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task1 = new Task("ACTION1", new TreeMap<>(), new TreeSet<>(), group, true);
        Task task2 = new Task("ACTION2", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task1);
        group.addTask(task2);
        group.close();

        // Schedule only first task
        task1.markScheduled();

        // Get scheduling time before all tasks are scheduled
        long schedulingTime = group.getSchedulingTime();
        assertTrue(schedulingTime >= 0, "Scheduling time should be non-negative");
    }

    @Test
    void testIsFirstFailureWithNullTask() {
        TaskGroup group = new TaskGroup();

        assertFalse(group.isFirstFailure(null), "Null task should not be first failure");
    }

    @Test
    void testIsConcludingTaskWithNullTask() {
        TaskGroup group = new TaskGroup();

        assertFalse(group.isConcludingTask(null), "Null task should not be concluding task");
    }

    @Test
    void testGetUnscheduledTimeBeforeAnyScheduled() throws Exception {
        TaskGroup group = new TaskGroup();
        Task task = new Task("ACTION", new TreeMap<>(), new TreeSet<>(), group, true);

        group.addTask(task);
        group.close();

        Thread.sleep(50);

        // Get time before any tasks are scheduled
        long unscheduledTime = group.getUnscheduledTime();
        assertTrue(unscheduledTime >= 50, "Unscheduled time should accumulate");
    }

    @Test
    void testGetPendingTimeBeforeScheduled() {
        TaskGroup group = new TaskGroup();

        // No tasks scheduled yet
        long pendingTime = group.getPendingTime();
        assertEquals(-1L, pendingTime, "Pending time should be -1 when no tasks scheduled");
    }

    @Test
    void testTaskGroupStatGetUnits() {
        // Test getUnits() on TaskGroup.Stat enum
        assertEquals("ms", TaskGroup.Stat.openTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.unscheduledTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.pendingTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.totalHandlingTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.longestHandlingTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.roundTripTime.getUnits());
        assertEquals("ms", TaskGroup.Stat.lifespan.getUnits());
        assertEquals("tasks", TaskGroup.Stat.taskCount.getUnits());
        assertEquals("tasks", TaskGroup.Stat.pendingCount.getUnits());
        assertEquals("tasks", TaskGroup.Stat.successCount.getUnits());
        assertEquals("tasks", TaskGroup.Stat.failureCount.getUnits());
    }
}

