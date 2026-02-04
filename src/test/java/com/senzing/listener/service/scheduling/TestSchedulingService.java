package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.locking.LockingService;
import com.senzing.util.Quantified;

import javax.json.JsonObject;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test-friendly SchedulingService implementation that allows testing
 * AbstractListenerService without requiring a database. This class is in
 * the scheduling package to access package-private TaskBuilder constructor.
 */
public class TestSchedulingService implements SchedulingService {
    private State state = State.UNINITIALIZED;
    private TaskHandler taskHandler;
    private Map<Quantified.Statistic, Number> stats = new LinkedHashMap<>();
    private boolean simulateFailures = false;
    private int failureCount = 0;
    private boolean throwOnCommit = false;
    private RuntimeException commitException = null;
    private Long pendingTaskCount = null;
    private Long pendingFollowUpCount = null;
    private AtomicLong lastTaskScheduledNanoTime = new AtomicLong(-1L);

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void init(JsonObject config, TaskHandler taskHandler) throws ServiceSetupException {
        this.taskHandler = taskHandler;
        this.state = State.READY;
    }

    @Override
    public Scheduler createScheduler(boolean followUp) {
        return new TestScheduler(this, followUp);
    }

    @Override
    public Scheduler createScheduler() {
        return createScheduler(false);
    }

    @Override
    public TaskHandler getTaskHandler() {
        return taskHandler;
    }

    @Override
    public LockingService getLockingService() {
        return null;
    }

    @Override
    public void destroy() {
        state = State.DESTROYED;
    }

    @Override
    public Map<Quantified.Statistic, Number> getStatistics() {
        return stats;
    }

    public void setSimulateFailures(boolean simulateFailures) {
        this.simulateFailures = simulateFailures;
    }

    public void setFailureCount(int failureCount) {
        this.failureCount = failureCount;
    }

    public void setThrowOnCommit(boolean throwOnCommit) {
        this.throwOnCommit = throwOnCommit;
    }

    public void setCommitException(RuntimeException e) {
        this.commitException = e;
    }

    public boolean isSimulateFailures() {
        return simulateFailures;
    }

    public int getFailureCount() {
        return failureCount;
    }

    public boolean isThrowOnCommit() {
        return throwOnCommit;
    }

    public RuntimeException getCommitException() {
        return commitException;
    }

    @Override
    public Long getRemainingTasksCount() {
        return this.pendingTaskCount;
    }

    @Override
    public Long getRemainingFollowUpTasksCount() {
        return this.pendingFollowUpCount;
    }

    @Override
    public long getLastTaskActivityNanoTime() {
        return this.lastTaskScheduledNanoTime.get();
    }

    public void updateLastTaskScheduledNanoTime() {
        this.lastTaskScheduledNanoTime.set(System.nanoTime());
    }

    /**
     * Sets the pending task count for testing purposes.
     * @param count The count to set, or null to indicate unknown.
     */
    public void setPendingTaskCount(Long count) {
        this.pendingTaskCount = count;
    }

    /**
     * Sets the pending follow-up task count for testing purposes.
     * @param count The count to set, or null to indicate unknown.
     */
    public void setPendingFollowUpCount(Long count) {
        this.pendingFollowUpCount = count;
    }

    /**
     * Test scheduler that creates real TaskBuilders.
     */
    public static class TestScheduler extends Scheduler {
        private TestSchedulingService service;
        private TestTaskGroup taskGroup;
        private List<Task> pendingTasks = new LinkedList<>();
        private boolean followUp;

        TestScheduler(TestSchedulingService service, boolean followUp) {
            this.service = service;
            this.followUp = followUp;
            this.taskGroup = followUp ? null : new TestTaskGroup(service);
        }

        @Override
        public TaskBuilder createTaskBuilder(String action) {
            // This works because we're in the same package as TaskBuilder
            return new TaskBuilder(this, action);
        }

        @Override
        public TaskGroup getTaskGroup() {
            return taskGroup;
        }

        @Override
        protected void schedule(Task task) {
            // Note: TaskBuilder already calls taskGroup.addTask(task) before calling
            // scheduler.schedule(task), so we don't call addTask here
            pendingTasks.add(task);
        }

        @Override
        public int getPendingCount() {
            return pendingTasks.size();
        }

        @Override
        public int commit() throws ServiceExecutionException {
            if (service.isThrowOnCommit() && service.getCommitException() != null) {
                throw service.getCommitException();
            }
            if (taskGroup != null) {
                // Close the task group first
                taskGroup.close();
                // Now mark all tasks as scheduled (must be after close)
                for (Task task : pendingTasks) {
                    this.service.updateLastTaskScheduledNanoTime();
                    task.markScheduled();
                }
                // Note: awaitCompletion() is called by AbstractListenerService.process()
                // after commit() returns, so we don't call it here
            }
            int count = pendingTasks.size();
            pendingTasks.clear();
            return count;
        }
    }

    /**
     * Test task group that can simulate failures.
     * Delegates most operations to the parent TaskGroup to maintain proper state machine.
     */
    public static class TestTaskGroup extends TaskGroup {
        private List<Task> localTasks = new ArrayList<>();
        private TestSchedulingService service;
        private boolean completionProcessed = false;

        TestTaskGroup(TestSchedulingService service) {
            this.service = service;
        }

        @Override
        public void awaitCompletion() {
            // Guard against double processing - only process tasks once
            synchronized (this) {
                if (completionProcessed) {
                    // Already processed, just wait for completion like the parent
                    super.awaitCompletion();
                    return;
                }
                completionProcessed = true;
            }

            // Simulate completion by properly transitioning tasks through their states
            if (service.isSimulateFailures()) {
                int failCount = service.getFailureCount();
                int taskCount = localTasks.size();
                for (int i = 0; i < taskCount; i++) {
                    Task task = localTasks.get(i);
                    // Transition: SCHEDULED -> STARTED -> (FAILED or SUCCESSFUL)
                    task.beginHandling();
                    if (i < failCount) {
                        task.failed(new ServiceExecutionException("Simulated task failure " + (i + 1)));
                    } else {
                        task.succeeded();
                    }
                }
            } else {
                // All tasks succeed
                for (Task task : localTasks) {
                    task.beginHandling();
                    task.succeeded();
                }
            }
        }

        @Override
        protected void addTask(Task task) {
            // Call parent to properly register in taskStateMap and maintain state
            super.addTask(task);
            localTasks.add(task);
        }
    }
}
