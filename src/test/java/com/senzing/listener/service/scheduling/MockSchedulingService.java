package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock implementation of {@link AbstractSchedulingService} for testing.
 * Unlike {@link TestSchedulingService} (used for communication package tests),
 * this mock actually exercises the AbstractSchedulingService code paths.
 */
class MockSchedulingService extends AbstractSchedulingService {

    private List<Task> allScheduledTasks = new ArrayList<>();
    private List<ScheduledTask> followUpQueue = new ArrayList<>();

    public MockSchedulingService() {
        // Default constructor
    }

    public List<Task> getScheduledTasks() {
        return new ArrayList<>(allScheduledTasks);
    }

    @Override
    protected void enqueueFollowUpTask(Task task) throws ServiceExecutionException {
        allScheduledTasks.add(task);
        followUpQueue.add(new ScheduledTask(task));
    }

    @Override
    protected List<ScheduledTask> dequeueFollowUpTasks(int count) throws ServiceExecutionException {
        List<ScheduledTask> result = new ArrayList<>();
        int toFetch = Math.min(count, followUpQueue.size());
        for (int i = 0; i < toFetch; i++) {
            result.add(followUpQueue.remove(0));
        }
        return result;
    }

    @Override
    protected void renewFollowUpTasks(List<ScheduledTask> tasks) throws ServiceExecutionException {
        // No-op for mock
    }

    @Override
    protected void completeFollowUpTask(ScheduledTask task) throws ServiceExecutionException {
        // No-op for mock
    }

    @Override
    protected void doInit(JsonObject config) throws ServiceSetupException {
        // No-op for mock
    }

    @Override
    protected void doDestroy() {
        // No-op for mock
    }

    @Override
    protected Long countScheduledFollowUpTasks() {
        return ((long) this.followUpQueue.size());
    }
}
