package com.senzing.listener.service;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import com.senzing.listener.service.scheduling.*;
import com.senzing.util.Quantified;
import org.junit.jupiter.api.*;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.senzing.listener.service.AbstractListenerService.MessagePart;

import static com.senzing.listener.service.AbstractListenerService.MessagePart.*;
import static com.senzing.listener.service.ListenerService.State.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link AbstractListenerService}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AbstractListenerServiceTest {

    /**
     * Concrete test implementation of AbstractListenerService.
     */
    private static class TestListenerService extends AbstractListenerService {
        private boolean doInitCalled = false;
        private boolean doDestroyCalled = false;
        private boolean throwOnDoInit = false;
        private RuntimeException initException = null;
        private boolean handleTaskCalled = false;
        private String lastAction = null;
        private Map<String, Object> lastParameters = null;
        private int lastMultiplicity = 0;
        private boolean throwOnHandleTask = false;
        private ServiceExecutionException handleTaskException = null;
        private boolean useTestSchedulingService = true;
        private TestSchedulingService testSchedulingService = null;

        TestListenerService(Map<MessagePart, String> messagePartMap) {
            super(messagePartMap);
        }

        TestListenerService(Map<MessagePart, String> messagePartMap, boolean useTestSchedulingService) {
            super(messagePartMap);
            this.useTestSchedulingService = useTestSchedulingService;
        }

        @Override
        protected SchedulingService initSchedulingService(JsonObject config) throws ServiceSetupException {
            if (useTestSchedulingService) {
                testSchedulingService = new TestSchedulingService();
                testSchedulingService.init(config, this.getTaskHandler());
                return testSchedulingService;
            }
            return super.initSchedulingService(config);
        }

        @Override
        protected void doInit(JsonObject config) throws ServiceSetupException {
            doInitCalled = true;
            if (throwOnDoInit) {
                if (initException != null) {
                    throw initException;
                }
                throw new ServiceSetupException("Test init exception");
            }
        }

        @Override
        protected void handleTask(String action, Map<String, Object> parameters,
                                  int multiplicity, Scheduler followUpScheduler)
                throws ServiceExecutionException {
            handleTaskCalled = true;
            lastAction = action;
            lastParameters = parameters;
            lastMultiplicity = multiplicity;
            if (throwOnHandleTask) {
                if (handleTaskException != null) {
                    throw handleTaskException;
                }
                throw new ServiceExecutionException("Test handle task exception");
            }
        }

        @Override
        protected void doDestroy() {
            doDestroyCalled = true;
        }

        public boolean isDoInitCalled() {
            return doInitCalled;
        }

        public boolean isDoDestroyCalled() {
            return doDestroyCalled;
        }

        public void setThrowOnDoInit(boolean throwOnDoInit) {
            this.throwOnDoInit = throwOnDoInit;
        }

        public void setInitException(RuntimeException e) {
            this.initException = e;
        }

        public boolean isHandleTaskCalled() {
            return handleTaskCalled;
        }

        public String getLastAction() {
            return lastAction;
        }

        public Map<String, Object> getLastParameters() {
            return lastParameters;
        }

        public int getLastMultiplicity() {
            return lastMultiplicity;
        }

        public void setThrowOnHandleTask(boolean throwOnHandleTask) {
            this.throwOnHandleTask = throwOnHandleTask;
        }

        public void setHandleTaskException(ServiceExecutionException e) {
            this.handleTaskException = e;
        }

        // Expose protected methods for testing
        public void testSetState(ListenerService.State state) {
            this.setState(state);
        }

        public SchedulingService testGetSchedulingService() {
            return this.getSchedulingService();
        }

        public TaskHandler testGetTaskHandler() {
            return this.getTaskHandler();
        }

        public String testGetActionForMessagePart(MessagePart part) {
            return this.getActionForMessagePart(part);
        }

        public TestSchedulingService getTestSchedulingService() {
            return testSchedulingService;
        }
    }

    // ========================================================================
    // Helper method to create test messages
    // ========================================================================

    private JsonObject createSimpleInfoMessage(String dataSource, String recordId, Long... affectedEntityIds) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("DATA_SOURCE", dataSource);
        builder.add("RECORD_ID", recordId);

        if (affectedEntityIds != null && affectedEntityIds.length > 0) {
            JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
            for (Long entityId : affectedEntityIds) {
                arrayBuilder.add(Json.createObjectBuilder().add("ENTITY_ID", entityId));
            }
            builder.add("AFFECTED_ENTITIES", arrayBuilder);
        }

        return builder.build();
    }

    private JsonObject createInfoMessageWithInterestingEntities(String dataSource, String recordId) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("DATA_SOURCE", dataSource);
        builder.add("RECORD_ID", recordId);

        // Add affected entities
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        affectedBuilder.add(Json.createObjectBuilder().add("ENTITY_ID", 100L));
        builder.add("AFFECTED_ENTITIES", affectedBuilder);

        // Add interesting entities
        JsonObjectBuilder interestingBuilder = Json.createObjectBuilder();
        JsonArrayBuilder entitiesBuilder = Json.createArrayBuilder();

        // First interesting entity
        JsonObjectBuilder entity1 = Json.createObjectBuilder();
        entity1.add("ENTITY_ID", 200L);
        entity1.add("DEGREES", 1);
        JsonArrayBuilder flagsBuilder1 = Json.createArrayBuilder();
        flagsBuilder1.add("FLAG1");
        flagsBuilder1.add("FLAG2");
        entity1.add("FLAGS", flagsBuilder1);

        // Add sample records
        JsonArrayBuilder sampleRecordsBuilder = Json.createArrayBuilder();
        JsonObjectBuilder sampleRecord = Json.createObjectBuilder();
        sampleRecord.add("DATA_SOURCE", "TEST_DS");
        sampleRecord.add("RECORD_ID", "REC123");
        sampleRecordsBuilder.add(sampleRecord);
        entity1.add("SAMPLE_RECORDS", sampleRecordsBuilder);

        entitiesBuilder.add(entity1);

        // Second interesting entity
        JsonObjectBuilder entity2 = Json.createObjectBuilder();
        entity2.add("ENTITY_ID", 201L);
        entity2.add("DEGREES", 2);
        JsonArrayBuilder flagsBuilder2 = Json.createArrayBuilder();
        flagsBuilder2.add("FLAG3");
        entity2.add("FLAGS", flagsBuilder2);
        entity2.add("SAMPLE_RECORDS", Json.createArrayBuilder());
        entitiesBuilder.add(entity2);

        interestingBuilder.add("ENTITIES", entitiesBuilder);
        builder.add("INTERESTING_ENTITIES", interestingBuilder);

        return builder.build();
    }

    private JsonObject createInfoMessageWithNotices(String dataSource, String recordId) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("DATA_SOURCE", dataSource);
        builder.add("RECORD_ID", recordId);

        // Add affected entities
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        affectedBuilder.add(Json.createObjectBuilder().add("ENTITY_ID", 100L));
        builder.add("AFFECTED_ENTITIES", affectedBuilder);

        // Build notices array
        JsonArrayBuilder noticesBuilder = Json.createArrayBuilder();
        JsonObjectBuilder notice1 = Json.createObjectBuilder();
        notice1.add("CODE", "NOTICE_CODE_1");
        notice1.add("DESCRIPTION", "First test notice");
        noticesBuilder.add(notice1);

        JsonObjectBuilder notice2 = Json.createObjectBuilder();
        notice2.add("CODE", "NOTICE_CODE_2");
        notice2.add("DESCRIPTION", "Second test notice");
        noticesBuilder.add(notice2);

        // NOTICES must be INSIDE INTERESTING_ENTITIES per SzInfoMessage parsing
        JsonObjectBuilder interestingBuilder = Json.createObjectBuilder();
        interestingBuilder.add("ENTITIES", Json.createArrayBuilder());
        interestingBuilder.add("NOTICES", noticesBuilder);
        builder.add("INTERESTING_ENTITIES", interestingBuilder);

        return builder.build();
    }

    // ========================================================================
    // getState() tests
    // ========================================================================

    @Test
    @Order(100)
    void testInitialState() {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        assertEquals(UNINITIALIZED, service.getState());
    }

    // ========================================================================
    // init() tests - covers lines 332-364
    // ========================================================================

    @Test
    @Order(200)
    void testInitAlreadyInitialized() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // First init should succeed
        service.init(null);
        assertEquals(AVAILABLE, service.getState());

        // Second init should throw IllegalStateException (covers lines 332-334)
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.init(null));
        assertTrue(exception.getMessage().contains("Cannot initialize"));
        assertTrue(exception.getMessage().contains(UNINITIALIZED.toString()));

        service.destroy();
    }

    @Test
    @Order(300)
    void testInitWithServiceSetupException() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.setThrowOnDoInit(true);

        // Should throw ServiceSetupException (covers lines 357-359)
        new SystemErr().execute(() -> {
            ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                    () -> service.init(null));
            assertTrue(exception.getMessage().contains("Test init exception"));
            assertEquals(UNINITIALIZED, service.getState());
        });
    }

    @Test
    @Order(400)
    void testInitWithRuntimeException() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.setThrowOnDoInit(true);
        service.setInitException(new RuntimeException("Test runtime exception"));

        // Should wrap in ServiceSetupException (covers lines 361-363)
        new SystemErr().execute(() -> {
            ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                    () -> service.init(null));
            assertNotNull(exception.getCause());
            assertTrue(exception.getCause().getMessage().contains("Test runtime exception"));
            assertEquals(UNINITIALIZED, service.getState());
        });
    }

    @Test
    @Order(500)
    void testInitWithNullConfig() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Should use empty config when null (covers line 343)
        service.init(null);
        assertTrue(service.isDoInitCalled());
        assertEquals(AVAILABLE, service.getState());
        service.destroy();
    }

    // ========================================================================
    // initSchedulingService() tests - covers lines 411-444
    // ========================================================================

    @Test
    @Order(600)
    void testInitSchedulingServiceWithInvalidClass() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        // Use real scheduling service initialization for this test
        TestListenerService service = new TestListenerService(messagePartMap, false);

        JsonObject config = Json.createObjectBuilder()
                .add(AbstractListenerService.SCHEDULING_SERVICE_CLASS_KEY, "java.lang.String")
                .build();

        // Should throw ServiceSetupException for non-SchedulingService class (covers lines 419-422)
        new SystemErr().execute(() -> {
            ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                    () -> service.init(config));
            assertTrue(exception.getMessage().contains("must") ||
                    exception.getMessage().contains("implement") ||
                    exception.getMessage().contains(SchedulingService.class.getName()));
        });
    }

    @Test
    @Order(700)
    void testInitSchedulingServiceWithNonExistentClass() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        // Use real scheduling service initialization for this test
        TestListenerService service = new TestListenerService(messagePartMap, false);

        JsonObject config = Json.createObjectBuilder()
                .add(AbstractListenerService.SCHEDULING_SERVICE_CLASS_KEY,
                        "com.nonexistent.NonExistentClass")
                .build();

        // Should wrap ClassNotFoundException (covers lines 442-443)
        new SystemErr().execute(() -> {
            ServiceSetupException exception = assertThrows(ServiceSetupException.class,
                    () -> service.init(config));
            assertTrue(exception.getMessage().contains("Failed to initialize") ||
                    exception.getCause() instanceof ClassNotFoundException);
        });
    }

    // ========================================================================
    // getDefaultSchedulingServiceConfig() test - covers line 486
    // ========================================================================

    @Test
    @Order(800)
    void testGetDefaultSchedulingServiceConfig() {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Default implementation returns null (covers line 486)
        assertNull(service.getDefaultSchedulingServiceConfig());
    }

    @Test
    @Order(900)
    void testGetDefaultSchedulingServiceClassName() {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        assertEquals(AbstractListenerService.DEFAULT_SCHEDULING_SERVICE_CLASS_NAME,
                service.getDefaultSchedulingServiceClassName());
    }

    // ========================================================================
    // waitUntilReady() tests - covers lines 284-298
    // ========================================================================

    @Test
    @Order(1000)
    void testWaitUntilReadyInAvailableState() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Should return TRUE when AVAILABLE (covers lines 286-287)
        assertEquals(Boolean.TRUE, service.waitUntilReady(0L));
        service.destroy();
    }

    @Test
    @Order(1100)
    void testWaitUntilReadyInDestroyingState() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Set state to DESTROYING
        service.testSetState(DESTROYING);

        // Should return null when DESTROYING (covers lines 288-290)
        assertNull(service.waitUntilReady(0L));
    }

    @Test
    @Order(1200)
    void testWaitUntilReadyInDestroyedState() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);
        service.destroy();

        // Should return null when DESTROYED (covers lines 288-290)
        assertNull(service.waitUntilReady(0L));
    }

    @Test
    @Order(1300)
    void testWaitUntilReadyWithNegativeTimeout() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Use a separate thread to initialize after a delay
        CountDownLatch latch = new CountDownLatch(1);
        Thread initThread = new Thread(() -> {
            try {
                Thread.sleep(100);
                service.init(null);
                latch.countDown();
            } catch (Exception e) {
                // ignore
            }
        });

        // Start the init thread
        initThread.start();

        // Wait with negative timeout (indefinite wait) - covers lines 292-293
        Boolean result = service.waitUntilReady(-1L);
        latch.await(2, TimeUnit.SECONDS);

        assertEquals(Boolean.TRUE, result);
        initThread.join(1000);
        service.destroy();
    }

    @Test
    @Order(1400)
    void testWaitUntilReadyWithPositiveTimeoutExpires() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // With timeout but no state change, should return FALSE (covers lines 294-297)
        Boolean result = service.waitUntilReady(50L);
        assertEquals(Boolean.FALSE, result);
    }

    @Test
    @Order(1450)
    void testWaitUntilReadyWithPositiveTimeout() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Use a separate thread to initialize after a delay
        CountDownLatch latch = new CountDownLatch(1);
        Thread initThread = new Thread(() -> {
            try {
                Thread.sleep(100);
                service.init(null);
                latch.countDown();
            } catch (Exception e) {
                // ignore
            }
        });

        // Start the init thread
        initThread.start();

        // Wait with positive timeout - covers lines 294-296
        Boolean result = service.waitUntilReady(500L);
        latch.await(1, TimeUnit.SECONDS);

        assertNotNull(result);
        initThread.join(1000);
        service.destroy();
    }

    // ========================================================================
    // getStatistics() test - covers line 223
    // ========================================================================

    @Test
    @Order(1500)
    void testGetStatistics() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Should delegate to scheduling service (covers line 223)
        Map<Quantified.Statistic, Number> stats = service.getStatistics();
        assertNotNull(stats);
        service.destroy();
    }

    // ========================================================================
    // getSchedulingService() test - covers line 824
    // ========================================================================

    @Test
    @Order(1600)
    void testGetSchedulingService() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Should return the backing scheduling service (covers line 824)
        SchedulingService schedulingService = service.testGetSchedulingService();
        assertNotNull(schedulingService);
        service.destroy();
    }

    @Test
    @Order(1700)
    void testGetTaskHandler() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        TaskHandler handler = service.testGetTaskHandler();
        assertNotNull(handler);
        service.destroy();
    }

    // ========================================================================
    // getActionForMessagePart() tests - covers line 625
    // ========================================================================

    @Test
    @Order(1800)
    void testGetActionForMessagePart() {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "AFFECTED_ACTION");
        messagePartMap.put(RECORD, "RECORD_ACTION");
        messagePartMap.put(INTERESTING_ENTITY, "INTERESTING_ACTION");
        messagePartMap.put(NOTICE, "NOTICE_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Should return mapped action (covers line 625)
        assertEquals("AFFECTED_ACTION", service.testGetActionForMessagePart(AFFECTED_ENTITY));
        assertEquals("RECORD_ACTION", service.testGetActionForMessagePart(RECORD));
        assertEquals("INTERESTING_ACTION", service.testGetActionForMessagePart(INTERESTING_ENTITY));
        assertEquals("NOTICE_ACTION", service.testGetActionForMessagePart(NOTICE));
    }

    // ========================================================================
    // destroy() tests - covers lines 783-816
    // ========================================================================

    @Test
    @Order(2400)
    void testDestroyAlreadyDestroyed() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // First destroy
        service.destroy();
        assertTrue(service.isDoDestroyCalled());
        assertEquals(DESTROYED, service.getState());

        // Second destroy should return immediately (covers lines 786-787)
        service.destroy();
        assertEquals(DESTROYED, service.getState());
    }

    @Test
    @Order(2500)
    void testDestroyWhileDestroying() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        CountDownLatch destroyingLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(1);
        AtomicBoolean secondDestroyComplete = new AtomicBoolean(false);

        // Start first destroy in a thread
        Thread destroyThread = new Thread(() -> {
            service.testSetState(DESTROYING);
            destroyingLatch.countDown();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // ignore
            }
            service.testSetState(DESTROYED);
            completeLatch.countDown();
        });

        destroyThread.start();
        destroyingLatch.await(1, TimeUnit.SECONDS);

        // Second destroy should wait (covers lines 790-799)
        Thread secondDestroyThread = new Thread(() -> {
            service.destroy();
            secondDestroyComplete.set(true);
        });
        secondDestroyThread.start();

        // Wait for both to complete
        completeLatch.await(1, TimeUnit.SECONDS);
        secondDestroyThread.join(2000);

        assertTrue(secondDestroyComplete.get());
        assertEquals(DESTROYED, service.getState());
    }

    // ========================================================================
    // process() tests - covers lines 496-562
    // ========================================================================

    @Test
    @Order(2600)
    void testProcessWhenNotAvailable() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);

        // Don't initialize - state is UNINITIALIZED
        JsonObject message = createSimpleInfoMessage("TEST", "1", 100L);

        // Should throw due to state check (covers lines 500-502)
        new SystemErr().execute(() -> {
            ServiceExecutionException exception = assertThrows(ServiceExecutionException.class,
                    () -> service.process(message));
            assertTrue(exception.getCause() instanceof IllegalStateException ||
                    exception.getMessage().contains("state"));
        });
    }

    @Test
    @Order(2650)
    void testProcessWithSimpleMessage() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        messagePartMap.put(RECORD, "PROCESS_RECORD");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L, 101L);

        // Should succeed with test scheduler
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(2700)
    void testProcessWithInterestingEntities() throws Exception {
        // Map all message parts including INTERESTING_ENTITY
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        messagePartMap.put(RECORD, "PROCESS_RECORD");
        messagePartMap.put(INTERESTING_ENTITY, "HANDLE_INTERESTING");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithInterestingEntities("TEST_DS", "REC001");

        // Should process interesting entities (covers lines 593-599, 717-743)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(2800)
    void testProcessWithNotices() throws Exception {
        // Map all message parts including NOTICE
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        messagePartMap.put(RECORD, "PROCESS_RECORD");
        messagePartMap.put(NOTICE, "HANDLE_NOTICE");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithNotices("TEST_DS", "REC001");

        // Should process notices (covers lines 603-609, 766-773)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(2900)
    void testProcessWithSingleTaskFailure() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Configure to simulate one failure (covers lines 534-540)
        service.getTestSchedulingService().setSimulateFailures(true);
        service.getTestSchedulingService().setFailureCount(1);

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L);

        new SystemErr().execute(() -> {
            ServiceExecutionException exception = assertThrows(ServiceExecutionException.class,
                    () -> service.process(message));
            assertNotNull(exception);
        });

        service.destroy();
    }

    @Test
    @Order(3000)
    void testProcessWithMultipleTaskFailures() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Configure to simulate multiple failures (covers lines 542-553)
        service.getTestSchedulingService().setSimulateFailures(true);
        service.getTestSchedulingService().setFailureCount(3);

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L, 101L, 102L);

        new SystemErr().execute(() -> {
            ServiceExecutionException exception = assertThrows(ServiceExecutionException.class,
                    () -> service.process(message));
            assertNotNull(exception);
            // The exception message should contain info about multiple failures
            // with "---" separators between each failed task
            assertTrue(exception.getMessage().contains("---"),
                    "Exception message should contain task failure separators");
        });

        service.destroy();
    }

    @Test
    @Order(3100)
    void testProcessWithRuntimeException() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Configure to throw runtime exception (covers lines 558-561)
        service.getTestSchedulingService().setThrowOnCommit(true);
        service.getTestSchedulingService().setCommitException(
                new RuntimeException("Simulated commit runtime exception"));

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L);

        new SystemErr().execute(() -> {
            ServiceExecutionException exception = assertThrows(ServiceExecutionException.class,
                    () -> service.process(message));
            assertNotNull(exception.getCause());
            assertTrue(exception.getCause() instanceof RuntimeException);
        });

        service.destroy();
    }

    // ========================================================================
    // handleRecord() tests - covers lines 648-656
    // ========================================================================

    @Test
    @Order(3200)
    void testHandleRecordWithNoAction() throws ServiceSetupException, ServiceExecutionException {
        // Create service without RECORD mapping
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST", "REC001", 100L);

        // handleRecord should do nothing when no action is mapped (covers lines 649-651)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(3300)
    void testHandleRecordWithEmptyAction() throws ServiceSetupException, ServiceExecutionException {
        // Create service with empty RECORD action
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(RECORD, "   "); // empty/whitespace action
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST", "REC001", 100L);

        // handleRecord should return early when action is empty (covers line 650)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(3400)
    void testHandleRecordWithValidAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(RECORD, "PROCESS_RECORD");
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L);

        // handleRecord should schedule task (covers lines 653-656)
        service.process(message);

        service.destroy();
    }

    // ========================================================================
    // handleAffected() tests - covers lines 680-687
    // ========================================================================

    @Test
    @Order(3500)
    void testHandleAffectedWithNoAction() throws ServiceSetupException, ServiceExecutionException {
        // Create service without AFFECTED_ENTITY mapping
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(RECORD, "RECORD_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST", "REC001", 100L);

        // handleAffected should do nothing when no action is mapped (covers lines 681-683)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(3600)
    void testHandleAffectedWithEmptyAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "   "); // empty action
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST", "REC001", 100L);

        // handleAffected should return early when action is empty (covers line 682)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(3700)
    void testHandleAffectedWithValidAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createSimpleInfoMessage("TEST_DS", "REC001", 100L, 101L);

        // handleAffected should schedule tasks (covers lines 685-686)
        service.process(message);

        service.destroy();
    }

    // ========================================================================
    // handleInteresting() tests - covers lines 717-743
    // ========================================================================

    @Test
    @Order(3800)
    void testHandleInterestingWithNoAction() throws ServiceSetupException, ServiceExecutionException {
        // Create service without INTERESTING_ENTITY mapping
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithInterestingEntities("TEST", "REC001");

        // handleInteresting should do nothing when no action is mapped (covers lines 718-720)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(3900)
    void testHandleInterestingWithEmptyAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        messagePartMap.put(INTERESTING_ENTITY, "   "); // empty action
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithInterestingEntities("TEST", "REC001");

        // handleInteresting should return early when action is empty (covers line 719)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(4000)
    void testHandleInterestingWithValidAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        messagePartMap.put(INTERESTING_ENTITY, "HANDLE_INTERESTING");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithInterestingEntities("TEST_DS", "REC001");

        // handleInteresting should schedule tasks with full parameters (covers lines 724-742)
        service.process(message);

        service.destroy();
    }

    // ========================================================================
    // handleNotice() tests - covers lines 766-773
    // ========================================================================

    @Test
    @Order(4100)
    void testHandleNoticeWithNoAction() throws ServiceSetupException, ServiceExecutionException {
        // Create service without NOTICE mapping
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithNotices("TEST", "REC001");

        // handleNotice should do nothing when no action is mapped (covers lines 767-769)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(4200)
    void testHandleNoticeWithEmptyAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        messagePartMap.put(NOTICE, "   "); // empty action
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithNotices("TEST", "REC001");

        // handleNotice should return early when action is empty (covers line 768)
        service.process(message);

        service.destroy();
    }

    @Test
    @Order(4300)
    void testHandleNoticeWithValidAction() throws ServiceSetupException, ServiceExecutionException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        messagePartMap.put(NOTICE, "HANDLE_NOTICE");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        JsonObject message = createInfoMessageWithNotices("TEST_DS", "REC001");

        // handleNotice should schedule tasks (covers lines 771-772)
        service.process(message);

        service.destroy();
    }

    // ========================================================================
    // taskAsJson() test
    // ========================================================================

    @Test
    @Order(4400)
    void testTaskAsJson() throws ServiceSetupException {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        Map<String, Object> params = new LinkedHashMap<>();
        params.put("ENTITY_ID", 12345L);
        params.put("DATA_SOURCE", "TEST");

        String json = service.taskAsJson("TEST_ACTION", params, 1);
        assertNotNull(json);
        assertTrue(json.contains("TEST_ACTION"));
        assertTrue(json.contains("ENTITY_ID"));
        assertTrue(json.contains("12345"));

        service.destroy();
    }

    // ========================================================================
    // ListenerTaskHandler tests
    // ========================================================================

    @Test
    @Order(4500)
    void testListenerTaskHandlerWaitUntilReady() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        TaskHandler handler = service.testGetTaskHandler();
        assertNotNull(handler);

        // Handler's waitUntilReady should delegate to service
        assertEquals(Boolean.TRUE, handler.waitUntilReady(0L));

        service.destroy();
    }

    @Test
    @Order(4600)
    void testListenerTaskHandlerHandleTask() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        TaskHandler handler = service.testGetTaskHandler();
        Map<String, Object> params = new HashMap<>();
        params.put("ENTITY_ID", 12345L);

        handler.handleTask("TEST_ACTION", params, 1, null);

        assertTrue(service.isHandleTaskCalled());
        assertEquals("TEST_ACTION", service.getLastAction());
        assertEquals(12345L, service.getLastParameters().get("ENTITY_ID"));
        assertEquals(1, service.getLastMultiplicity());

        service.destroy();
    }

    @Test
    @Order(4700)
    void testListenerTaskHandlerHandleTaskWithException() throws Exception {
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(AFFECTED_ENTITY, "TEST_ACTION");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);
        service.setThrowOnHandleTask(true);

        TaskHandler handler = service.testGetTaskHandler();
        Map<String, Object> params = new HashMap<>();

        new SystemErr().execute(() -> {
            assertThrows(ServiceExecutionException.class,
                    () -> handler.handleTask("TEST_ACTION", params, 1, null));
        });

        service.destroy();
    }

    // ========================================================================
    // MessagePart enum test
    // ========================================================================

    @Test
    @Order(4800)
    void testMessagePartValues() {
        MessagePart[] values = MessagePart.values();
        assertEquals(4, values.length);
        assertNotNull(MessagePart.valueOf("RECORD"));
        assertNotNull(MessagePart.valueOf("AFFECTED_ENTITY"));
        assertNotNull(MessagePart.valueOf("INTERESTING_ENTITY"));
        assertNotNull(MessagePart.valueOf("NOTICE"));
    }

    // ========================================================================
    // Process with all message parts mapped
    // ========================================================================

    @Test
    @Order(4900)
    void testProcessWithAllMessagePartsMapped() throws ServiceSetupException, ServiceExecutionException {
        // Create service with all message parts mapped
        Map<MessagePart, String> messagePartMap = new EnumMap<>(MessagePart.class);
        messagePartMap.put(RECORD, "PROCESS_RECORD");
        messagePartMap.put(AFFECTED_ENTITY, "REFRESH_ENTITY");
        messagePartMap.put(INTERESTING_ENTITY, "HANDLE_INTERESTING");
        messagePartMap.put(NOTICE, "HANDLE_NOTICE");
        TestListenerService service = new TestListenerService(messagePartMap);
        service.init(null);

        // Create a comprehensive message with all parts
        JsonObjectBuilder builder = Json.createObjectBuilder();
        builder.add("DATA_SOURCE", "FULL_TEST_DS");
        builder.add("RECORD_ID", "FULL_REC001");

        // Affected entities
        JsonArrayBuilder affectedBuilder = Json.createArrayBuilder();
        affectedBuilder.add(Json.createObjectBuilder().add("ENTITY_ID", 100L));
        affectedBuilder.add(Json.createObjectBuilder().add("ENTITY_ID", 101L));
        builder.add("AFFECTED_ENTITIES", affectedBuilder);

        // Build notices array first
        JsonArrayBuilder noticesBuilder = Json.createArrayBuilder();
        noticesBuilder.add(Json.createObjectBuilder()
                .add("CODE", "CODE1")
                .add("DESCRIPTION", "Desc1"));

        // Interesting entities with NOTICES inside (per SzInfoMessage parsing)
        JsonObjectBuilder interestingBuilder = Json.createObjectBuilder();
        JsonArrayBuilder entitiesBuilder = Json.createArrayBuilder();
        JsonObjectBuilder entity1 = Json.createObjectBuilder();
        entity1.add("ENTITY_ID", 200L);
        entity1.add("DEGREES", 1);
        entity1.add("FLAGS", Json.createArrayBuilder().add("FLAG1"));
        entity1.add("SAMPLE_RECORDS", Json.createArrayBuilder()
                .add(Json.createObjectBuilder().add("DATA_SOURCE", "DS1").add("RECORD_ID", "R1")));
        entitiesBuilder.add(entity1);
        interestingBuilder.add("ENTITIES", entitiesBuilder);
        interestingBuilder.add("NOTICES", noticesBuilder);
        builder.add("INTERESTING_ENTITIES", interestingBuilder);

        JsonObject message = builder.build();

        // Process should handle all parts
        service.process(message);

        service.destroy();
    }
}
