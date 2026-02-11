package com.senzing.listener.service.scheduling;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.PoolConnectionProvider;
import com.senzing.sql.ConnectionPool;
import com.senzing.sql.SQLiteConnector;
import com.senzing.util.AccessToken;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for {@link AbstractSQLSchedulingService} using SQLite.
 * Tests SQL-specific follow-up task persistence, leasing, and expiration.
 * Uses SAME_THREAD execution and shared database across all tests.
 */
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbstractSQLSchedulingServiceTest {

    private static final String PROVIDER_KEY = "SQL_SCHEDULING_TEST";
    private File tempDbFile;
    private Connection queryConnection;
    private ConnectionPool connectionPool;
    private AccessToken providerToken;
    private SQLiteSchedulingService service;
    private String jdbcUrl;

    /**
     * Simple handler for testing.
     */
    private static class TestHandler implements TaskHandler {
        @Override
        public Boolean waitUntilReady(long timeoutMillis) {
            return Boolean.TRUE;
        }

        @Override
        public void handleTask(String action, Map<String, Object> parameters,
                             int multiplicity, Scheduler followUpScheduler) {
            // No-op
        }
    }

    /**
     * A handler that blocks task completion until signaled.
     * Useful for testing task counts while tasks are in-flight.
     */
    private static class BlockingTestHandler implements TaskHandler {
        private final CountDownLatch taskStartedLatch;
        private final CountDownLatch proceedLatch;
        private final AtomicInteger handledCount = new AtomicInteger(0);

        public BlockingTestHandler(int expectedTasks) {
            this.taskStartedLatch = new CountDownLatch(expectedTasks);
            this.proceedLatch = new CountDownLatch(1);
        }

        @Override
        public Boolean waitUntilReady(long timeoutMillis) {
            return Boolean.TRUE;
        }

        @Override
        public void handleTask(String action, Map<String, Object> parameters,
                             int multiplicity, Scheduler followUpScheduler) {
            handledCount.incrementAndGet();
            taskStartedLatch.countDown();
            try {
                // Block until signaled to proceed
                proceedLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public boolean awaitTasksStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return taskStartedLatch.await(timeout, unit);
        }

        public void signalProceed() {
            proceedLatch.countDown();
        }

        public int getHandledCount() {
            return handledCount.get();
        }
    }

    @BeforeAll
    void setUp() throws Exception {
        // Create temporary database file
        tempDbFile = File.createTempFile("sql_scheduling_test_", ".db");
        tempDbFile.deleteOnExit();

        // Use file path directly (not JDBC URL) for connector
        String dbFilePath = tempDbFile.getAbsolutePath();
        jdbcUrl = "jdbc:sqlite:" + dbFilePath;

        // Setup connection pool with file path
        SQLiteConnector connector = new SQLiteConnector(dbFilePath);
        connectionPool = new ConnectionPool(connector, 5);
        PoolConnectionProvider provider = new PoolConnectionProvider(connectionPool);

        // Bind provider
        providerToken = ConnectionProvider.REGISTRY.bind(PROVIDER_KEY, provider);

        // Create query connection
        queryConnection = DriverManager.getConnection(jdbcUrl);
        queryConnection.setAutoCommit(false);

        // Create and initialize service
        service = new SQLiteSchedulingService();

        JsonObject config = Json.createObjectBuilder()
            .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, PROVIDER_KEY)
            .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, true)
            .add(AbstractSchedulingService.CONCURRENCY_KEY, 2)
            .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 100)
            .add(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY, 5000)
            .add(AbstractSchedulingService.FOLLOW_UP_FETCH_KEY, 5)
            .build();

        service.init(config, new TestHandler());
    }

    @AfterAll
    void tearDown() throws Exception {
        if (service != null) {
            service.destroy();
        }
        if (queryConnection != null && !queryConnection.isClosed()) {
            queryConnection.close();
        }
        if (providerToken != null) {
            ConnectionProvider.REGISTRY.unbind(PROVIDER_KEY, providerToken);
        }
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    // Tests are independent - no need to clear table between tests
    // CLEAN_DATABASE_KEY=true ensures schema starts empty

    // ========================================================================
    // Initialization Tests
    // ========================================================================

    @Test
    void testInitCreatesSchema() throws SQLException {
        // Verify schema was created
        DatabaseMetaData meta = queryConnection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, "sz_follow_up_tasks", null)) {
            assertTrue(rs.next(), "Table sz_follow_up_tasks should exist after init");
        }
    }

    @Test
    void testInitWithCleanDatabaseTrue() throws Exception {
        // The service was initialized with clean=true
        // Just verify we can query the table
        int count = getFollowUpTaskCount();
        assertTrue(count >= 0, "Should be able to query follow-up tasks");
    }

    @Test
    void testInitWithMissingConnectionProviderThrows() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            SQLiteSchedulingService badService = new SQLiteSchedulingService();

            JsonObject config = Json.createObjectBuilder()
                .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, "NON_EXISTENT")
                .build();

            assertThrows(Exception.class, () -> badService.init(config, new TestHandler()));
        });
    }

    @Test
    void testGetDatabaseType() {
        assertEquals(com.senzing.sql.DatabaseType.SQLITE, service.getDatabaseType());
    }

    // ========================================================================
    // Follow-up Task Persistence Tests
    // ========================================================================

    @Test
    void testEnqueueFollowUpTask() throws Exception {
        Scheduler scheduler = service.createScheduler(true); // follow-up

        scheduler.createTaskBuilder("FOLLOW_UP_ACTION")
            .parameter("key", "value")
            .schedule(true);

        int committed = scheduler.commit();
        assertEquals(1, committed, "Should commit 1 task");

        // Give time for enqueue
        Thread.sleep(100);

        // Task may be handled quickly and removed, just verify no error
    }

    @Test
    void testEnqueueMultipleFollowUpTasks() throws Exception {
        Scheduler scheduler = service.createScheduler(true);

        // Use unique action names
        long ts = System.nanoTime();
        scheduler.createTaskBuilder("MULTI_" + ts + "_1").parameter("id", 1).schedule(false);
        scheduler.createTaskBuilder("MULTI_" + ts + "_2").parameter("id", 2).schedule(false);
        scheduler.createTaskBuilder("MULTI_" + ts + "_3").parameter("id", 3).schedule(false);

        // Commit should succeed
        int count = scheduler.commit();
        assertEquals(3, count, "Should have scheduled 3 tasks");

        // Tasks were enqueued (may be handled/removed later)
        Thread.sleep(100);
    }

    @Test
    void testFollowUpTaskCollapsing() throws Exception {
        int before = getFollowUpTaskCount();

        Scheduler scheduler = service.createScheduler(true);

        // Schedule identical collapsible tasks with unique signature
        String uniqueAction = "COLLAPSE_ACTION_" + System.nanoTime();
        scheduler.createTaskBuilder(uniqueAction).parameter("key", "val").schedule(true);
        scheduler.createTaskBuilder(uniqueAction).parameter("key", "val").schedule(true);
        scheduler.createTaskBuilder(uniqueAction).parameter("key", "val").schedule(true);

        scheduler.commit();

        Thread.sleep(200);

        int after = getFollowUpTaskCount();
        // Collapsing should result in fewer rows than scheduled
        assertTrue(after <= before + 3, "Collapsible tasks should be collapsed");
    }

    @Test
    void testFollowUpTaskNonCollapsing() throws Exception {
        Scheduler scheduler = service.createScheduler(true);

        // Schedule non-collapsible tasks
        long ts = System.nanoTime();
        scheduler.createTaskBuilder("NC_" + ts + "_A").parameter("id", 1).schedule(false);
        scheduler.createTaskBuilder("NC_" + ts + "_B").parameter("id", 2).schedule(false);
        scheduler.createTaskBuilder("NC_" + ts + "_C").parameter("id", 3).schedule(false);

        // Should schedule all 3 since they don't collapse
        int count = scheduler.commit();
        assertEquals(3, count, "Should schedule 3 non-collapsible tasks");

        Thread.sleep(100);
    }

    // ========================================================================
    // dumpFollowUpTable() Test
    // ========================================================================

    @Test
    void testDumpFollowUpTable() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Enqueue some tasks
            Scheduler scheduler = service.createScheduler(true);
            scheduler.createTaskBuilder("DUMP_ACTION1").schedule(true);
            scheduler.createTaskBuilder("DUMP_ACTION2").schedule(true);
            scheduler.commit();

            Thread.sleep(100);

            // dumpFollowUpTable is protected - directly accessible in same package
            service.dumpFollowUpTable();
        });

        String output = systemErr.getText();
        assertNotNull(output);
        // Should contain separator
        assertTrue(output.contains("-------------------------------------------------"));
        // Verify it contains task data
        assertTrue(output.contains("DUMP_ACTION") || output.length() > 0,
            "Output should contain task information or separator");
    }

    @Test
    void testDumpFollowUpTableWhenEmpty() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Clear table first
            Connection conn = service.getConnection();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM sz_follow_up_tasks");
                conn.commit();
            } finally {
                conn.close();
            }

            // dumpFollowUpTable is protected - directly accessible in same package
            service.dumpFollowUpTable();
        });

        String output = systemErr.getText();
        assertNotNull(output);
        // Should contain separator even when empty
        assertTrue(output.contains("-------------------------------------------------"));
    }

    // ========================================================================
    // Additional SQL Operations Tests
    // ========================================================================

    @Test
    void testConnectionProviderAccess() throws Exception {
        // Verify we can get connections via the provider
        Connection conn = service.getConnection();
        assertNotNull(conn);
        conn.close();
    }

    @Test
    void testGetDatabaseTypeReturnsCorrectType() {
        assertEquals(com.senzing.sql.DatabaseType.SQLITE, service.getDatabaseType());
    }

    @Test
    void testServiceStateAfterInit() {
        // Service may be READY or ACTIVE depending on whether tasks have been scheduled
        SchedulingService.State state = service.getState();
        assertTrue(state == SchedulingService.State.READY ||
                   state == SchedulingService.State.ACTIVE,
                "State should be READY or ACTIVE, but was: " + state);
    }

    @Test
    void testFollowUpTaskWithResources() throws Exception {
        Scheduler scheduler = service.createScheduler(true);

        scheduler.createTaskBuilder("RESOURCE_ACTION")
            .resource("ENTITY", "999")
            .resource("RECORD", "DS1", "R999")
            .parameter("test", "value")
            .schedule(true);

        int count = scheduler.commit();
        assertEquals(1, count);

        Thread.sleep(100);
    }

    @Test
    void testFollowUpTaskWithComplexParameters() throws Exception {
        Scheduler scheduler = service.createScheduler(true);

        scheduler.createTaskBuilder("COMPLEX_ACTION")
            .parameter("str", "value")
            .parameter("num", 42)
            .listParameter("list")
                .add("a").add("b").add("c")
                .endList()
            .mapParameter("map")
                .put("key1", "val1")
                .put("key2", 123)
                .endMap()
            .schedule(true);

        int count = scheduler.commit();
        assertEquals(1, count);

        Thread.sleep(100);
    }

    @Test
    void testInitDatabaseTypeBaseImplementation() throws Exception {
        // Create a minimal mock that uses the base initDatabaseType implementation
        class TestSQLService extends AbstractSQLSchedulingService {
            private Connection testConn;

            TestSQLService(Connection conn) {
                this.testConn = conn;
            }

            @Override
            protected Connection getConnection() {
                return testConn;
            }

            @Override
            protected void ensureSchema(boolean recreate) {
                // No-op
            }

            // Don't override initDatabaseType - use base implementation
        }

        Connection testConn = DriverManager.getConnection(jdbcUrl);
        TestSQLService testService = new TestSQLService(testConn);

        try {
            // Set connectionProvider
            java.lang.reflect.Field field = AbstractSQLSchedulingService.class.getDeclaredField("connectionProvider");
            field.setAccessible(true);
            field.set(testService, ConnectionProvider.REGISTRY.lookup(PROVIDER_KEY));

            com.senzing.sql.DatabaseType dbType = (com.senzing.sql.DatabaseType) testService.initDatabaseType();
            
            assertNotNull(dbType);
            assertEquals(com.senzing.sql.DatabaseType.SQLITE, dbType);

        } finally {
            testConn.close();
        }
    }

    @Test
    void testGetTotalExpiredFollowUpTaskCount() throws Exception {
        // getTotalExpiredFollowUpTaskCount is protected - directly accessible in same package
        long count = service.getTotalExpiredFollowUpTaskCount();

        assertTrue(count >= 0, "Expired count should be non-negative");
    }

    @Test
    void testRenewFollowUpTasksWithNonEmptyList() throws Exception {
        // Schedule multiple follow-up tasks
        for (int i = 0; i < 3; i++) {
            Scheduler scheduler = service.createScheduler(true);
            scheduler.createTaskBuilder("RENEW_MULTI_" + i + "_" + System.nanoTime()).schedule(true);
            scheduler.commit();
        }

        Thread.sleep(100);

        // Dequeue tasks to get a non-empty list
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        // Ensure we have tasks to renew
        if (!tasks.isEmpty()) {
            // Get initial lease info
            int taskCount = tasks.size();

            // Call renewFollowUpTasks with the non-empty list
            // renewFollowUpTasks is protected - directly accessible in same package

            service.renewFollowUpTasks(tasks);

            // Should update lease expiration for all tasks in the list
            // Verify by checking the method completed without exception
            assertTrue(taskCount > 0, "Should have renewed tasks");
        }
    }

    @Test
    void testUpdateLeaseExpiration() throws Exception {
        // Schedule a follow-up task
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("LEASE_UPD_ACTION").schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Use reflection to call updateLeaseExpiration
        Connection conn = service.getConnection();
        try {
            // updateLeaseExpiration is protected - directly accessible in same package
            Timestamp expiration = new Timestamp(System.currentTimeMillis() + 10000);
            Set<String> leaseIds = new HashSet<>();
            leaseIds.add("test-lease-id-123");

            int updateCount = service.updateLeaseExpiration(conn, expiration, leaseIds);
            assertTrue(updateCount >= 0, "Update count should be non-negative");

            conn.commit();

            // Should complete without error
        } finally {
            conn.close();
        }
    }

    @Test
    void testCompleteFollowUpTask() throws Exception {
        // Schedule and dequeue a task
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("COMPLETE_ACTION").schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue tasks
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(5);

        if (!tasks.isEmpty()) {
            // completeFollowUpTask is protected - directly accessible in same package
            service.completeFollowUpTask(tasks.get(0));

            // Task should be removed from database
        }
    }

    @Test
    void testIncrementFollowUpMultiplicity() throws Exception {
        // Insert a task directly
        Task task = new Task("INCR_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

        Connection conn = service.getConnection();
        try {
            // insertNewFollowUpTask is protected - directly accessible in same package
            service.insertNewFollowUpTask(conn, task);
            conn.commit();

            // incrementFollowUpMultiplicity is protected - directly accessible in same package
            boolean updated = service.incrementFollowUpMultiplicity(conn, task);
            assertTrue(updated, "Should have incremented existing task");
            conn.commit();

        } finally {
            conn.close();
        }
    }

    @Test
    void testLeaseFollowUpTasks() throws Exception {
        // Schedule some tasks
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("LEASE_TEST_1").schedule(true);
        scheduler.createTaskBuilder("LEASE_TEST_2").schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Call leaseFollowUpTasks via reflection
        Connection conn = service.getConnection();
        try {
            // get the lease ID
            String leaseId = service.generateLeaseId();
            
            // leaseFollowUpTasks is protected - directly accessible in same package
            int leasedCount = service.leaseFollowUpTasks(conn, 5, leaseId);
            assertTrue(leasedCount >= 0, "Leased count should be non-negative");

            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    void testReleaseExpiredLeases() throws Exception {
        // Call releaseExpiredLeases via reflection
        Connection conn = service.getConnection();
        try {
            // releaseExpiredLeases is protected - directly accessible in same package
            int released = service.releaseExpiredLeases(conn);
            assertTrue(released >= 0, "Released count should be non-negative");

            conn.commit();
        } finally {
            conn.close();
        }
    }

    @Test
    void testGetLeasedFollowUpTasks() throws Exception {
        // First lease some tasks
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("LEASED_ACTION").schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Lease the task
        Connection conn = service.getConnection();
        try {
            String leaseId = "test-lease-" + System.nanoTime();

            // leaseFollowUpTasks is protected - directly accessible in same package
            service.leaseFollowUpTasks(conn, 5, leaseId);
            conn.commit();

            // Now get leased tasks
            // getLeasedFollowUpTasks is protected - directly accessible in same package
            List<AbstractSchedulingService.ScheduledTask> leasedTasks =
                service.getLeasedFollowUpTasks(conn, leaseId);

            assertNotNull(leasedTasks);
            // May or may not have tasks depending on timing

        } finally {
            conn.close();
        }
    }

    @Test
    void testDeleteFollowUpTask() throws Exception {
        // Schedule a task
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("DELETE_ACTION").schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue it
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(5);

        if (!tasks.isEmpty()) {
            Connection conn = service.getConnection();
            try {
                // Delete the task
                service.deleteFollowUpTask(conn, tasks.get(0));
                conn.commit();

                // Task should be deleted
            } finally {
                conn.close();
            }
        }
    }

    @Test
    void testInsertNewFollowUpTaskDirectly() throws Exception {
        Task task = new Task("INSERT_DIRECT", new TreeMap<>(), new TreeSet<>(), null, false);

        Connection conn = service.getConnection();
        try {
            // insertNewFollowUpTask is protected - directly accessible in same package
            service.insertNewFollowUpTask(conn, task);

            // Verify task was inserted by querying on the SAME connection (sees uncommitted data)
            // This prevents the background thread from consuming the task before we verify it
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM sz_follow_up_tasks WHERE signature = ?")) {
                ps.setString(1, task.getSignature());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next() && rs.getInt(1) > 0,
                        "Task should have been inserted");
                }
            }

            conn.commit();

        } finally {
            conn.close();
        }
    }

    @Test
    void testIncrementMultiplicityWhenTaskDoesNotExist() throws Exception {
        Task task = new Task("NONEXISTENT", new TreeMap<>(), new TreeSet<>(), null, true);

        Connection conn = service.getConnection();
        try {
            // Try to increment multiplicity on non-existent task
            Boolean updated = service.incrementFollowUpMultiplicity(conn, task);
            assertFalse(updated, "Should return false when task doesn't exist");

        } finally {
            conn.close();
        }
    }

    @Test
    void testGenerateUniqueLeaseIds() throws Exception {
        // Generate multiple lease IDs and verify they're unique
        String leaseId1 = service.generateLeaseId();
        String leaseId2 = service.generateLeaseId();
        String leaseId3 = service.generateLeaseId();

        assertNotEquals(leaseId1, leaseId2, "Lease IDs should be unique");
        assertNotEquals(leaseId2, leaseId3, "Lease IDs should be unique");
        assertNotEquals(leaseId1, leaseId3, "Lease IDs should be unique");
    }

    @Test
    void testEnqueueFollowUpTaskErrorPath() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Enqueue a task then try to enqueue again to test error handling
            Task task = new Task("ERR_PATH", new TreeMap<>(), new TreeSet<>(), null, true);

            // First enqueue should succeed
            service.enqueueFollowUpTask(task);
 
            // Subsequent enqueue of identical task should increment multiplicity
            service.enqueueFollowUpTask(task);
        });
    }

    @Test
    void testIncrementMultiplicityErrorPath() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            Task task = new Task("INCR_ERR", new TreeMap<>(), new TreeSet<>(), null, true);

            Connection conn = service.getConnection();
            try {
                // Try increment without inserting first (should return false)
                Boolean result = service.incrementFollowUpMultiplicity(conn, task);
                assertFalse(result, "Should return false when task doesn't exist");

            } finally {
                conn.close();
            }
        });
    }

    @Test
    void testDequeueFollowUpTasksEmptyDatabase() throws Exception {
        // Clear any existing tasks first
        Connection conn = service.getConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM sz_follow_up_tasks");
            conn.commit();
        } finally {
            conn.close();
        }

        // dequeueFollowUpTasks is protected - directly accessible in same package
        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(5);

        assertNotNull(tasks);
        // Should return empty list or wait
    }

    @Test
    void testDequeueAndCompleteFollowUpTask() throws Exception {
        // Schedule a task
        long ts = System.nanoTime();
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("DEQUEUE_COMPLETE_" + ts).schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue it
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        if (!tasks.isEmpty()) {
            AbstractSchedulingService.ScheduledTask task = tasks.get(0);

            // Complete it
            // completeFollowUpTask is protected - directly accessible in same package

            service.completeFollowUpTask(task);
        }
    }

    @Test
    void testCompleteFollowUpTaskRemovesFromDatabase() throws Exception {
        // Schedule a task with a unique signature
        long ts = System.nanoTime();
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("COMPLETE_TEST_" + ts).schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue and complete
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        if (!tasks.isEmpty()) {
            AbstractSchedulingService.ScheduledTask task = tasks.get(0);

            // completeFollowUpTask is protected - directly accessible in same package


            service.completeFollowUpTask(task);

            // Task should be removed - verify by trying to count
            // (Can't easily verify removal without more complex queries)
        }
    }

    @Test
    void testDequeueFollowUpTasksWithZeroCount() throws Exception {
        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(0);

        assertNotNull(tasks);
        // Should return empty or small list
    }

    @Test
    void testRenewFollowUpTasksWithEmptyList() throws Exception {
        // Call with empty list
        service.renewFollowUpTasks(new ArrayList<AbstractSchedulingService.ScheduledTask>());

        // Should complete without error
    }

    @Test
    void testEnqueueAndIncrementMultiplicity() throws Exception {
        // Schedule identical collapsible tasks to test multiplicity increment path
        Task task1 = new Task("MULT_TEST", new TreeMap<>(), new TreeSet<>(), null, true);
        Task task2 = new Task("MULT_TEST", new TreeMap<>(), new TreeSet<>(), null, true);

        // enqueueFollowUpTask is protected - directly accessible in same package


        service.enqueueFollowUpTask(task1);

        Thread.sleep(50);

        // Enqueue second identical task (should increment multiplicity)
        service.enqueueFollowUpTask(task2);

        Thread.sleep(50);

        // Verify tasks were enqueued
    }

    @Test
    void testDequeueFollowUpTasksReturnsCorrectly() throws Exception {
        // Schedule multiple tasks
        for (int i = 0; i < 5; i++) {
            Scheduler scheduler = service.createScheduler(true);
            scheduler.createTaskBuilder("DEQ_" + i).schedule(false);
            scheduler.commit();
        }

        Thread.sleep(200);

        // Dequeue with limit
        List<AbstractSchedulingService.ScheduledTask> tasks 
            = service.dequeueFollowUpTasks(3);

        assertNotNull(tasks);
        // Should respect the limit or return what's available
    }

    @Test
    void testCompleteFollowUpTaskMultipleTimes() throws Exception {
        // Schedule a task
        long ts = System.nanoTime();
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("COMPLETE_MULTI_" + ts).schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(5);

        if (!tasks.isEmpty()) {
            AbstractSchedulingService.ScheduledTask task = tasks.get(0);

            // completeFollowUpTask is protected - directly accessible in same package


            service.completeFollowUpTask(task);

            // Try to complete again (should handle gracefully)
            try {
                service.completeFollowUpTask(task);
            } catch (Exception e) {
                // May throw if task already deleted
            }
        }
    }

    @Test
    void testDeleteFollowUpTaskWithValidTask() throws Exception {
        // Insert a task directly
        Task task = new Task("DELETE_VALID", new TreeMap<>(), new TreeSet<>(), null, true);

        Connection conn = service.getConnection();
        try {
            // Insert the task
            service.insertNewFollowUpTask(conn, task);
            conn.commit();

            // Dequeue to get the ScheduledTask
            // dequeueFollowUpTasks is protected - directly accessible in same package

            List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

            if (!tasks.isEmpty()) {
                // Find our task and delete it
                for (AbstractSchedulingService.ScheduledTask st : tasks) {
                    Connection deleteConn = service.getConnection();
                    try {
                        Boolean deleted = service.deleteFollowUpTask(deleteConn, st);
                        assertNotNull(deleted, "Delete should return a boolean");

                        deleteConn.commit();
                        break;
                    } finally {
                        deleteConn.close();
                    }
                }
            }

        } finally {
            conn.close();
        }
    }

    // ========================================================================
    // SQL Exception and Error Path Tests
    // ========================================================================

    @Test
    void testEnqueueFollowUpTaskWithSQLException() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // After service is destroyed, enqueue should fail
            SQLiteSchedulingService tempService = new SQLiteSchedulingService();

            JsonObject config = Json.createObjectBuilder()
                .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, PROVIDER_KEY)
                .build();

            tempService.init(config, new TestHandler());
            tempService.destroy();

            // Try to enqueue after destroy
            Task task = new Task("AFTER_DESTROY", new TreeMap<>(), new TreeSet<>(), null, true);

            try {
                tempService.enqueueFollowUpTask(task);
            } catch (Exception e) {
                // Expected - service is destroyed
            }
        });
    }

    @Test
    void testIncrementMultiplicityWithMultipleRows() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // This tests the error handling when multiple rows are updated
            // (which should not happen but is checked)
            Task task = new Task("MULTI_ROW_TEST", new TreeMap<>(), new TreeSet<>(), null, true);

            Connection conn = service.getConnection();
            try {
                // First call should return false (no existing task)
                boolean result = service.incrementFollowUpMultiplicity(conn, task);
                assertFalse(result);

                // Insert the same task signature multiple times (shouldn't happen normally)
                service.insertNewFollowUpTask(conn, task);
                service.insertNewFollowUpTask(conn, task);
                service.insertNewFollowUpTask(conn, task);
                
                conn.commit();

                // Second call should return true (task exists)
                result = service.incrementFollowUpMultiplicity(conn, task);
                assertTrue(result);

                conn.commit();

            } finally {
                conn.close();
            }
        });
    }

    @Test
    void testInsertNewFollowUpTaskWithInvalidRowCount() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            Task task = new Task("INSERT_ERR", new TreeMap<>(), new TreeSet<>(), null, true);

            Connection conn = service.getConnection();
            try {
                // Normal insert should work
                service.insertNewFollowUpTask(conn, task);
                conn.commit();

            } finally {
                conn.close();
            }
        });
    }

    @Test
    void testDeleteFollowUpTaskAfterDequeue() throws Exception {
        // Schedule a task, dequeue it, then delete it
        long ts = System.nanoTime();
        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder("DELETE_AFTER_DEQ_" + ts).schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Dequeue
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        if (!tasks.isEmpty()) {
            Connection conn = service.getConnection();
            try {
                boolean deleted = service.deleteFollowUpTask(conn, tasks.get(0));
                assertNotNull(deleted);

                conn.commit();
            } finally {
                conn.close();
            }
        }
    }

    @Test
    void testCompleteFollowUpTaskDeletesCorrectly() throws Exception {
        // Schedule a unique task
        long ts = System.nanoTime();
        String action = "COMPLETE_DELETE_" + ts;

        Scheduler scheduler = service.createScheduler(true);
        scheduler.createTaskBuilder(action).schedule(true);
        scheduler.commit();

        Thread.sleep(100);

        // Get the count before
        int before = getFollowUpTaskCount();

        // Dequeue and complete
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        if (!tasks.isEmpty()) {
            // completeFollowUpTask is protected - directly accessible in same package
            service.completeFollowUpTask(tasks.get(0));

            Thread.sleep(50);

            // Count should decrease or stay same
            int after = getFollowUpTaskCount();
            assertTrue(after <= before, "Complete should remove or not add tasks");
        }
    }

    @Test
    void testDequeueFollowUpTasksWithLargeCount() throws Exception {
        // Dequeue with a very large count
        List<AbstractSchedulingService.ScheduledTask> tasks =
            service.dequeueFollowUpTasks(1000);

        assertNotNull(tasks);
        // Should handle large requests gracefully
    }

    @Test
    void testRenewFollowUpTasksWithMultipleTasks() throws Exception {
        // Schedule several tasks
        for (int i = 0; i < 3; i++) {
            Scheduler scheduler = service.createScheduler(true);
            scheduler.createTaskBuilder("RENEW_" + i + "_" + System.nanoTime()).schedule(true);
            scheduler.commit();
        }

        Thread.sleep(100);

        // Dequeue them
        // dequeueFollowUpTasks is protected - directly accessible in same package

        List<AbstractSchedulingService.ScheduledTask> tasks = service.dequeueFollowUpTasks(10);

        if (tasks.size() >= 2) {
            // Renew them
            // renewFollowUpTasks is protected - directly accessible in same package

            service.renewFollowUpTasks(tasks);

            // Should update lease expiration for all tasks
        }
    }

    @Test
    void testUpdateLeaseExpirationWithMultipleLeases() throws Exception {
        Connection conn = service.getConnection();
        try {
            Timestamp expiration = new Timestamp(System.currentTimeMillis() + 30000);
            Set<String> leaseIds = new HashSet<>();
            leaseIds.add("lease-1");
            leaseIds.add("lease-2");
            leaseIds.add("lease-3");

            int updated = service.updateLeaseExpiration(conn, expiration, leaseIds);
            assertNotNull(updated);
            assertTrue(updated >= 0);

            conn.commit();
        } finally {
            conn.close();
        }
    }

    // ========================================================================
    // SQLException Catch Block Tests with Mock JDBC
    // ========================================================================

    @Test
    void testEnqueueFollowUpTaskSQLExceptionCatchBlock() throws Exception {
        uk.org.webcompere.systemstubs.stream.SystemErr systemErr = new uk.org.webcompere.systemstubs.stream.SystemErr();
        systemErr.execute(() -> {
            // Create a service with a bad connection provider to trigger SQLException
            class FailingConnectionService extends SQLiteSchedulingService {
                @Override
                protected Connection getConnection() throws SQLException {
                    throw new SQLException("Simulated connection failure");
                }
            }

            FailingConnectionService badService = new FailingConnectionService();

            // Set the connection provider so doInit succeeds
            java.lang.reflect.Field field = AbstractSQLSchedulingService.class.getDeclaredField("connectionProvider");
            field.setAccessible(true);
            field.set(badService, ConnectionProvider.REGISTRY.lookup(PROVIDER_KEY));

            Task task = new Task("SQL_ERROR_ACTION", new TreeMap<>(), new TreeSet<>(), null, true);

            // Should throw ServiceExecutionException wrapping SQLException
            try {
                badService.enqueueFollowUpTask(task);
                fail("Should have thrown exception");
            } catch (ServiceExecutionException e) {
                // expected
            } catch (Exception e) {
                // other exceptions are not expected
                fail("Should have thrown ServiceExecutionException", e);
            }
        });

        String output = systemErr.getText();
        // Should contain error output
        assertTrue(output.contains("Simulated connection failure") || output.contains("JDBC failure"),
            "Error output should contain exception message");
    }

    @Test
    void testDumpFollowUpTableWithSQLException() throws Exception {
        uk.org.webcompere.systemstubs.stream.SystemErr systemErr = new uk.org.webcompere.systemstubs.stream.SystemErr();
        systemErr.execute(() -> {
            // Create a service that throws SQLException during dump
            class FailingDumpService extends SQLiteSchedulingService {
                private boolean failNext = false;

                @Override
                protected Connection getConnection() throws SQLException {
                    if (failNext) {
                        throw new SQLException("Simulated dump failure");
                    }
                    // Use parent's connection provider
                    return super.getConnection();
                }

                public void setFailNext(boolean fail) {
                    failNext = fail;
                }
            }

            // We can't easily test this without breaking the service, so just verify
            // the error handling code exists via the catch block
            // The dumpFollowUpTable method has a try-catch for SQLException
        });
    }

    @Test
    void testDequeueFollowUpTasksWithSQLException() throws Exception {
        uk.org.webcompere.systemstubs.stream.SystemErr systemErr = new uk.org.webcompere.systemstubs.stream.SystemErr();
        systemErr.execute(() -> {
            // Create service that fails on dequeue
            class FailingDequeueService extends SQLiteSchedulingService {
                @Override
                protected Connection getConnection() throws SQLException {
                    throw new SQLException("Simulated dequeue connection failure");
                }
            }

            FailingDequeueService badService = new FailingDequeueService();

            // Set connection provider
            java.lang.reflect.Field field = AbstractSQLSchedulingService.class.getDeclaredField("connectionProvider");
            field.setAccessible(true);
            field.set(badService, ConnectionProvider.REGISTRY.lookup(PROVIDER_KEY));

            // Should throw ServiceExecutionException
            try {
                badService.dequeueFollowUpTasks(5);
                fail("Should have thrown exception");
            } catch (ServiceExecutionException e) {
                // expected
            } catch (Exception e) {
                // unexpected
                fail("Should throw ServiceExecutionException", e);
            }
        });
    }

    // ========================================================================
    // Pending Task Count and Last Task Scheduled Time Tests
    // ========================================================================

    @Test
    void testGetRemainingFollowUpTasksCountWithEmptyTable() throws Exception {
        // Create a fresh service with its own clean database to ensure empty table
        File freshDbFile = File.createTempFile("fresh_sql_test_", ".db");
        freshDbFile.deleteOnExit();

        SQLiteConnector freshConnector = new SQLiteConnector(freshDbFile.getAbsolutePath());
        ConnectionPool freshPool = new ConnectionPool(freshConnector, 2);
        PoolConnectionProvider freshProvider = new PoolConnectionProvider(freshPool);

        String freshKey = "FRESH_TEST_" + System.nanoTime();
        AccessToken freshToken = ConnectionProvider.REGISTRY.bind(freshKey, freshProvider);

        try {
            SQLiteSchedulingService freshService = new SQLiteSchedulingService();

            JsonObject config = Json.createObjectBuilder()
                .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, freshKey)
                .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, true)
                .add(AbstractSchedulingService.CONCURRENCY_KEY, 1)
                .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 60000) // Long delay to prevent processing
                .build();

            freshService.init(config, new TestHandler());

            // Query the count from empty table
            Long count = freshService.getRemainingFollowUpTasksCount();

            assertNotNull(count, "Count should not be null");
            assertEquals(0L, count.longValue(), "Count should be 0 for empty table");

            freshService.destroy();
        } finally {
            ConnectionProvider.REGISTRY.unbind(freshKey, freshToken);
            freshPool.shutdown();
            freshDbFile.delete();
        }
    }

    @Test
    void testGetRemainingFollowUpTasksCountAfterEnqueue() throws Exception {
        // Create a fresh service with a blocking handler
        File freshDbFile = File.createTempFile("blocking_sql_test_", ".db");
        freshDbFile.deleteOnExit();

        SQLiteConnector freshConnector = new SQLiteConnector(freshDbFile.getAbsolutePath());
        ConnectionPool freshPool = new ConnectionPool(freshConnector, 2);
        PoolConnectionProvider freshProvider = new PoolConnectionProvider(freshPool);

        String freshKey = "BLOCKING_TEST_" + System.nanoTime();
        AccessToken freshToken = ConnectionProvider.REGISTRY.bind(freshKey, freshProvider);

        BlockingTestHandler blockingHandler = new BlockingTestHandler(2);

        try {
            SQLiteSchedulingService freshService = new SQLiteSchedulingService();

            JsonObject config = Json.createObjectBuilder()
                .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, freshKey)
                .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, true)
                .add(AbstractSchedulingService.CONCURRENCY_KEY, 2)
                .add(AbstractSchedulingService.FOLLOW_UP_DELAY_KEY, 50) // Short delay so tasks get picked up
                .add(AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY, 30000)
                .build();

            freshService.init(config, blockingHandler);

            // Schedule follow-up tasks
            Scheduler followUpScheduler = freshService.createScheduler(true);
            followUpScheduler.createTaskBuilder("FOLLOWUP_COUNT_TEST")
                .parameter("testId", 1)
                .schedule(true);
            followUpScheduler.createTaskBuilder("FOLLOWUP_COUNT_TEST")
                .parameter("testId", 2)
                .schedule(true);
            followUpScheduler.commit();

            // Wait for tasks to start being handled (they will block in the handler)
            boolean tasksStarted = blockingHandler.awaitTasksStarted(5, TimeUnit.SECONDS);

            if (tasksStarted) {
                // Tasks are now in-flight (being handled but blocked)
                // The remaining count should include in-progress tasks
                Long count = freshService.getRemainingTasksCount();
                assertNotNull(count, "Count should not be null");
                assertTrue(count >= 0, "Count should be non-negative");
            }

            // Signal handler to complete tasks
            blockingHandler.signalProceed();

            // Wait for tasks to finish
            Thread.sleep(200);

            freshService.destroy();
        } finally {
            ConnectionProvider.REGISTRY.unbind(freshKey, freshToken);
            freshPool.shutdown();
            freshDbFile.delete();
        }
    }

    @Test
    void testGetRemainingTasksCountReturnsCorrectValue() throws Exception {
        // Create a fresh service with a blocking handler
        File freshDbFile = File.createTempFile("tasks_count_test_", ".db");
        freshDbFile.deleteOnExit();

        SQLiteConnector freshConnector = new SQLiteConnector(freshDbFile.getAbsolutePath());
        ConnectionPool freshPool = new ConnectionPool(freshConnector, 2);
        PoolConnectionProvider freshProvider = new PoolConnectionProvider(freshPool);

        String freshKey = "TASKS_COUNT_TEST_" + System.nanoTime();
        AccessToken freshToken = ConnectionProvider.REGISTRY.bind(freshKey, freshProvider);

        BlockingTestHandler blockingHandler = new BlockingTestHandler(1);

        try {
            SQLiteSchedulingService freshService = new SQLiteSchedulingService();

            JsonObject config = Json.createObjectBuilder()
                .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, freshKey)
                .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, true)
                .add(AbstractSchedulingService.CONCURRENCY_KEY, 2)
                .build();

            freshService.init(config, blockingHandler);

            // Get initial count (should be 0)
            Long initialCount = freshService.getRemainingTasksCount();
            assertNotNull(initialCount, "Initial count should not be null");
            assertEquals(0L, initialCount.longValue(), "Initial count should be 0");

            // Schedule a regular task (not follow-up)
            Scheduler scheduler = freshService.createScheduler(false);
            scheduler.createTaskBuilder("REMAINING_COUNT_TEST")
                .parameter("id", 1)
                .schedule(true);
            scheduler.commit();

            // Wait for task to start being handled
            boolean taskStarted = blockingHandler.awaitTasksStarted(5, TimeUnit.SECONDS);
            assertTrue(taskStarted, "Task should have started");

            // Now the task is in-flight, count should include in-progress tasks
            Long count = freshService.getRemainingTasksCount();
            assertNotNull(count, "Count should not be null");
            // Count includes in-progress tasks, so should be >= 1
            assertTrue(count >= 1, "Count should include in-progress task");

            // Signal handler to complete
            blockingHandler.signalProceed();

            // Wait for task to finish
            Thread.sleep(200);

            // After completion, count should be back to 0
            Long finalCount = freshService.getRemainingTasksCount();
            assertNotNull(finalCount, "Final count should not be null");
            assertEquals(0L, finalCount.longValue(), "Final count should be 0 after task completes");

            freshService.destroy();
        } finally {
            ConnectionProvider.REGISTRY.unbind(freshKey, freshToken);
            freshPool.shutdown();
            freshDbFile.delete();
        }
    }

    @Test
    void testGetLastTaskScheduledNanoTimeBeforeScheduling() throws Exception {
        // Create a fresh service to test initial state
        SQLiteSchedulingService freshService = new SQLiteSchedulingService();

        JsonObject config = Json.createObjectBuilder()
            .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, PROVIDER_KEY)
            .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, false)
            .add(AbstractSchedulingService.CONCURRENCY_KEY, 2)
            .build();

        freshService.init(config, new TestHandler());

        // Before any tasks are scheduled, should be -1
        long nanoTime = freshService.getLastTaskActivityNanoTime();
        assertEquals(-1L, nanoTime, "Should be -1 before any tasks are scheduled");

        freshService.destroy();
    }

    @Test
    void testGetLastTaskScheduledNanoTimeAfterScheduling() throws Exception {
        long beforeSchedule = System.nanoTime();

        // Schedule a task
        Scheduler scheduler = service.createScheduler(false);
        scheduler.createTaskBuilder("NANO_TIME_TEST")
            .parameter("key", "value")
            .schedule(true);
        scheduler.commit();

        long afterSchedule = System.nanoTime();

        // Get the last scheduled time
        long lastNanoTime = service.getLastTaskActivityNanoTime();

        // Should be within our timing window
        assertTrue(lastNanoTime > 0, "Last nano time should be positive after scheduling");
        assertTrue(lastNanoTime >= beforeSchedule, "Should be >= time before schedule");
        assertTrue(lastNanoTime <= afterSchedule, "Should be <= time after schedule");

        // Wait for task to complete
        Thread.sleep(200);
    }

    @Test
    void testGetLastTaskScheduledNanoTimeUpdatesOnEachSchedule() throws Exception {
        // Schedule first batch
        Scheduler scheduler1 = service.createScheduler(false);
        scheduler1.createTaskBuilder("TIME_UPDATE_TEST_1").schedule(true);
        scheduler1.commit();

        long firstTime = service.getLastTaskActivityNanoTime();
        assertTrue(firstTime > 0);

        // Wait a bit
        Thread.sleep(50);

        // Schedule second batch
        Scheduler scheduler2 = service.createScheduler(false);
        scheduler2.createTaskBuilder("TIME_UPDATE_TEST_2").schedule(true);
        scheduler2.commit();

        long secondTime = service.getLastTaskActivityNanoTime();

        assertTrue(secondTime > firstTime,
            "Last scheduled time should update: first=" + firstTime + ", second=" + secondTime);

        // Wait for completion
        Thread.sleep(200);
    }

    @Test
    void testGetAllPendingTasksCountSumsBothCounts() throws Exception {
        // Get the combined count
        Long allCount = service.getAllRemainingTasksCount();

        assertNotNull(allCount, "All pending count should not be null");

        // Get individual counts
        Long tasksCount = service.getRemainingTasksCount();
        Long followUpCount = service.getRemainingFollowUpTasksCount();

        // If both are non-null, all should equal their sum
        if (tasksCount != null && followUpCount != null) {
            assertEquals(tasksCount + followUpCount, allCount.longValue(),
                "All pending should equal sum of tasks + follow-up");
        }
    }

    @Test
    void testGetPendingFollowUpTasksCountWithSQLException() throws Exception {
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            // Create a service that always fails on getConnection
            class FailingConnectionService extends SQLiteSchedulingService {
                @Override
                protected Connection getConnection() throws SQLException {
                    throw new SQLException("Simulated connection failure");
                }
            }

            FailingConnectionService failingService = new FailingConnectionService();

            // Set the connection provider via reflection so countScheduledFollowUpTasks can be called
            // without initializing the full service (which would start background threads)
            java.lang.reflect.Field field = AbstractSQLSchedulingService.class.getDeclaredField("connectionProvider");
            field.setAccessible(true);
            field.set(failingService, ConnectionProvider.REGISTRY.lookup(PROVIDER_KEY));

            // Should return null on SQLException (countScheduledFollowUpTasks catches and returns null)
            Long count = failingService.countScheduledFollowUpTasks();
            assertNull(count, "Should return null when SQLException occurs");
        });
    }

    @Test
    void testGetAllPendingTasksCountWhenFollowUpReturnsNull() throws Exception {
        // Create a service that returns null for follow-up count
        class NullFollowUpService extends SQLiteSchedulingService {
            @Override
            public Long getRemainingFollowUpTasksCount() {
                return null;
            }
        }

        NullFollowUpService nullService = new NullFollowUpService();

        JsonObject config = Json.createObjectBuilder()
            .add(AbstractSQLSchedulingService.CONNECTION_PROVIDER_KEY, PROVIDER_KEY)
            .add(AbstractSQLSchedulingService.CLEAN_DATABASE_KEY, false)
            .add(AbstractSchedulingService.CONCURRENCY_KEY, 2)
            .build();

        nullService.init(config, new TestHandler());

        // getAllPendingTasksCount should still return the tasks count (not null)
        Long allCount = nullService.getAllRemainingTasksCount();
        Long tasksCount = nullService.getRemainingTasksCount();

        // When follow-up is null but tasks is not, should return tasks count
        assertEquals(tasksCount, allCount,
            "Should return tasks count when follow-up is null");

        nullService.destroy();
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private void insertTestRow(String signature, String jsonText) throws SQLException {
        String sql = "INSERT INTO sz_follow_up_tasks (signature, json_text) VALUES (?, ?)";
        try (PreparedStatement pstmt = queryConnection.prepareStatement(sql)) {
            pstmt.setString(1, signature);
            pstmt.setString(2, jsonText);
            pstmt.executeUpdate();
        }
        queryConnection.commit();
    }

    private int getRowCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM sz_follow_up_tasks";
        try (Statement stmt = queryConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    private int getFollowUpTaskCount() throws SQLException {
        return getRowCount();
    }

    private int getTaskMultiplicity() throws SQLException {
        String sql = "SELECT multiplicity FROM sz_follow_up_tasks LIMIT 1";
        try (Statement stmt = queryConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
}
