package com.senzing.listener.service.locking;

import com.senzing.listener.service.exception.ServiceExecutionException;
import com.senzing.listener.service.exception.ServiceSetupException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import uk.org.webcompere.systemstubs.stream.SystemErr;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static com.senzing.listener.service.locking.LockingService.State.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ProcessScopeLockingService} and {@link AbstractLockingService}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ProcessScopeLockingServiceTest {

    // ========================================================================
    // State and Initialization Tests
    // ========================================================================

    @Test
    @Order(100)
    void testInitialState() {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        assertEquals(UNINITIALIZED, service.getState());
    }

    @Test
    @Order(200)
    void testInitTransitionsToInitialized() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);
        assertEquals(INITIALIZED, service.getState());
        service.destroy();
    }

    @Test
    @Order(300)
    void testInitWhenNotUninitializedThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // Try to init again - should throw
        assertThrows(IllegalStateException.class, () -> service.init(null));

        service.destroy();
    }

    @Test
    @Order(400)
    void testDestroyTransitionsToDestroyed() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);
        service.destroy();
        assertEquals(DESTROYED, service.getState());
    }

    @Test
    @Order(500)
    void testGetScopeReturnsProcess() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        assertEquals(LockScope.PROCESS, service.getScope());
        service.init(null);
        assertEquals(LockScope.PROCESS, service.getScope());
        service.destroy();
    }

    // ========================================================================
    // acquireLocks() Validation Tests
    // ========================================================================

    @Test
    @Order(600)
    void testAcquireLocksWithNullSetThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        assertThrows(NullPointerException.class,
                () -> service.acquireLocks(null, 0));

        service.destroy();
    }

    @Test
    @Order(700)
    void testAcquireLocksWithEmptySetThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        assertThrows(IllegalArgumentException.class,
                () -> service.acquireLocks(Set.of(), 0));

        service.destroy();
    }

    @Test
    @Order(800)
    void testAcquireLocksWithNullElementThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = new HashSet<>();
        keys.add(new ResourceKey("ENTITY", "123"));
        keys.add(null);

        assertThrows(NullPointerException.class,
                () -> service.acquireLocks(keys, 0));

        service.destroy();
    }

    @Test
    @Order(900)
    void testAcquireLocksWhenNotInitializedThrows() {
        ProcessScopeLockingService service = new ProcessScopeLockingService();

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "123"));

        assertThrows(IllegalStateException.class,
                () -> service.acquireLocks(keys, 0));
    }

    @Test
    @Order(1000)
    void testAcquireLocksWhenDestroyedThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);
        service.destroy();

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "123"));

        assertThrows(IllegalStateException.class,
                () -> service.acquireLocks(keys, 0));
    }

    // ========================================================================
    // releaseLocks() Validation Tests
    // ========================================================================

    @Test
    @Order(1100)
    void testReleaseLocksWithNullTokenThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        assertThrows(NullPointerException.class,
                () -> service.releaseLocks(null));

        service.destroy();
    }

    @Test
    @Order(1200)
    void testReleaseLocksWhenNotInitializedThrows() {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        LockToken token = new LockToken(LockScope.PROCESS);

        assertThrows(IllegalStateException.class,
                () -> service.releaseLocks(token));
    }

    @Test
    @Order(1300)
    void testReleaseLocksWithUnrecognizedTokenThrows() throws ServiceSetupException {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        LockToken unknownToken = new LockToken(LockScope.PROCESS);

        assertThrows(IllegalArgumentException.class,
                () -> service.releaseLocks(unknownToken));

        service.destroy();
    }

    // ========================================================================
    // Basic Lock/Unlock Tests
    // ========================================================================

    @Test
    @Order(1400)
    void testAcquireAndReleaseSingleLock() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));

        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);
        assertEquals(LockScope.PROCESS, token.getScope());

        int released = service.releaseLocks(token);
        assertEquals(1, released);

        service.destroy();
    }

    @Test
    @Order(1500)
    void testAcquireAndReleaseMultipleLocks() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(
                new ResourceKey("ENTITY", "100"),
                new ResourceKey("ENTITY", "101"),
                new ResourceKey("RECORD", "DS1", "R001")
        );

        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);

        int released = service.releaseLocks(token);
        assertEquals(3, released);

        service.destroy();
    }

    @Test
    @Order(1600)
    void testAcquireLocksReturnsNullWhenResourceLocked() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));

        // First lock should succeed
        LockToken token1 = service.acquireLocks(keys, 0);
        assertNotNull(token1);

        // Second lock with wait=0 should fail immediately
        LockToken token2 = service.acquireLocks(keys, 0);
        assertNull(token2);

        // Release the first lock
        service.releaseLocks(token1);

        // Now locking should succeed
        LockToken token3 = service.acquireLocks(keys, 0);
        assertNotNull(token3);

        service.releaseLocks(token3);
        service.destroy();
    }

    @Test
    @Order(1700)
    void testAcquireLocksWithWaitTimeout() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));

        // First lock should succeed
        LockToken token1 = service.acquireLocks(keys, 0);
        assertNotNull(token1);

        // Second lock with short wait should timeout
        long start = System.currentTimeMillis();
        LockToken token2 = service.acquireLocks(keys, 200);
        long elapsed = System.currentTimeMillis() - start;

        assertNull(token2);
        assertTrue(elapsed >= 150, "Should have waited approximately 200ms, elapsed: " + elapsed);

        service.releaseLocks(token1);
        service.destroy();
    }

    @Test
    @Order(1800)
    void testAcquireLocksWithNonOverlappingKeys() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys1 = Set.of(new ResourceKey("ENTITY", "100"));
        Set<ResourceKey> keys2 = Set.of(new ResourceKey("ENTITY", "200"));

        // Both locks should succeed since they don't overlap
        LockToken token1 = service.acquireLocks(keys1, 0);
        LockToken token2 = service.acquireLocks(keys2, 0);

        assertNotNull(token1);
        assertNotNull(token2);

        service.releaseLocks(token1);
        service.releaseLocks(token2);
        service.destroy();
    }

    @Test
    @Order(1900)
    void testAcquireLocksWithPartialOverlap() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys1 = Set.of(
                new ResourceKey("ENTITY", "100"),
                new ResourceKey("ENTITY", "101")
        );
        Set<ResourceKey> keys2 = Set.of(
                new ResourceKey("ENTITY", "101"), // overlaps!
                new ResourceKey("ENTITY", "102")
        );

        // First lock should succeed
        LockToken token1 = service.acquireLocks(keys1, 0);
        assertNotNull(token1);

        // Second lock should fail due to overlap on ENTITY:101
        LockToken token2 = service.acquireLocks(keys2, 0);
        assertNull(token2);

        service.releaseLocks(token1);

        // Now second lock should succeed
        LockToken token3 = service.acquireLocks(keys2, 0);
        assertNotNull(token3);

        service.releaseLocks(token3);
        service.destroy();
    }

    // ========================================================================
    // releaseLocks() During DESTROYING State Test
    // ========================================================================

    @Test
    @Order(2000)
    void testReleaseLocksAllowedDuringDestroying() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));
        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);

        // Start destroy in background thread (it will wait for locks to be released)
        CountDownLatch destroyStarted = new CountDownLatch(1);
        CountDownLatch destroyComplete = new CountDownLatch(1);
        Thread destroyThread = new Thread(() -> {
            destroyStarted.countDown();
            service.destroy();
            destroyComplete.countDown();
        });
        destroyThread.start();

        // Wait for destroy to start
        destroyStarted.await(5, TimeUnit.SECONDS);
        Thread.sleep(100); // Give destroy thread time to set state to DESTROYING

        // Release locks while in DESTROYING state should work
        int released = service.releaseLocks(token);
        assertEquals(1, released);

        // Destroy should now complete
        assertTrue(destroyComplete.await(5, TimeUnit.SECONDS));
        destroyThread.join(5000);
    }

    // ========================================================================
    // Concurrent Access Tests
    // ========================================================================

    @Test
    @Order(2100)
    void testConcurrentLockAcquisitionSameResource() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        List<Future<LockToken>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                startLatch.await(); // Wait for all threads to start together
                LockToken token = service.acquireLocks(keys, 100); // Short wait
                if (token != null) {
                    successCount.incrementAndGet();
                    Thread.sleep(50); // Hold lock briefly
                    service.releaseLocks(token);
                } else {
                    failCount.incrementAndGet();
                }
                return token;
            }));
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all futures to complete
        for (Future<LockToken> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // At least one thread should have acquired the lock
        assertTrue(successCount.get() >= 1);
        // Total should equal number of threads
        assertEquals(numThreads, successCount.get() + failCount.get());

        service.destroy();
    }

    @Test
    @Order(2200)
    void testConcurrentLockAcquisitionBlocksUntilReleased() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));
        AtomicBoolean thread2Started = new AtomicBoolean(false);
        AtomicBoolean thread2GotLock = new AtomicBoolean(false);
        CountDownLatch thread2LockAcquired = new CountDownLatch(1);

        // Thread 1: Acquire lock and hold it
        LockToken token1 = service.acquireLocks(keys, 0);
        assertNotNull(token1);

        // Thread 2: Try to acquire lock with indefinite wait
        Thread thread2 = new Thread(() -> {
            thread2Started.set(true);
            try {
                LockToken token2 = service.acquireLocks(keys, -1); // Indefinite wait
                if (token2 != null) {
                    thread2GotLock.set(true);
                    thread2LockAcquired.countDown();
                    service.releaseLocks(token2);
                }
            } catch (Exception e) {
                // Interrupted
            }
        });
        thread2.start();

        // Wait for thread 2 to start and begin waiting
        Thread.sleep(200);
        assertTrue(thread2Started.get());
        assertFalse(thread2GotLock.get()); // Should still be waiting

        // Release lock in thread 1
        service.releaseLocks(token1);

        // Thread 2 should now acquire the lock
        assertTrue(thread2LockAcquired.await(5, TimeUnit.SECONDS));
        assertTrue(thread2GotLock.get());

        thread2.join(5000);
        service.destroy();
    }

    @Test
    @Order(2300)
    void testDeadlockPreventionWithOverlappingResources() throws Exception {
        // Test that sorting resource keys prevents deadlocks
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // Create two sets with overlapping resources in different order
        ResourceKey keyA = new ResourceKey("ENTITY", "AAA");
        ResourceKey keyB = new ResourceKey("ENTITY", "BBB");
        ResourceKey keyC = new ResourceKey("ENTITY", "CCC");

        // Thread 1 wants A, B
        // Thread 2 wants B, C
        // Without sorting, this could deadlock

        Set<ResourceKey> set1 = Set.of(keyA, keyB);
        Set<ResourceKey> set2 = Set.of(keyB, keyC);

        AtomicBoolean thread1Success = new AtomicBoolean(false);
        AtomicBoolean thread2Success = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);

        Thread t1 = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    LockToken token = service.acquireLocks(set1, 1000);
                    if (token != null) {
                        Thread.sleep(10);
                        service.releaseLocks(token);
                        thread1Success.set(true);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 10; i++) {
                    LockToken token = service.acquireLocks(set2, 1000);
                    if (token != null) {
                        Thread.sleep(10);
                        service.releaseLocks(token);
                        thread2Success.set(true);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });

        t1.start();
        t2.start();
        startLatch.countDown();

        // Should complete without deadlock
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "Threads should complete without deadlock");

        // Both threads should have succeeded at least once
        assertTrue(thread1Success.get(), "Thread 1 should have acquired locks at least once");
        assertTrue(thread2Success.get(), "Thread 2 should have acquired locks at least once");

        t1.join(5000);
        t2.join(5000);
        service.destroy();
    }

    @Test
    @Order(2400)
    void testManyThreadsManyResources() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        int numThreads = 10;
        int numIterations = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger totalLockCount = new AtomicInteger(0);

        List<ResourceKey> allKeys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            allKeys.add(new ResourceKey("RESOURCE", String.valueOf(i)));
        }

        Random random = new Random(42); // Fixed seed for reproducibility

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random localRandom = new Random(42 + threadId);
                    for (int i = 0; i < numIterations; i++) {
                        // Pick 1-3 random resources
                        int numResources = localRandom.nextInt(3) + 1;
                        Set<ResourceKey> keys = new HashSet<>();
                        for (int r = 0; r < numResources; r++) {
                            keys.add(allKeys.get(localRandom.nextInt(allKeys.size())));
                        }

                        LockToken token = service.acquireLocks(keys, 500);
                        if (token != null) {
                            totalLockCount.incrementAndGet();
                            Thread.sleep(5);
                            service.releaseLocks(token);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete without deadlock");

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // At least some locks should have been acquired
        assertTrue(totalLockCount.get() > 0, "Some locks should have been acquired");

        service.destroy();
    }

    // ========================================================================
    // Interrupt Handling Test
    // ========================================================================

    @Test
    @Order(2500)
    void testAcquireLocksInterruptedReturnsNull() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));

        // Acquire lock first
        LockToken token1 = service.acquireLocks(keys, 0);
        assertNotNull(token1);

        AtomicReference<LockToken> result = new AtomicReference<>();
        Thread waitingThread = new Thread(() -> {
            try {
                // This will wait indefinitely, but we'll interrupt it
                result.set(service.acquireLocks(keys, -1));
            } catch (Exception e) {
                // Expected
            }
        });

        waitingThread.start();
        Thread.sleep(100); // Let thread start waiting

        // Interrupt the waiting thread
        waitingThread.interrupt();
        waitingThread.join(5000);

        // Result should be null due to interruption
        assertNull(result.get());

        service.releaseLocks(token1);
        service.destroy();
    }

    // ========================================================================
    // Destroy Waits for Locks Test
    // ========================================================================

    @Test
    @Order(2600)
    void testDestroyWaitsForLocksToBeReleased() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));
        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);

        AtomicBoolean destroyStarted = new AtomicBoolean(false);
        AtomicBoolean destroyCompleted = new AtomicBoolean(false);

        Thread destroyThread = new Thread(() -> {
            destroyStarted.set(true);
            service.destroy();
            destroyCompleted.set(true);
        });

        destroyThread.start();
        Thread.sleep(200);

        assertTrue(destroyStarted.get());
        assertFalse(destroyCompleted.get()); // Should still be waiting

        // Release the lock
        service.releaseLocks(token);

        // Destroy should complete
        destroyThread.join(5000);
        assertTrue(destroyCompleted.get());
        assertEquals(DESTROYED, service.getState());
    }

    // ========================================================================
    // dumpLocks() Test with SystemStubs
    // ========================================================================

    @Test
    @Order(2700)
    void testDumpLocksOutputsLockState() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // Acquire some locks
        Set<ResourceKey> keys1 = Set.of(
                new ResourceKey("ENTITY", "100"),
                new ResourceKey("ENTITY", "101")
        );
        Set<ResourceKey> keys2 = Set.of(
                new ResourceKey("RECORD", "DS1", "R001")
        );

        LockToken token1 = service.acquireLocks(keys1, 0);
        LockToken token2 = service.acquireLocks(keys2, 0);

        assertNotNull(token1);
        assertNotNull(token2);

        // Capture stderr output from dumpLocks()
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            service.dumpLocks();
        });
        String output = systemErr.getText();

        // Verify expected content in output
        assertNotNull(output);
        assertTrue(output.contains("***"), "Output should contain separator stars");
        assertTrue(output.contains("---") || output.contains("--"), "Output should contain dashes");
        assertTrue(output.contains("ENTITY"), "Output should contain ENTITY resource type");
        assertTrue(output.contains("RECORD"), "Output should contain RECORD resource type");
        assertTrue(output.contains("100") || output.contains("101"), "Output should contain entity IDs");
        assertTrue(output.contains("DS1") || output.contains("R001"), "Output should contain record info");

        service.releaseLocks(token1);
        service.releaseLocks(token2);
        service.destroy();
    }

    @Test
    @Order(2800)
    void testDumpLocksWithNoLocks() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // No locks acquired - dumpLocks should still work
        SystemErr systemErr = new SystemErr();
        systemErr.execute(() -> {
            service.dumpLocks();
        });
        String output = systemErr.getText();

        assertNotNull(output);
        assertTrue(output.contains("***"), "Output should contain separator stars");

        service.destroy();
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    @Test
    @Order(2900)
    void testReacquireSameResourceAfterRelease() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));

        // Acquire, release, reacquire multiple times
        for (int i = 0; i < 5; i++) {
            LockToken token = service.acquireLocks(keys, 0);
            assertNotNull(token, "Iteration " + i + " should acquire lock");
            service.releaseLocks(token);
        }

        service.destroy();
    }

    @Test
    @Order(3000)
    void testAcquireLocksSortsResourceKeys() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // Create keys in reverse order
        Set<ResourceKey> keys = new LinkedHashSet<>();
        keys.add(new ResourceKey("ZZZ", "3"));
        keys.add(new ResourceKey("AAA", "1"));
        keys.add(new ResourceKey("MMM", "2"));

        // Acquiring locks should work regardless of order (due to sorting)
        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);

        int released = service.releaseLocks(token);
        assertEquals(3, released);

        service.destroy();
    }

    @Test
    @Order(3100)
    void testAcquireLocksWithDuplicateKeysInSet() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        // Sets don't allow duplicates, but create equivalent keys
        ResourceKey key1 = new ResourceKey("ENTITY", "100");
        ResourceKey key2 = new ResourceKey("ENTITY", "100"); // Same as key1

        Set<ResourceKey> keys = new HashSet<>();
        keys.add(key1);
        keys.add(key2); // Won't actually add since it's a duplicate

        LockToken token = service.acquireLocks(keys, 0);
        assertNotNull(token);

        int released = service.releaseLocks(token);
        assertEquals(1, released); // Only one unique key

        service.destroy();
    }

    @Test
    @Order(3200)
    void testReleaseLocksCannotReleaseWithDifferentToken() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();
        service.init(null);

        Set<ResourceKey> keys = Set.of(new ResourceKey("ENTITY", "100"));
        LockToken realToken = service.acquireLocks(keys, 0);
        assertNotNull(realToken);

        // Create a different token
        LockToken fakeToken = new LockToken(LockScope.PROCESS);

        // Trying to release with wrong token should throw
        assertThrows(IllegalArgumentException.class,
                () -> service.releaseLocks(fakeToken));

        // Release with real token should work
        service.releaseLocks(realToken);
        service.destroy();
    }

    // ========================================================================
    // State Notification Tests
    // ========================================================================

    @Test
    @Order(3300)
    void testSetStateNotifiesWaitingThreads() throws Exception {
        ProcessScopeLockingService service = new ProcessScopeLockingService();

        CountDownLatch waitingLatch = new CountDownLatch(1);
        CountDownLatch notifiedLatch = new CountDownLatch(1);

        Thread waitingThread = new Thread(() -> {
            synchronized (service) {
                waitingLatch.countDown();
                while (service.getState() == UNINITIALIZED) {
                    try {
                        service.wait(5000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                notifiedLatch.countDown();
            }
        });

        waitingThread.start();
        waitingLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(100); // Ensure thread is waiting

        // Initialize - this should notify waiting threads
        service.init(null);

        assertTrue(notifiedLatch.await(5, TimeUnit.SECONDS),
                "Waiting thread should be notified of state change");

        waitingThread.join(5000);
        service.destroy();
    }
}
