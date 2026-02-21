package com.senzing.listener.service.locking;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LockScope} enum.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class LockScopeTest {

    @Test
    @Order(100)
    void testProcessScopeExists() {
        assertNotNull(LockScope.PROCESS);
        assertEquals("PROCESS", LockScope.PROCESS.name());
    }

    @Test
    @Order(200)
    void testLocalhostScopeExists() {
        assertNotNull(LockScope.LOCALHOST);
        assertEquals("LOCALHOST", LockScope.LOCALHOST.name());
    }

    @Test
    @Order(300)
    void testClusterScopeExists() {
        assertNotNull(LockScope.CLUSTER);
        assertEquals("CLUSTER", LockScope.CLUSTER.name());
    }

    @Test
    @Order(400)
    void testValuesCount() {
        LockScope[] values = LockScope.values();
        assertEquals(3, values.length);
    }

    @Test
    @Order(500)
    void testValueOf() {
        assertEquals(LockScope.PROCESS, LockScope.valueOf("PROCESS"));
        assertEquals(LockScope.LOCALHOST, LockScope.valueOf("LOCALHOST"));
        assertEquals(LockScope.CLUSTER, LockScope.valueOf("CLUSTER"));
    }

    @Test
    @Order(600)
    void testValueOfInvalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> LockScope.valueOf("INVALID"));
    }

    @Test
    @Order(700)
    void testOrdinals() {
        assertEquals(0, LockScope.PROCESS.ordinal());
        assertEquals(1, LockScope.LOCALHOST.ordinal());
        assertEquals(2, LockScope.CLUSTER.ordinal());
    }
}
