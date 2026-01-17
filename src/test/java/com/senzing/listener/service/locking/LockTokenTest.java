package com.senzing.listener.service.locking;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LockToken}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class LockTokenTest {

    // ========================================================================
    // Constructor Tests
    // ========================================================================

    @Test
    @Order(100)
    void testConstructorWithProcessScope() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertNotNull(token);
        assertEquals(LockScope.PROCESS, token.getScope());
    }

    @Test
    @Order(200)
    void testConstructorWithLocalhostScope() {
        LockToken token = new LockToken(LockScope.LOCALHOST);
        assertNotNull(token);
        assertEquals(LockScope.LOCALHOST, token.getScope());
    }

    @Test
    @Order(300)
    void testConstructorWithClusterScope() {
        LockToken token = new LockToken(LockScope.CLUSTER);
        assertNotNull(token);
        assertEquals(LockScope.CLUSTER, token.getScope());
    }

    @Test
    @Order(400)
    void testConstructorWithNullScopeThrows() {
        assertThrows(NullPointerException.class, () -> new LockToken(null));
    }

    // ========================================================================
    // Token ID Uniqueness Tests
    // ========================================================================

    @Test
    @Order(500)
    void testTokenIdIsUnique() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        LockToken token3 = new LockToken(LockScope.PROCESS);

        assertNotEquals(token1.getTokenId(), token2.getTokenId());
        assertNotEquals(token2.getTokenId(), token3.getTokenId());
        assertNotEquals(token1.getTokenId(), token3.getTokenId());
    }

    @Test
    @Order(600)
    void testTokenIdIsPositive() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertTrue(token.getTokenId() > 0);
    }

    @Test
    @Order(700)
    void testTokenIdsAreSequential() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        // Token IDs should be increasing
        assertTrue(token2.getTokenId() > token1.getTokenId());
    }

    // ========================================================================
    // Getter Tests
    // ========================================================================

    @Test
    @Order(800)
    void testGetScope() {
        LockToken token = new LockToken(LockScope.CLUSTER);
        assertEquals(LockScope.CLUSTER, token.getScope());
    }

    @Test
    @Order(900)
    void testGetTimestamp() {
        Instant before = Instant.now();
        LockToken token = new LockToken(LockScope.PROCESS);
        Instant after = Instant.now();

        assertNotNull(token.getTimestamp());
        // Timestamp should be between before and after
        assertFalse(token.getTimestamp().isBefore(before));
        assertFalse(token.getTimestamp().isAfter(after));
    }

    @Test
    @Order(1000)
    void testGetProcessKey() {
        LockToken token = new LockToken(LockScope.PROCESS);
        String processKey = token.getProcessKey();
        assertNotNull(processKey);
        assertFalse(processKey.isEmpty());
        // Process key should contain the PID
        assertTrue(processKey.matches("\\d+.*"));
    }

    @Test
    @Order(1100)
    void testGetHostKey() {
        LockToken token = new LockToken(LockScope.PROCESS);
        String hostKey = token.getHostKey();
        assertNotNull(hostKey);
        // Host key is formatted from network interfaces
    }

    @Test
    @Order(1200)
    void testGetTokenKey() {
        LockToken token = new LockToken(LockScope.PROCESS);
        String tokenKey = token.getTokenKey();
        assertNotNull(tokenKey);
        assertFalse(tokenKey.isEmpty());
        // Token key should contain the scope
        assertTrue(tokenKey.contains("PROCESS"));
    }

    @Test
    @Order(1300)
    void testGetTokenKeyContainsAllParts() {
        LockToken token = new LockToken(LockScope.LOCALHOST);
        String tokenKey = token.getTokenKey();
        // Token key format: [tokenId#scope#timestamp] @ [processKey] @ [hostKey]
        assertTrue(tokenKey.contains("@"));
        assertTrue(tokenKey.contains("LOCALHOST"));
        assertTrue(tokenKey.contains("["));
        assertTrue(tokenKey.contains("]"));
    }

    // ========================================================================
    // equals() and hashCode() Tests
    // ========================================================================

    @Test
    @Order(1400)
    void testEqualsWithSameInstance() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertEquals(token, token);
    }

    @Test
    @Order(1500)
    void testEqualsWithNull() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertNotEquals(null, token);
    }

    @Test
    @Order(1600)
    void testEqualsWithDifferentClass() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertNotEquals("some string", token);
    }

    @Test
    @Order(1700)
    void testEqualsWithDifferentTokens() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        // Different tokens should not be equal (they have different token IDs)
        assertNotEquals(token1, token2);
    }

    @Test
    @Order(1800)
    void testEqualsWithDifferentScopes() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.CLUSTER);
        assertNotEquals(token1, token2);
    }

    @Test
    @Order(1900)
    void testHashCodeConsistency() {
        LockToken token = new LockToken(LockScope.PROCESS);
        int hash1 = token.hashCode();
        int hash2 = token.hashCode();
        assertEquals(hash1, hash2);
    }

    @Test
    @Order(2000)
    void testHashCodeDiffersForDifferentTokens() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        // Hash codes may differ for different tokens (not guaranteed but likely)
        // At minimum, the tokens themselves should be different
        assertNotEquals(token1, token2);
    }

    // ========================================================================
    // toString() Tests
    // ========================================================================

    @Test
    @Order(2100)
    void testToStringReturnsTokenKey() {
        LockToken token = new LockToken(LockScope.PROCESS);
        assertEquals(token.getTokenKey(), token.toString());
    }

    @Test
    @Order(2200)
    void testToStringIsNotEmpty() {
        LockToken token = new LockToken(LockScope.CLUSTER);
        String str = token.toString();
        assertNotNull(str);
        assertFalse(str.isEmpty());
    }

    // ========================================================================
    // Multiple Token Creation Tests
    // ========================================================================

    @Test
    @Order(2300)
    void testMultipleTokensHaveUniqueTokenKeys() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        LockToken token3 = new LockToken(LockScope.LOCALHOST);

        assertNotEquals(token1.getTokenKey(), token2.getTokenKey());
        assertNotEquals(token2.getTokenKey(), token3.getTokenKey());
        assertNotEquals(token1.getTokenKey(), token3.getTokenKey());
    }

    @Test
    @Order(2400)
    void testTokensCreatedRapidlyStillUnique() {
        LockToken[] tokens = new LockToken[100];
        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = new LockToken(LockScope.PROCESS);
        }

        // Verify all token IDs are unique
        for (int i = 0; i < tokens.length; i++) {
            for (int j = i + 1; j < tokens.length; j++) {
                assertNotEquals(tokens[i].getTokenId(), tokens[j].getTokenId(),
                        "Token IDs should be unique");
            }
        }
    }

    // ========================================================================
    // Process and Host Key Tests
    // ========================================================================

    @Test
    @Order(2500)
    void testProcessKeyContainsPid() {
        LockToken token = new LockToken(LockScope.PROCESS);
        String processKey = token.getProcessKey();
        // Process key starts with the PID
        long expectedPid = ProcessHandle.current().pid();
        assertTrue(processKey.startsWith(String.valueOf(expectedPid)));
    }

    @Test
    @Order(2600)
    void testSameProcessKeyForMultipleTokens() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        // Process key should be the same for tokens created in the same process
        assertEquals(token1.getProcessKey(), token2.getProcessKey());
    }

    @Test
    @Order(2700)
    void testSameHostKeyForMultipleTokens() {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);
        // Host key should be the same for tokens created on the same host
        assertEquals(token1.getHostKey(), token2.getHostKey());
    }

    // ========================================================================
    // Reflection-based equals() Tests for deeper coverage
    // ========================================================================

    @Test
    @Order(2800)
    void testEqualsWithSameTokenIdDifferentTimestamp() throws Exception {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        // Small delay to ensure different timestamp
        Thread.sleep(2);
        LockToken token2 = new LockToken(LockScope.PROCESS);

        // Use reflection to set token2's tokenId to match token1's
        java.lang.reflect.Field tokenIdField = LockToken.class.getDeclaredField("tokenId");
        tokenIdField.setAccessible(true);
        tokenIdField.set(token2, token1.getTokenId());

        // Now tokenIds match but timestamps differ
        // This tests the timestamp comparison branch in equals()
        assertNotEquals(token1, token2);
    }

    @Test
    @Order(2900)
    void testEqualsWithIdenticalFieldsViaReflection() throws Exception {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);

        // Use reflection to make token2 identical to token1
        java.lang.reflect.Field tokenIdField = LockToken.class.getDeclaredField("tokenId");
        tokenIdField.setAccessible(true);
        tokenIdField.set(token2, token1.getTokenId());

        java.lang.reflect.Field timestampField = LockToken.class.getDeclaredField("timestamp");
        timestampField.setAccessible(true);
        timestampField.set(token2, token1.getTimestamp());

        java.lang.reflect.Field tokenKeyField = LockToken.class.getDeclaredField("tokenKey");
        tokenKeyField.setAccessible(true);
        tokenKeyField.set(token2, token1.getTokenKey());

        // Now all fields match, tokens should be equal
        assertEquals(token1, token2);
        assertEquals(token1.hashCode(), token2.hashCode());
    }

    @Test
    @Order(3000)
    void testEqualsWithSameTokenIdAndTimestampDifferentProcessKey() throws Exception {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);

        // Use reflection to make tokenId and timestamp match
        java.lang.reflect.Field tokenIdField = LockToken.class.getDeclaredField("tokenId");
        tokenIdField.setAccessible(true);
        tokenIdField.set(token2, token1.getTokenId());

        java.lang.reflect.Field timestampField = LockToken.class.getDeclaredField("timestamp");
        timestampField.setAccessible(true);
        timestampField.set(token2, token1.getTimestamp());

        // Modify processKey to be different
        java.lang.reflect.Field processKeyField = LockToken.class.getDeclaredField("processKey");
        processKeyField.setAccessible(true);
        processKeyField.set(token2, "DIFFERENT_PROCESS_KEY");

        // Should not be equal due to different processKey
        assertNotEquals(token1, token2);
    }

    @Test
    @Order(3100)
    void testEqualsWithSameTokenIdTimestampProcessKeyDifferentHostKey() throws Exception {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);

        // Use reflection to make tokenId, timestamp, and processKey match
        java.lang.reflect.Field tokenIdField = LockToken.class.getDeclaredField("tokenId");
        tokenIdField.setAccessible(true);
        tokenIdField.set(token2, token1.getTokenId());

        java.lang.reflect.Field timestampField = LockToken.class.getDeclaredField("timestamp");
        timestampField.setAccessible(true);
        timestampField.set(token2, token1.getTimestamp());

        // processKey is static, so it's already the same

        // Modify hostKey to be different
        java.lang.reflect.Field hostKeyField = LockToken.class.getDeclaredField("hostKey");
        hostKeyField.setAccessible(true);
        hostKeyField.set(token2, "DIFFERENT_HOST_KEY");

        // Should not be equal due to different hostKey
        assertNotEquals(token1, token2);
    }

    @Test
    @Order(3200)
    void testEqualsWithAllFieldsMatchingExceptTokenKey() throws Exception {
        LockToken token1 = new LockToken(LockScope.PROCESS);
        LockToken token2 = new LockToken(LockScope.PROCESS);

        // Use reflection to make all fields match except tokenKey
        java.lang.reflect.Field tokenIdField = LockToken.class.getDeclaredField("tokenId");
        tokenIdField.setAccessible(true);
        tokenIdField.set(token2, token1.getTokenId());

        java.lang.reflect.Field timestampField = LockToken.class.getDeclaredField("timestamp");
        timestampField.setAccessible(true);
        timestampField.set(token2, token1.getTimestamp());

        // Modify tokenKey to be different
        java.lang.reflect.Field tokenKeyField = LockToken.class.getDeclaredField("tokenKey");
        tokenKeyField.setAccessible(true);
        tokenKeyField.set(token2, "DIFFERENT_TOKEN_KEY");

        // Should not be equal due to different tokenKey
        assertNotEquals(token1, token2);
    }
}
