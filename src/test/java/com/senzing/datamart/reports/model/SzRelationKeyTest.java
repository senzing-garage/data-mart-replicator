package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SzRelationKey}.
 *
 * <p>Note: SzRelationKey is an <b>immutable</b> class with no setter methods.
 * Its fields are primitive {@code long} values, so they cannot be null.
 * All values are set via the constructor and retrieved via getters.</p>
 */
class SzRelationKeyTest {

    // ========================================================================
    // Constructor and getter tests
    // ========================================================================

    @Test
    void testConstructorAndGetters() {
        SzRelationKey key = new SzRelationKey(100L, 200L);

        assertEquals(100L, key.getEntityId(), "getEntityId should return the value passed to constructor");
        assertEquals(200L, key.getRelatedId(), "getRelatedId should return the value passed to constructor");
    }

    @Test
    void testConstructorWithZeroValues() {
        SzRelationKey key = new SzRelationKey(0L, 0L);

        assertEquals(0L, key.getEntityId(), "getEntityId should return 0 when constructed with 0");
        assertEquals(0L, key.getRelatedId(), "getRelatedId should return 0 when constructed with 0");
    }

    @Test
    void testConstructorWithNegativeValues() {
        SzRelationKey key = new SzRelationKey(-100L, -200L);

        assertEquals(-100L, key.getEntityId(), "getEntityId should handle negative values");
        assertEquals(-200L, key.getRelatedId(), "getRelatedId should handle negative values");
    }

    @Test
    void testConstructorWithMaxValues() {
        SzRelationKey key = new SzRelationKey(Long.MAX_VALUE, Long.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, key.getEntityId(), "getEntityId should handle Long.MAX_VALUE");
        assertEquals(Long.MAX_VALUE, key.getRelatedId(), "getRelatedId should handle Long.MAX_VALUE");
    }

    @Test
    void testConstructorWithMinValues() {
        SzRelationKey key = new SzRelationKey(Long.MIN_VALUE, Long.MIN_VALUE);

        assertEquals(Long.MIN_VALUE, key.getEntityId(), "getEntityId should handle Long.MIN_VALUE");
        assertEquals(Long.MIN_VALUE, key.getRelatedId(), "getRelatedId should handle Long.MIN_VALUE");
    }

    @Test
    void testGetEntityIdReturnsConstructorValue() {
        // Explicit test that getEntityId returns exactly the value from the constructor
        long expectedEntityId = 12345L;
        SzRelationKey key = new SzRelationKey(expectedEntityId, 999L);
        assertEquals(expectedEntityId, key.getEntityId(),
                "getEntityId must return the exact value passed to the constructor");
    }

    @Test
    void testGetRelatedIdReturnsConstructorValue() {
        // Explicit test that getRelatedId returns exactly the value from the constructor
        long expectedRelatedId = 67890L;
        SzRelationKey key = new SzRelationKey(999L, expectedRelatedId);
        assertEquals(expectedRelatedId, key.getRelatedId(),
                "getRelatedId must return the exact value passed to the constructor");
    }

    @Test
    void testImmutability() {
        // Verify that the class is immutable - getter values don't change after construction
        SzRelationKey key = new SzRelationKey(100L, 200L);

        // Call getters multiple times - values should remain the same
        assertEquals(100L, key.getEntityId());
        assertEquals(200L, key.getRelatedId());
        assertEquals(100L, key.getEntityId());
        assertEquals(200L, key.getRelatedId());
    }

    // ========================================================================
    // equals() tests - exercise all conditional branches
    // ========================================================================

    @Test
    void testEqualsWithNullParameter() {
        // Line 61-62: obj == null should return false
        SzRelationKey key = new SzRelationKey(100L, 200L);
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    @Test
    void testEqualsWithSameReference() {
        // Line 64-65: this == obj should return true
        SzRelationKey key = new SzRelationKey(100L, 200L);
        assertTrue(key.equals(key), "Same reference should be equal");
    }

    @Test
    void testEqualsWithDifferentClass() {
        // Line 67-68: different class should return false
        SzRelationKey key = new SzRelationKey(100L, 200L);
        assertFalse(key.equals("not a key"), "Different class should not be equal");
        assertFalse(key.equals(Integer.valueOf(100)), "Different class should not be equal");
    }

    @Test
    void testEqualsWithEqualObjects() {
        // Line 70-71: same entityId and relatedId should return true
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertTrue(key1.equals(key2), "Equal keys should be equal");
        assertTrue(key2.equals(key1), "Equality should be symmetric");
    }

    @Test
    void testEqualsWithDifferentEntityId() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(101L, 200L);

        assertFalse(key1.equals(key2), "Different entityId should not be equal");
    }

    @Test
    void testEqualsWithDifferentRelatedId() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 201L);

        assertFalse(key1.equals(key2), "Different relatedId should not be equal");
    }

    @Test
    void testEqualsWithBothDifferent() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(101L, 201L);

        assertFalse(key1.equals(key2), "Both different should not be equal");
    }

    // ========================================================================
    // hashCode() tests
    // ========================================================================

    @Test
    void testHashCodeConsistencyForEqualObjects() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertEquals(key1.hashCode(), key2.hashCode(),
                "Equal objects must have the same hashCode (equals/hashCode contract)");
    }

    @Test
    void testHashCodeMultipleCalls() {
        SzRelationKey key = new SzRelationKey(100L, 200L);
        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2, "hashCode should be consistent across multiple calls");
    }

    @Test
    void testHashCodeDifferentForDifferentEntityId() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(101L, 200L);

        // Different objects should (usually) have different hashCodes
        assertNotEquals(key1.hashCode(), key2.hashCode(),
                "Objects with different entityId should have different hashCodes");
    }

    @Test
    void testHashCodeDifferentForDifferentRelatedId() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 201L);

        // Different objects should (usually) have different hashCodes
        assertNotEquals(key1.hashCode(), key2.hashCode(),
                "Objects with different relatedId should have different hashCodes");
    }

    @Test
    void testHashCodeDifferentForBothDifferent() {
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(999L, 888L);

        assertNotEquals(key1.hashCode(), key2.hashCode(),
                "Objects with completely different values should have different hashCodes");
    }

    @Test
    void testEqualsAndHashCodeContract() {
        // The contract: if two objects are equal according to equals(),
        // they must have the same hashCode
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        // First verify they are equal
        assertTrue(key1.equals(key2), "Keys should be equal");
        assertTrue(key2.equals(key1), "Equality should be symmetric");

        // Then verify hashCodes are equal
        assertEquals(key1.hashCode(), key2.hashCode(),
                "Equal objects must have equal hashCodes");
    }

    @Test
    void testNotEqualsImpliesDifferentHashCode() {
        // While not strictly required by the contract, good hashCode implementations
        // should produce different hashCodes for different objects
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 201L);

        assertFalse(key1.equals(key2), "Keys should not be equal");
        assertNotEquals(key1.hashCode(), key2.hashCode(),
                "Unequal objects should ideally have different hashCodes");
    }

    // ========================================================================
    // compareTo() tests - exercise all conditional branches
    // ========================================================================

    @Test
    void testCompareToWithNullParameter() {
        // Line 96-97: key == null should return 1 (positive)
        SzRelationKey key = new SzRelationKey(100L, 200L);
        assertTrue(key.compareTo(null) > 0, "compareTo(null) should return positive");
        assertEquals(1, key.compareTo(null), "compareTo(null) should return 1");
    }

    @Test
    void testCompareToWithEqualKeys() {
        // Line 114: equal keys should return 0
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertEquals(0, key1.compareTo(key2), "Equal keys should compare to 0");
    }

    @Test
    void testCompareToWithSmallerEntityId() {
        // Line 100-101: this.entityId < key.entityId should return -1
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(200L, 200L);

        assertTrue(key1.compareTo(key2) < 0, "Smaller entityId should sort first");
        assertEquals(-1, key1.compareTo(key2), "Should return -1 for smaller entityId");
    }

    @Test
    void testCompareToWithLargerEntityId() {
        // Line 103-104: this.entityId > key.entityId should return 1
        SzRelationKey key1 = new SzRelationKey(200L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertTrue(key1.compareTo(key2) > 0, "Larger entityId should sort last");
        assertEquals(1, key1.compareTo(key2), "Should return 1 for larger entityId");
    }

    @Test
    void testCompareToWithSameEntityIdSmallerRelatedId() {
        // Line 108-109: same entityId, this.relatedId < key.relatedId should return -1
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 300L);

        assertTrue(key1.compareTo(key2) < 0, "Smaller relatedId should sort first when entityId equal");
        assertEquals(-1, key1.compareTo(key2), "Should return -1 for smaller relatedId");
    }

    @Test
    void testCompareToWithSameEntityIdLargerRelatedId() {
        // Line 111-112: same entityId, this.relatedId > key.relatedId should return 1
        SzRelationKey key1 = new SzRelationKey(100L, 300L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertTrue(key1.compareTo(key2) > 0, "Larger relatedId should sort last when entityId equal");
        assertEquals(1, key1.compareTo(key2), "Should return 1 for larger relatedId");
    }

    @Test
    void testCompareToConsistentWithEquals() {
        // Verify that compareTo returns 0 when equals returns true
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 200L);

        assertEquals(key1, key2);
        assertEquals(0, key1.compareTo(key2), "compareTo should return 0 for equal objects");
    }

    @Test
    void testCompareToSymmetry() {
        // Verify that sgn(compareTo(a, b)) == -sgn(compareTo(b, a))
        SzRelationKey key1 = new SzRelationKey(100L, 200L);
        SzRelationKey key2 = new SzRelationKey(100L, 300L);

        int cmp1 = key1.compareTo(key2);
        int cmp2 = key2.compareTo(key1);

        assertTrue(cmp1 < 0 && cmp2 > 0, "compareTo should be antisymmetric");
    }

    // ========================================================================
    // toString() tests
    // ========================================================================

    @Test
    void testToString() {
        SzRelationKey key = new SzRelationKey(100L, 200L);
        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertEquals("100:200", result, "toString should format as entityId:relatedId");
    }

    @Test
    void testToStringWithZeroValues() {
        SzRelationKey key = new SzRelationKey(0L, 0L);
        assertEquals("0:0", key.toString());
    }

    @Test
    void testToStringWithNegativeValues() {
        SzRelationKey key = new SzRelationKey(-100L, -200L);
        assertEquals("-100:-200", key.toString());
    }

    // ========================================================================
    // Serializable test
    // ========================================================================

    @Test
    void testSerializable() {
        SzRelationKey key = new SzRelationKey(100L, 200L);
        assertTrue(key instanceof java.io.Serializable, "SzRelationKey should be Serializable");
    }
}
