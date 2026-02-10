package com.senzing.datamart.handlers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Comprehensive unit tests for {@link MatchPairKey}.
 * Tests immutability, constructor, getters, equals, hashCode, compareTo, and toString.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MatchPairKeyTest {

    /**
     * Test constructor with both non-null parameters.
     */
    @Test
    @Order(100)
    void testConstructorWithValidParameters() {
        String matchKey = "NAME";
        String principle = "JOHN_DOE";

        MatchPairKey key = new MatchPairKey(matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test constructor with null match key.
     * Documentation says null values are allowed.
     */
    @Test
    @Order(200)
    void testConstructorWithNullMatchKey() {
        String principle = "JOHN_DOE";

        MatchPairKey key = new MatchPairKey(null, principle);

        assertNotNull(key, "Key should not be null");
        assertNull(key.getMatchKey(), "Match key should be null");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test constructor with null principle.
     * Documentation says null values are allowed.
     */
    @Test
    @Order(300)
    void testConstructorWithNullPrinciple() {
        String matchKey = "NAME";

        MatchPairKey key = new MatchPairKey(matchKey, null);

        assertNotNull(key, "Key should not be null");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertNull(key.getPrinciple(), "Principle should be null");
    }

    /**
     * Test constructor with both null parameters.
     * Documentation says null values are allowed.
     */
    @Test
    @Order(400)
    void testConstructorWithBothNull() {
        MatchPairKey key = new MatchPairKey(null, null);

        assertNotNull(key, "Key should not be null");
        assertNull(key.getMatchKey(), "Match key should be null");
        assertNull(key.getPrinciple(), "Principle should be null");
    }

    /**
     * Test getters return the correct values.
     */
    @Test
    @Order(500)
    void testGetters() {
        MatchPairKey key1 = new MatchPairKey("ADDR", "123_MAIN_ST");
        assertEquals("ADDR", key1.getMatchKey());
        assertEquals("123_MAIN_ST", key1.getPrinciple());

        MatchPairKey key2 = new MatchPairKey(null, "VALUE");
        assertNull(key2.getMatchKey());
        assertEquals("VALUE", key2.getPrinciple());
    }

    /**
     * Test equals with same instance (reflexive).
     */
    @Test
    @Order(600)
    void testEqualsReflexive() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        assertEquals(key, key, "Instance should equal itself");
    }

    /**
     * Test equals with null.
     * Documentation says it returns false for null.
     */
    @Test
    @Order(700)
    void testEqualsWithNull() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        assertNotEquals(key, null, "Should not equal null");
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    /**
     * Test equals with different class.
     * Documentation says it returns false for different class.
     */
    @Test
    @Order(800)
    void testEqualsWithDifferentClass() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        assertNotEquals(key, "not a MatchPairKey");
        assertNotEquals(key, Integer.valueOf(42));
    }

    /**
     * Test equals with equivalent instance (symmetric).
     */
    @Test
    @Order(900)
    void testEqualsSymmetric() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertEquals(key1, key2, "Equal instances should be equal");
        assertEquals(key2, key1, "Equality should be symmetric");
    }

    /**
     * Test equals with different match key.
     */
    @Test
    @Order(1000)
    void testEqualsWithDifferentMatchKey() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("ADDR", "JOHN");

        assertNotEquals(key1, key2, "Different match keys should not be equal");
    }

    /**
     * Test equals with different principle.
     */
    @Test
    @Order(1100)
    void testEqualsWithDifferentPrinciple() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JANE");

        assertNotEquals(key1, key2, "Different principles should not be equal");
    }

    /**
     * Test equals with null match keys.
     */
    @Test
    @Order(1200)
    void testEqualsWithNullMatchKeys() {
        MatchPairKey key1 = new MatchPairKey(null, "JOHN");
        MatchPairKey key2 = new MatchPairKey(null, "JOHN");

        assertEquals(key1, key2, "Null match keys should be equal");
    }

    /**
     * Test equals with null principles.
     */
    @Test
    @Order(1300)
    void testEqualsWithNullPrinciples() {
        MatchPairKey key1 = new MatchPairKey("NAME", null);
        MatchPairKey key2 = new MatchPairKey("NAME", null);

        assertEquals(key1, key2, "Null principles should be equal");
    }

    /**
     * Test equals transitive property.
     */
    @Test
    @Order(1400)
    void testEqualsTransitive() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key3 = new MatchPairKey("NAME", "JOHN");

        assertEquals(key1, key2);
        assertEquals(key2, key3);
        assertEquals(key1, key3, "Equality should be transitive");
    }

    /**
     * Test hashCode consistency with equals.
     * Equal objects must have equal hash codes.
     */
    @Test
    @Order(1500)
    void testHashCodeConsistentWithEquals() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertEquals(key1, key2, "Keys should be equal");
        assertEquals(key1.hashCode(), key2.hashCode(),
            "Equal objects must have equal hash codes");
    }

    /**
     * Test hashCode is stable (multiple calls return same value).
     */
    @Test
    @Order(1600)
    void testHashCodeStable() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2, "hashCode should be stable");
    }

    /**
     * Test hashCode with null values.
     */
    @Test
    @Order(1700)
    void testHashCodeWithNulls() {
        MatchPairKey key1 = new MatchPairKey(null, null);
        MatchPairKey key2 = new MatchPairKey(null, null);

        assertEquals(key1.hashCode(), key2.hashCode(),
            "Equal instances with nulls should have same hash code");
    }

    /**
     * Test compareTo with null parameter.
     * Documentation says it returns 1 when parameter is null.
     */
    @Test
    @Order(1800)
    void testCompareToWithNull() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        assertEquals(1, key.compareTo(null),
            "compareTo(null) should return 1");
    }

    /**
     * Test compareTo with same instance.
     * Documentation says it returns 0 when comparing to itself.
     */
    @Test
    @Order(1900)
    void testCompareToWithSameInstance() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        assertEquals(0, key.compareTo(key),
            "compareTo same instance should return 0");
    }

    /**
     * Test compareTo with equal instance.
     */
    @Test
    @Order(2000)
    void testCompareToWithEqualInstance() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2),
            "Equal instances should compare as 0");
        assertEquals(0, key2.compareTo(key1),
            "Comparison should be symmetric");
    }

    /**
     * Test compareTo sorts by principle first.
     * Documentation says it sorts first on principles.
     */
    @Test
    @Order(2100)
    void testCompareToSortsByPrincipleFirst() {
        MatchPairKey key1 = new MatchPairKey("NAME", "ALICE");
        MatchPairKey key2 = new MatchPairKey("NAME", "BOB");

        assertTrue(key1.compareTo(key2) < 0,
            "ALICE should sort before BOB");
        assertTrue(key2.compareTo(key1) > 0,
            "BOB should sort after ALICE");
    }

    /**
     * Test compareTo sorts by match key when principles are equal.
     * Documentation says it sorts on match keys after principles.
     */
    @Test
    @Order(2200)
    void testCompareToSortsByMatchKeySecond() {
        MatchPairKey key1 = new MatchPairKey("ADDR", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "ADDR should sort before NAME when principles are equal");
        assertTrue(key2.compareTo(key1) > 0,
            "NAME should sort after ADDR when principles are equal");
    }

    /**
     * Test compareTo with null principle sorts before non-null.
     * Documentation says null values sort less-than non-null values.
     */
    @Test
    @Order(2300)
    void testCompareToNullPrincipleSortsFirst() {
        MatchPairKey key1 = new MatchPairKey("NAME", null);
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null principle should sort before non-null");
        assertTrue(key2.compareTo(key1) > 0,
            "Non-null principle should sort after null");
    }

    /**
     * Test compareTo with null match key sorts before non-null.
     * Documentation says null values sort less-than non-null values.
     */
    @Test
    @Order(2400)
    void testCompareToNullMatchKeySortsFirst() {
        MatchPairKey key1 = new MatchPairKey(null, "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null match key should sort before non-null");
        assertTrue(key2.compareTo(key1) > 0,
            "Non-null match key should sort after null");
    }

    /**
     * Test compareTo is consistent with equals.
     */
    @Test
    @Order(2500)
    void testCompareToConsistentWithEquals() {
        MatchPairKey key1 = new MatchPairKey("NAME", "JOHN");
        MatchPairKey key2 = new MatchPairKey("NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2), "Equal objects should compare as 0");
        assertEquals(key1, key2, "Objects that compare as 0 should be equal");
    }

    /**
     * Test compareTo transitivity.
     */
    @Test
    @Order(2600)
    void testCompareToTransitive() {
        MatchPairKey key1 = new MatchPairKey("NAME", "ALICE");
        MatchPairKey key2 = new MatchPairKey("NAME", "BOB");
        MatchPairKey key3 = new MatchPairKey("NAME", "CHARLIE");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key3) < 0);
        assertTrue(key1.compareTo(key3) < 0, "compareTo should be transitive");
    }

    /**
     * Test sorting a collection using compareTo.
     */
    @Test
    @Order(2700)
    void testSortingWithCompareTo() {
        List<MatchPairKey> keys = new ArrayList<>();
        keys.add(new MatchPairKey("NAME", "CHARLIE"));
        keys.add(new MatchPairKey(null, "BOB"));
        keys.add(new MatchPairKey("NAME", "ALICE"));
        keys.add(new MatchPairKey("ADDR", null));
        keys.add(new MatchPairKey(null, null));

        Collections.sort(keys);

        // Expected order: nulls first in both principle and match key
        // 1. (null, null)
        // 2. (null, BOB) or (ADDR, null) - null principle sorts first
        // 3. Remaining sorted by principle then match key

        assertEquals(new MatchPairKey(null, null), keys.get(0),
            "Both nulls should sort first");
        assertNull(keys.get(1).getPrinciple(),
            "Second should have null principle");
    }

    /**
     * Test toString format.
     * Documentation says it returns [principle:matchKey].
     */
    @Test
    @Order(2800)
    void testToStringFormat() {
        MatchPairKey key = new MatchPairKey("NAME", "JOHN");

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("JOHN"), "toString should contain principle");
        assertTrue(result.contains("NAME"), "toString should contain match key");
        assertEquals("[JOHN:NAME]", result,
            "toString should return [principle:matchKey]");
    }

    /**
     * Test toString with null values.
     */
    @Test
    @Order(2900)
    void testToStringWithNulls() {
        MatchPairKey key1 = new MatchPairKey(null, null);
        String result1 = key1.toString();
        assertEquals("[null:null]", result1);

        MatchPairKey key2 = new MatchPairKey("NAME", null);
        String result2 = key2.toString();
        assertEquals("[null:NAME]", result2);

        MatchPairKey key3 = new MatchPairKey(null, "JOHN");
        String result3 = key3.toString();
        assertEquals("[JOHN:null]", result3);
    }

    /**
     * Test immutability - values don't change after construction.
     */
    @Test
    @Order(3000)
    void testImmutability() {
        String matchKey = "NAME";
        String principle = "JOHN";

        MatchPairKey key = new MatchPairKey(matchKey, principle);

        // Get values multiple times
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());
    }

    /**
     * Test that modifying source strings after construction doesn't affect key.
     * (Strings are immutable in Java, but verify the reference is stored correctly)
     */
    @Test
    @Order(3100)
    void testSourceStringIndependence() {
        StringBuilder matchKeyBuilder = new StringBuilder("NAME");
        StringBuilder principleBuilder = new StringBuilder("JOHN");

        MatchPairKey key = new MatchPairKey(
            matchKeyBuilder.toString(),
            principleBuilder.toString());

        // Modify the source builders (doesn't affect the strings passed to constructor)
        matchKeyBuilder.append("_MODIFIED");
        principleBuilder.append("_MODIFIED");

        assertEquals("NAME", key.getMatchKey(),
            "Match key should not be affected");
        assertEquals("JOHN", key.getPrinciple(),
            "Principle should not be affected");
    }
}
