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
 * Comprehensive unit tests for {@link CrossMatchKey}.
 * Tests immutability, constructors, getters, equals, hashCode, compareTo, and toString.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrossMatchKeyTest {

    /**
     * Test single-source constructor with valid parameters.
     * Documentation says this constructor uses the same source for both source1 and source2.
     */
    @Test
    @Order(100)
    void testSingleSourceConstructor() {
        String source = "TEST";
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossMatchKey key = new CrossMatchKey(source, matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(source, key.getSource1(), "Source1 should match");
        assertEquals(source, key.getSource2(), "Source2 should match source (same source)");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test single-source constructor with null source throws NullPointerException.
     * Documentation says it throws NullPointerException if source is null.
     */
    @Test
    @Order(200)
    void testSingleSourceConstructorWithNullSource() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossMatchKey(null, "NAME", "JOHN"),
            "Should throw NullPointerException for null source"
        );

        assertTrue(exception.getMessage().contains("data source"),
            "Exception message should mention data source");
    }

    /**
     * Test single-source constructor with null matchKey and principle (allowed).
     * Documentation says matchKey and principle are optional.
     */
    @Test
    @Order(300)
    void testSingleSourceConstructorWithNullOptionals() {
        String source = "TEST";

        CrossMatchKey key = new CrossMatchKey(source, null, null);

        assertNotNull(key, "Key should not be null");
        assertEquals(source, key.getSource1());
        assertEquals(source, key.getSource2());
        assertNull(key.getMatchKey(), "Match key can be null");
        assertNull(key.getPrinciple(), "Principle can be null");
    }

    /**
     * Test two-source constructor with valid parameters.
     */
    @Test
    @Order(400)
    void testTwoSourceConstructor() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossMatchKey key = new CrossMatchKey(source1, source2, matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(source1, key.getSource1(), "Source1 should match");
        assertEquals(source2, key.getSource2(), "Source2 should match");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test two-source constructor with null source1 throws NullPointerException.
     * Documentation says it throws NullPointerException if either data source is null.
     */
    @Test
    @Order(500)
    void testTwoSourceConstructorWithNullSource1() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossMatchKey(null, "CUSTOMERS", "NAME", "JOHN"),
            "Should throw NullPointerException for null source1"
        );

        assertTrue(exception.getMessage().contains("first"),
            "Exception message should mention first data source");
    }

    /**
     * Test two-source constructor with null source2 throws NullPointerException.
     * Documentation says it throws NullPointerException if either data source is null.
     */
    @Test
    @Order(600)
    void testTwoSourceConstructorWithNullSource2() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossMatchKey("TEST", null, "NAME", "JOHN"),
            "Should throw NullPointerException for null source2"
        );

        assertTrue(exception.getMessage().contains("second"),
            "Exception message should mention second data source");
    }

    /**
     * Test two-source constructor with both sources null.
     */
    @Test
    @Order(700)
    void testTwoSourceConstructorWithBothSourcesNull() {
        assertThrows(
            NullPointerException.class,
            () -> new CrossMatchKey(null, null, "NAME", "JOHN"),
            "Should throw NullPointerException when both sources are null"
        );
    }

    /**
     * Test getters return correct values.
     */
    @Test
    @Order(800)
    void testGetters() {
        CrossMatchKey key = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals("TEST", key.getSource1());
        assertEquals("CUSTOMERS", key.getSource2());
        assertEquals("NAME", key.getMatchKey());
        assertEquals("JOHN", key.getPrinciple());
    }

    /**
     * Test equals with same instance (reflexive).
     */
    @Test
    @Order(900)
    void testEqualsReflexive() {
        CrossMatchKey key = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals(key, key, "Instance should equal itself");
    }

    /**
     * Test equals with null.
     * Documentation says it returns false for null.
     */
    @Test
    @Order(1000)
    void testEqualsWithNull() {
        CrossMatchKey key = new CrossMatchKey("TEST", "NAME", "JOHN");

        assertNotEquals(key, null, "Should not equal null");
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    /**
     * Test equals with different class.
     * Documentation says it returns false for different class.
     */
    @Test
    @Order(1100)
    void testEqualsWithDifferentClass() {
        CrossMatchKey key = new CrossMatchKey("TEST", "NAME", "JOHN");

        assertNotEquals(key, "not a CrossMatchKey");
        assertNotEquals(key, Integer.valueOf(42));
    }

    /**
     * Test equals with equivalent instance (symmetric).
     */
    @Test
    @Order(1200)
    void testEqualsSymmetric() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals(key1, key2, "Equal instances should be equal");
        assertEquals(key2, key1, "Equality should be symmetric");
    }

    /**
     * Test equals with different source1.
     */
    @Test
    @Order(1300)
    void testEqualsWithDifferentSource1() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("OTHER", "CUSTOMERS", "NAME", "JOHN");

        assertNotEquals(key1, key2, "Different source1 should not be equal");
    }

    /**
     * Test equals with different source2.
     */
    @Test
    @Order(1400)
    void testEqualsWithDifferentSource2() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "OTHER", "NAME", "JOHN");

        assertNotEquals(key1, key2, "Different source2 should not be equal");
    }

    /**
     * Test equals with different match key.
     */
    @Test
    @Order(1500)
    void testEqualsWithDifferentMatchKey() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "ADDR", "JOHN");

        assertNotEquals(key1, key2, "Different match keys should not be equal");
    }

    /**
     * Test equals with different principle.
     */
    @Test
    @Order(1600)
    void testEqualsWithDifferentPrinciple() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JANE");

        assertNotEquals(key1, key2, "Different principles should not be equal");
    }

    /**
     * Test hashCode consistency with equals.
     */
    @Test
    @Order(1700)
    void testHashCodeConsistentWithEquals() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals(key1, key2, "Keys should be equal");
        assertEquals(key1.hashCode(), key2.hashCode(),
            "Equal objects must have equal hash codes");
    }

    /**
     * Test hashCode is stable.
     */
    @Test
    @Order(1800)
    void testHashCodeStable() {
        CrossMatchKey key = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2, "hashCode should be stable");
    }

    /**
     * Test compareTo with null parameter.
     * Documentation says it returns 1 when parameter is null.
     */
    @Test
    @Order(1900)
    void testCompareToWithNull() {
        CrossMatchKey key = new CrossMatchKey("TEST", "NAME", "JOHN");

        assertEquals(1, key.compareTo(null),
            "compareTo(null) should return 1");
    }

    /**
     * Test compareTo with same instance.
     */
    @Test
    @Order(2000)
    void testCompareToWithSameInstance() {
        CrossMatchKey key = new CrossMatchKey("TEST", "NAME", "JOHN");

        assertEquals(0, key.compareTo(key),
            "compareTo same instance should return 0");
    }

    /**
     * Test compareTo with equal instance.
     */
    @Test
    @Order(2100)
    void testCompareToWithEqualInstance() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2),
            "Equal instances should compare as 0");
    }

    /**
     * Test compareTo sorts by source1 first.
     * Documentation says it sorts first on associated data sources.
     */
    @Test
    @Order(2200)
    void testCompareToSortsBySource1First() {
        CrossMatchKey key1 = new CrossMatchKey("ALPHA", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("BETA", "CUSTOMERS", "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "ALPHA should sort before BETA");
        assertTrue(key2.compareTo(key1) > 0,
            "BETA should sort after ALPHA");
    }

    /**
     * Test compareTo sorts by source2 when source1 is equal.
     * Documentation says it sorts on data sources first.
     */
    @Test
    @Order(2300)
    void testCompareToSortsBySource2Second() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "ALPHA", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "BETA", "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "ALPHA should sort before BETA when source1 is equal");
        assertTrue(key2.compareTo(key1) > 0,
            "BETA should sort after ALPHA when source1 is equal");
    }

    /**
     * Test compareTo sorts by principle when sources are equal.
     * Documentation says it sorts on principles after data sources.
     */
    @Test
    @Order(2400)
    void testCompareToSortsByPrincipleThird() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "ALICE");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "BOB");

        assertTrue(key1.compareTo(key2) < 0,
            "ALICE should sort before BOB when sources and match key are equal");
    }

    /**
     * Test compareTo sorts by match key when sources and principle are equal.
     * Documentation says it sorts on match keys after principles.
     */
    @Test
    @Order(2500)
    void testCompareToSortsByMatchKeyLast() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "ADDR", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "ADDR should sort before NAME when everything else is equal");
    }

    /**
     * Test compareTo with null principle sorts before non-null.
     * Documentation says null values sort less-than non-null values.
     */
    @Test
    @Order(2600)
    void testCompareToNullPrincipleSortsFirst() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", null);
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null principle should sort before non-null");
    }

    /**
     * Test compareTo with null match key sorts before non-null.
     * Documentation says null values sort less-than non-null values.
     */
    @Test
    @Order(2700)
    void testCompareToNullMatchKeySortsFirst() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", null, "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null match key should sort before non-null");
    }

    /**
     * Test compareTo is consistent with equals.
     */
    @Test
    @Order(2800)
    void testCompareToConsistentWithEquals() {
        CrossMatchKey key1 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");
        CrossMatchKey key2 = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2), "Equal objects should compare as 0");
        assertEquals(key1, key2, "Objects that compare as 0 should be equal");
    }

    /**
     * Test sorting a collection.
     */
    @Test
    @Order(2900)
    void testSortingWithCompareTo() {
        List<CrossMatchKey> keys = new ArrayList<>();
        keys.add(new CrossMatchKey("TEST", "BETA", "NAME", "CHARLIE"));
        keys.add(new CrossMatchKey("TEST", "ALPHA", "NAME", "BOB"));
        keys.add(new CrossMatchKey("ALPHA", "BETA", "NAME", "ALICE"));
        keys.add(new CrossMatchKey("TEST", "ALPHA", null, null));

        Collections.sort(keys);

        // First should be ALPHA:BETA (source1 sorts first)
        assertEquals("ALPHA", keys.get(0).getSource1(),
            "First should have source1=ALPHA");

        // Last should be TEST:BETA
        assertEquals("TEST", keys.get(keys.size() - 1).getSource1());
        assertEquals("BETA", keys.get(keys.size() - 1).getSource2());
    }

    /**
     * Test toString format.
     * Documentation shows format as source1:source2[:principle:matchKey].
     */
    @Test
    @Order(3000)
    void testToStringFormat() {
        CrossMatchKey key = new CrossMatchKey("TEST", "CUSTOMERS", "NAME", "JOHN");

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("TEST"), "toString should contain source1");
        assertTrue(result.contains("CUSTOMERS"), "toString should contain source2");
        assertTrue(result.contains("NAME"), "toString should contain match key");
        assertTrue(result.contains("JOHN"), "toString should contain principle");
        // Format: TEST:CUSTOMERS[:JOHN:NAME]
        assertTrue(result.startsWith("TEST:CUSTOMERS["),
            "toString should start with sources");
    }

    /**
     * Test toString with null values.
     */
    @Test
    @Order(3100)
    void testToStringWithNulls() {
        CrossMatchKey key = new CrossMatchKey("TEST", "CUSTOMERS", null, null);

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("TEST"));
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("null"), "toString should show null values");
    }

    /**
     * Test immutability - values don't change after construction.
     */
    @Test
    @Order(3200)
    void testImmutability() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossMatchKey key = new CrossMatchKey(source1, source2, matchKey, principle);

        // Get values multiple times
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());

        // Again
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
    }

    /**
     * Test single-source constructor delegates to two-source constructor correctly.
     */
    @Test
    @Order(3300)
    void testSingleSourceDelegation() {
        CrossMatchKey keySingle = new CrossMatchKey("TEST", "NAME", "JOHN");
        CrossMatchKey keyDouble = new CrossMatchKey("TEST", "TEST", "NAME", "JOHN");

        assertEquals(keySingle, keyDouble,
            "Single-source and two-source constructors should produce equal keys");
        assertEquals(keySingle.hashCode(), keyDouble.hashCode(),
            "Hash codes should match");
        assertEquals(0, keySingle.compareTo(keyDouble),
            "Keys should compare as equal");
    }
}
