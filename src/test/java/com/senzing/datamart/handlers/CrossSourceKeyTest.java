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
 * Comprehensive unit tests for {@link CrossSourceKey}.
 * Tests immutability, constructors, getters, equals, hashCode, compareTo, and toString.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrossSourceKeyTest {

    /**
     * Test single-source constructor with valid parameter.
     * Documentation says this constructor uses the same source for both source1 and source2.
     */
    @Test
    @Order(100)
    void testSingleSourceConstructor() {
        String source = "TEST";

        CrossSourceKey key = new CrossSourceKey(source);

        assertNotNull(key, "Key should not be null");
        assertEquals(source, key.getSource1(), "Source1 should match");
        assertEquals(source, key.getSource2(), "Source2 should match source (same source)");
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
            () -> new CrossSourceKey((String) null),
            "Should throw NullPointerException for null source"
        );

        assertTrue(exception.getMessage().contains("data source"),
            "Exception message should mention data source");
    }

    /**
     * Test two-source constructor with valid parameters.
     */
    @Test
    @Order(300)
    void testTwoSourceConstructor() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";

        CrossSourceKey key = new CrossSourceKey(source1, source2);

        assertNotNull(key, "Key should not be null");
        assertEquals(source1, key.getSource1(), "Source1 should match");
        assertEquals(source2, key.getSource2(), "Source2 should match");
    }

    /**
     * Test two-source constructor with null source1 throws NullPointerException.
     * Documentation says both sources are required (non-null).
     */
    @Test
    @Order(400)
    void testTwoSourceConstructorWithNullSource1() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossSourceKey(null, "CUSTOMERS"),
            "Should throw NullPointerException for null source1"
        );

        assertTrue(exception.getMessage().contains("first"),
            "Exception message should mention first data source");
    }

    /**
     * Test two-source constructor with null source2 throws NullPointerException.
     * Documentation says either data source cannot be null.
     */
    @Test
    @Order(500)
    void testTwoSourceConstructorWithNullSource2() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossSourceKey("TEST", null),
            "Should throw NullPointerException for null source2"
        );

        assertTrue(exception.getMessage().contains("second"),
            "Exception message should mention second data source");
    }

    /**
     * Test two-source constructor with both sources null.
     */
    @Test
    @Order(600)
    void testTwoSourceConstructorWithBothSourcesNull() {
        assertThrows(
            NullPointerException.class,
            () -> new CrossSourceKey(null, null),
            "Should throw NullPointerException when both sources are null"
        );
    }

    /**
     * Test getters return correct values.
     */
    @Test
    @Order(700)
    void testGetters() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals("TEST", key.getSource1(),
            "getSource1 should return first source");
        assertEquals("CUSTOMERS", key.getSource2(),
            "getSource2 should return second source");
    }

    /**
     * Test equals with same instance (reflexive).
     */
    @Test
    @Order(800)
    void testEqualsReflexive() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(key, key, "Instance should equal itself");
    }

    /**
     * Test equals with null.
     * Documentation says it returns false for null.
     */
    @Test
    @Order(900)
    void testEqualsWithNull() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertNotEquals(key, null, "Should not equal null");
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    /**
     * Test equals with different class.
     * Documentation says it returns false for different class.
     */
    @Test
    @Order(1000)
    void testEqualsWithDifferentClass() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertNotEquals(key, "not a CrossSourceKey");
        assertNotEquals(key, Integer.valueOf(42));
    }

    /**
     * Test equals with equivalent instance (symmetric).
     */
    @Test
    @Order(1100)
    void testEqualsSymmetric() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(key1, key2, "Equal instances should be equal");
        assertEquals(key2, key1, "Equality should be symmetric");
    }

    /**
     * Test equals with different source1.
     */
    @Test
    @Order(1200)
    void testEqualsWithDifferentSource1() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("OTHER", "CUSTOMERS");

        assertNotEquals(key1, key2, "Different source1 should not be equal");
    }

    /**
     * Test equals with different source2.
     */
    @Test
    @Order(1300)
    void testEqualsWithDifferentSource2() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "OTHER");

        assertNotEquals(key1, key2, "Different source2 should not be equal");
    }

    /**
     * Test equals transitive property.
     */
    @Test
    @Order(1400)
    void testEqualsTransitive() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key3 = new CrossSourceKey("TEST", "CUSTOMERS");

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
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "CUSTOMERS");

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
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2, "hashCode should be stable");
    }

    /**
     * Test compareTo with null parameter.
     * Documentation says it returns 1 when parameter is null.
     */
    @Test
    @Order(1700)
    void testCompareToWithNull() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(1, key.compareTo(null),
            "compareTo(null) should return 1");
    }

    /**
     * Test compareTo with same instance.
     * Documentation says it returns 0 when comparing to itself.
     */
    @Test
    @Order(1800)
    void testCompareToWithSameInstance() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(0, key.compareTo(key),
            "compareTo same instance should return 0");
    }

    /**
     * Test compareTo with equal instance.
     */
    @Test
    @Order(1900)
    void testCompareToWithEqualInstance() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(0, key1.compareTo(key2),
            "Equal instances should compare as 0");
        assertEquals(0, key2.compareTo(key1),
            "Comparison should be symmetric");
    }

    /**
     * Test compareTo sorts by source1 first.
     * Documentation says it sorts first on data sources.
     */
    @Test
    @Order(2000)
    void testCompareToSortsBySource1First() {
        CrossSourceKey key1 = new CrossSourceKey("ALPHA", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("BETA", "CUSTOMERS");

        assertTrue(key1.compareTo(key2) < 0,
            "ALPHA should sort before BETA");
        assertTrue(key2.compareTo(key1) > 0,
            "BETA should sort after ALPHA");
    }

    /**
     * Test compareTo sorts by source2 when source1 is equal.
     * Documentation says it sorts on data sources.
     */
    @Test
    @Order(2100)
    void testCompareToSortsBySource2Second() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "ALPHA");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "BETA");

        assertTrue(key1.compareTo(key2) < 0,
            "ALPHA should sort before BETA when source1 is equal");
        assertTrue(key2.compareTo(key1) > 0,
            "BETA should sort after ALPHA when source1 is equal");
    }

    /**
     * Test compareTo is consistent with equals.
     */
    @Test
    @Order(2200)
    void testCompareToConsistentWithEquals() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "CUSTOMERS");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "CUSTOMERS");

        assertEquals(0, key1.compareTo(key2), "Equal objects should compare as 0");
        assertEquals(key1, key2, "Objects that compare as 0 should be equal");
    }

    /**
     * Test compareTo transitivity.
     */
    @Test
    @Order(2300)
    void testCompareToTransitive() {
        CrossSourceKey key1 = new CrossSourceKey("TEST", "ALPHA");
        CrossSourceKey key2 = new CrossSourceKey("TEST", "BETA");
        CrossSourceKey key3 = new CrossSourceKey("TEST", "GAMMA");

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key3) < 0);
        assertTrue(key1.compareTo(key3) < 0, "compareTo should be transitive");
    }

    /**
     * Test sorting a collection using compareTo.
     */
    @Test
    @Order(2400)
    void testSortingWithCompareTo() {
        List<CrossSourceKey> keys = new ArrayList<>();
        keys.add(new CrossSourceKey("TEST", "GAMMA"));
        keys.add(new CrossSourceKey("ALPHA", "BETA"));
        keys.add(new CrossSourceKey("TEST", "ALPHA"));
        keys.add(new CrossSourceKey("BETA", "ALPHA"));

        Collections.sort(keys);

        // First should be ALPHA:BETA (source1 sorts first)
        assertEquals("ALPHA", keys.get(0).getSource1(),
            "First should have source1=ALPHA");
        assertEquals("BETA", keys.get(0).getSource2(),
            "First should have source2=BETA");

        // Second should be BETA:ALPHA
        assertEquals("BETA", keys.get(1).getSource1());
        assertEquals("ALPHA", keys.get(1).getSource2());

        // Last two should be TEST:ALPHA and TEST:GAMMA
        assertEquals("TEST", keys.get(2).getSource1());
        assertEquals("ALPHA", keys.get(2).getSource2());
        assertEquals("TEST", keys.get(3).getSource1());
        assertEquals("GAMMA", keys.get(3).getSource2());
    }

    /**
     * Test toString format.
     * Documentation shows format as source1:source2.
     */
    @Test
    @Order(2500)
    void testToStringFormat() {
        CrossSourceKey key = new CrossSourceKey("TEST", "CUSTOMERS");

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("TEST"), "toString should contain source1");
        assertTrue(result.contains("CUSTOMERS"), "toString should contain source2");
        assertEquals("TEST:CUSTOMERS", result,
            "toString should return source1:source2");
    }

    /**
     * Test toString with different sources.
     */
    @Test
    @Order(2600)
    void testToStringWithDifferentSources() {
        CrossSourceKey key = new CrossSourceKey("ALPHA", "BETA");

        String result = key.toString();

        assertEquals("ALPHA:BETA", result);
    }

    /**
     * Test toString with same sources.
     */
    @Test
    @Order(2700)
    void testToStringWithSameSources() {
        CrossSourceKey key = new CrossSourceKey("TEST");

        String result = key.toString();

        assertEquals("TEST:TEST", result,
            "Single source constructor should produce TEST:TEST");
    }

    /**
     * Test immutability - values don't change after construction.
     */
    @Test
    @Order(2800)
    void testImmutability() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";

        CrossSourceKey key = new CrossSourceKey(source1, source2);

        // Get values multiple times
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
    }

    /**
     * Test single-source constructor delegates to two-source constructor correctly.
     * Documentation says single-source constructor uses same source for both.
     */
    @Test
    @Order(2900)
    void testSingleSourceDelegation() {
        CrossSourceKey keySingle = new CrossSourceKey("TEST");
        CrossSourceKey keyDouble = new CrossSourceKey("TEST", "TEST");

        assertEquals(keySingle, keyDouble,
            "Single-source and two-source constructors should produce equal keys");
        assertEquals(keySingle.hashCode(), keyDouble.hashCode(),
            "Hash codes should match");
        assertEquals(0, keySingle.compareTo(keyDouble),
            "Keys should compare as equal");
    }

    /**
     * Test that modifying source strings after construction doesn't affect key.
     * (Strings are immutable in Java, but verify the reference is stored correctly)
     */
    @Test
    @Order(3000)
    void testSourceStringIndependence() {
        StringBuilder source1Builder = new StringBuilder("TEST");
        StringBuilder source2Builder = new StringBuilder("CUSTOMERS");

        CrossSourceKey key = new CrossSourceKey(
            source1Builder.toString(),
            source2Builder.toString());

        // Modify the source builders (doesn't affect the strings passed to constructor)
        source1Builder.append("_MODIFIED");
        source2Builder.append("_MODIFIED");

        assertEquals("TEST", key.getSource1(),
            "Source1 should not be affected");
        assertEquals("CUSTOMERS", key.getSource2(),
            "Source2 should not be affected");
    }

    /**
     * Test documentation accuracy: compareTo javadoc mentions principles and match keys
     * but CrossSourceKey only has data sources.
     * This test documents that the javadoc is incorrect/copy-pasted.
     */
    @Test
    @Order(3100)
    void testCompareToDocumentationNote() {
        // NOTE: The compareTo javadoc says "sort first on the associated data sources,
        // then on principles and then on match keys" but CrossSourceKey doesn't have
        // principles or match keys - only data sources.
        // This is likely copied from CrossMatchKey or CrossRelationKey.

        // The actual behavior is to sort only on source1 then source2.
        CrossSourceKey key1 = new CrossSourceKey("A", "B");
        CrossSourceKey key2 = new CrossSourceKey("A", "C");

        assertTrue(key1.compareTo(key2) < 0,
            "Actually sorts only by sources, not principles/match keys");
    }
}
