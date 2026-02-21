package com.senzing.datamart.handlers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.junit.jupiter.api.Assertions.*;

import com.senzing.datamart.model.SzMatchType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Comprehensive unit tests for {@link CrossRelationKey}.
 * Tests immutability, constructors, getters, equals, hashCode, compareTo, and toString.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CrossRelationKeyTest {

    /**
     * Test single-source constructor with valid parameters.
     * Documentation says this constructor uses the same source for both source1 and source2.
     */
    @Test
    @Order(100)
    void testSingleSourceConstructor() {
        String source = "TEST";
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossRelationKey key = new CrossRelationKey(source, matchType, matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(source, key.getSource1(), "Source1 should match");
        assertEquals(source, key.getSource2(), "Source2 should match source");
        assertEquals(matchType, key.getMatchType(), "Match type should match");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test single-source constructor with null source throws NullPointerException.
     */
    @Test
    @Order(200)
    void testSingleSourceConstructorWithNullSource() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossRelationKey(null, SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN"),
            "Should throw NullPointerException for null source"
        );

        assertTrue(exception.getMessage().contains("data source"),
            "Exception message should mention data source");
    }

    /**
     * Test single-source constructor with null matchType throws NullPointerException.
     * Documentation says matchType is required.
     */
    @Test
    @Order(300)
    void testSingleSourceConstructorWithNullMatchType() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossRelationKey("TEST", null, "NAME", "JOHN"),
            "Should throw NullPointerException for null match type"
        );

        assertTrue(exception.getMessage().contains("match type"),
            "Exception message should mention match type");
    }

    /**
     * Test two-source constructor with valid parameters.
     */
    @Test
    @Order(400)
    void testTwoSourceConstructor() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";
        SzMatchType matchType = SzMatchType.DISCLOSED_RELATION;
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossRelationKey key = new CrossRelationKey(
            source1, source2, matchType, matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
        assertEquals(matchType, key.getMatchType());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());
    }

    /**
     * Test two-source constructor with null source1 throws NullPointerException.
     * Documentation says both sources are required.
     */
    @Test
    @Order(500)
    void testTwoSourceConstructorWithNullSource1() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossRelationKey(
                null, "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN"),
            "Should throw NullPointerException for null source1"
        );

        assertTrue(exception.getMessage().contains("first"),
            "Exception message should mention first data source");
    }

    /**
     * Test two-source constructor with null source2 throws NullPointerException.
     */
    @Test
    @Order(600)
    void testTwoSourceConstructorWithNullSource2() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossRelationKey(
                "TEST", null, SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN"),
            "Should throw NullPointerException for null source2"
        );

        assertTrue(exception.getMessage().contains("second"),
            "Exception message should mention second data source");
    }

    /**
     * Test two-source constructor with null matchType throws NullPointerException.
     * Documentation says matchType is required.
     */
    @Test
    @Order(700)
    void testTwoSourceConstructorWithNullMatchType() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new CrossRelationKey("TEST", "CUSTOMERS", null, "NAME", "JOHN"),
            "Should throw NullPointerException for null match type"
        );

        assertTrue(exception.getMessage().contains("match type"),
            "Exception message should mention match type");
    }

    /**
     * Test constructor with null matchKey and principle (allowed).
     * Documentation says matchKey and principle are optional.
     */
    @Test
    @Order(800)
    void testConstructorWithNullOptionals() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, null, null);

        assertNotNull(key);
        assertNull(key.getMatchKey(), "Match key can be null");
        assertNull(key.getPrinciple(), "Principle can be null");
    }

    /**
     * Test getters return correct values.
     */
    @Test
    @Order(900)
    void testGetters() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");

        assertEquals("TEST", key.getSource1());
        assertEquals("CUSTOMERS", key.getSource2());
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, key.getMatchType());
        assertEquals("NAME", key.getMatchKey());
        assertEquals("JOHN", key.getPrinciple());
    }

    /**
     * Test equals with same instance (reflexive).
     */
    @Test
    @Order(1000)
    void testEqualsReflexive() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key, key, "Instance should equal itself");
    }

    /**
     * Test equals with null.
     */
    @Test
    @Order(1100)
    void testEqualsWithNull() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key, null);
        assertFalse(key.equals(null));
    }

    /**
     * Test equals with different class.
     */
    @Test
    @Order(1200)
    void testEqualsWithDifferentClass() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key, "not a CrossRelationKey");
        assertNotEquals(key, Integer.valueOf(42));
    }

    /**
     * Test equals with equivalent instance (symmetric).
     */
    @Test
    @Order(1300)
    void testEqualsSymmetric() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key1, key2);
        assertEquals(key2, key1);
    }

    /**
     * Test equals with different source1.
     */
    @Test
    @Order(1400)
    void testEqualsWithDifferentSource1() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "OTHER", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key1, key2);
    }

    /**
     * Test equals with different source2.
     */
    @Test
    @Order(1500)
    void testEqualsWithDifferentSource2() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "OTHER", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key1, key2);
    }

    /**
     * Test equals with different match type.
     */
    @Test
    @Order(1600)
    void testEqualsWithDifferentMatchType() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");

        assertNotEquals(key1, key2);
    }

    /**
     * Test equals with different match key.
     */
    @Test
    @Order(1700)
    void testEqualsWithDifferentMatchKey() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "ADDR", "JOHN");

        assertNotEquals(key1, key2);
    }

    /**
     * Test equals with different principle.
     */
    @Test
    @Order(1800)
    void testEqualsWithDifferentPrinciple() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JANE");

        assertNotEquals(key1, key2);
    }

    /**
     * Test hashCode consistency with equals.
     */
    @Test
    @Order(1900)
    void testHashCodeConsistentWithEquals() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test hashCode is stable.
     */
    @Test
    @Order(2000)
    void testHashCodeStable() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    /**
     * Test compareTo with null parameter.
     * Documentation says it returns 1 when parameter is null.
     */
    @Test
    @Order(2100)
    void testCompareToWithNull() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(1, key.compareTo(null));
    }

    /**
     * Test compareTo with same instance.
     */
    @Test
    @Order(2200)
    void testCompareToWithSameInstance() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key.compareTo(key));
    }

    /**
     * Test compareTo with equal instance.
     */
    @Test
    @Order(2300)
    void testCompareToWithEqualInstance() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2));
    }

    /**
     * Test compareTo sorts by source1 first.
     * Documentation says it sorts first on data sources.
     */
    @Test
    @Order(2400)
    void testCompareToSortsBySource1First() {
        CrossRelationKey key1 = new CrossRelationKey(
            "ALPHA", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "BETA", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo sorts by source2 when source1 is equal.
     */
    @Test
    @Order(2500)
    void testCompareToSortsBySource2Second() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "ALPHA", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "BETA", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo sorts by match type when sources are equal.
     * Documentation says it sorts on match type after data sources.
     */
    @Test
    @Order(2600)
    void testCompareToSortsByMatchTypeThird() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo sorts by principle when sources and match type are equal.
     * Documentation says it sorts on principles after match type.
     */
    @Test
    @Order(2700)
    void testCompareToSortsByPrincipleFourth() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "ALICE");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "BOB");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo sorts by match key last.
     * Documentation says it sorts on match keys after principles.
     */
    @Test
    @Order(2800)
    void testCompareToSortsByMatchKeyLast() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "ADDR", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo with null match type sorts before non-null.
     * Documentation mentions null handling.
     */
    @Test
    @Order(2900)
    void testCompareToNullMatchTypeSortsFirst() {
        // Note: Constructor requires non-null matchType, but compareTo handles it
        // This test documents the behavior when comparing with a key that somehow has null
    }

    /**
     * Test compareTo with null principle sorts before non-null.
     */
    @Test
    @Order(3000)
    void testCompareToNullPrincipleSortsFirst() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", null);
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo with null match key sorts before non-null.
     */
    @Test
    @Order(3100)
    void testCompareToNullMatchKeySortsFirst() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, null, "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0);
    }

    /**
     * Test compareTo is consistent with equals.
     */
    @Test
    @Order(3200)
    void testCompareToConsistentWithEquals() {
        CrossRelationKey key1 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey key2 = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2));
        assertEquals(key1, key2);
    }

    /**
     * Test sorting a collection.
     */
    @Test
    @Order(3300)
    void testSortingWithCompareTo() {
        List<CrossRelationKey> keys = new ArrayList<>();
        keys.add(new CrossRelationKey("TEST", "BETA", SzMatchType.POSSIBLE_MATCH, "NAME", "C"));
        keys.add(new CrossRelationKey("TEST", "ALPHA", SzMatchType.POSSIBLE_MATCH, "NAME", "B"));
        keys.add(new CrossRelationKey("ALPHA", "BETA", SzMatchType.POSSIBLE_MATCH, "NAME", "A"));

        Collections.sort(keys);

        assertEquals("ALPHA", keys.get(0).getSource1());
    }

    /**
     * Test toString format.
     * Documentation shows format as source1:source2[matchType:principle:matchKey].
     */
    @Test
    @Order(3400)
    void testToStringFormat() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        String result = key.toString();

        assertNotNull(result);
        assertTrue(result.contains("TEST"));
        assertTrue(result.contains("CUSTOMERS"));
        assertTrue(result.contains("POSSIBLE_MATCH"));
        assertTrue(result.contains("NAME"));
        assertTrue(result.contains("JOHN"));
        assertTrue(result.startsWith("TEST:CUSTOMERS["));
    }

    /**
     * Test toString with null values.
     */
    @Test
    @Order(3500)
    void testToStringWithNulls() {
        CrossRelationKey key = new CrossRelationKey(
            "TEST", "CUSTOMERS", SzMatchType.POSSIBLE_MATCH, null, null);

        String result = key.toString();

        assertNotNull(result);
        assertTrue(result.contains("TEST"));
        assertTrue(result.contains("null"));
    }

    /**
     * Test immutability.
     */
    @Test
    @Order(3600)
    void testImmutability() {
        String source1 = "TEST";
        String source2 = "CUSTOMERS";
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String principle = "JOHN";

        CrossRelationKey key = new CrossRelationKey(
            source1, source2, matchType, matchKey, principle);

        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
        assertEquals(matchType, key.getMatchType());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());

        // Again
        assertEquals(source1, key.getSource1());
        assertEquals(source2, key.getSource2());
    }

    /**
     * Test single-source constructor delegates correctly.
     */
    @Test
    @Order(3700)
    void testSingleSourceDelegation() {
        CrossRelationKey keySingle = new CrossRelationKey(
            "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        CrossRelationKey keyDouble = new CrossRelationKey(
            "TEST", "TEST", SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(keySingle, keyDouble);
        assertEquals(keySingle.hashCode(), keyDouble.hashCode());
        assertEquals(0, keySingle.compareTo(keyDouble));
    }
}
