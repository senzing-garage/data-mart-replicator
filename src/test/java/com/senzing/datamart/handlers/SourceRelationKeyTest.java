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
import java.util.Set;

/**
 * Comprehensive unit tests for {@link SourceRelationKey}.
 * Tests immutability, constructor, getters, equals, hashCode, compareTo, toString, and variants().
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SourceRelationKeyTest {

    /**
     * Test constructor with valid parameters.
     */
    @Test
    @Order(100)
    void testConstructorWithValidParameters() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String principle = "JOHN";

        SourceRelationKey key = new SourceRelationKey(matchType, matchKey, principle);

        assertNotNull(key, "Key should not be null");
        assertEquals(matchType, key.getMatchType(), "Match type should match");
        assertEquals(matchKey, key.getMatchKey(), "Match key should match");
        assertEquals(principle, key.getPrinciple(), "Principle should match");
    }

    /**
     * Test constructor with null matchType throws NullPointerException.
     * Documentation says match type cannot be null.
     */
    @Test
    @Order(200)
    void testConstructorWithNullMatchType() {
        NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> new SourceRelationKey(null, "NAME", "JOHN"),
            "Should throw NullPointerException for null match type"
        );

        assertTrue(exception.getMessage().contains("match type"),
            "Exception message should mention match type");
    }

    /**
     * Test constructor with null matchKey and principle (allowed).
     * Documentation says matchKey and principle can be null.
     */
    @Test
    @Order(300)
    void testConstructorWithNullOptionals() {
        SzMatchType matchType = SzMatchType.DISCLOSED_RELATION;

        SourceRelationKey key = new SourceRelationKey(matchType, null, null);

        assertNotNull(key, "Key should not be null");
        assertEquals(matchType, key.getMatchType());
        assertNull(key.getMatchKey(), "Match key can be null");
        assertNull(key.getPrinciple(), "Principle can be null");
    }

    /**
     * Test constructor with all match types.
     */
    @Test
    @Order(400)
    void testConstructorWithAllMatchTypes() {
        for (SzMatchType matchType : SzMatchType.values()) {
            SourceRelationKey key = new SourceRelationKey(matchType, "NAME", "JOHN");

            assertEquals(matchType, key.getMatchType(),
                "Match type should be " + matchType);
        }
    }

    /**
     * Test getters return correct values.
     */
    @Test
    @Order(500)
    void testGetters() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");

        assertEquals(SzMatchType.AMBIGUOUS_MATCH, key.getMatchType());
        assertEquals("NAME", key.getMatchKey());
        assertEquals("JOHN", key.getPrinciple());
    }

    /**
     * Test equals with same instance (reflexive).
     */
    @Test
    @Order(600)
    void testEqualsReflexive() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key, key, "Instance should equal itself");
    }

    /**
     * Test equals with null.
     * Documentation says it returns false for null.
     */
    @Test
    @Order(700)
    void testEqualsWithNull() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key, null, "Should not equal null");
        assertFalse(key.equals(null), "equals(null) should return false");
    }

    /**
     * Test equals with different class.
     */
    @Test
    @Order(800)
    void testEqualsWithDifferentClass() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertNotEquals(key, "not a SourceRelationKey");
        assertNotEquals(key, Integer.valueOf(42));
    }

    /**
     * Test equals with equivalent instance (symmetric).
     */
    @Test
    @Order(900)
    void testEqualsSymmetric() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key1, key2, "Equal instances should be equal");
        assertEquals(key2, key1, "Equality should be symmetric");
    }

    /**
     * Test equals with different match type.
     */
    @Test
    @Order(1000)
    void testEqualsWithDifferentMatchType() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");

        assertNotEquals(key1, key2, "Different match types should not be equal");
    }

    /**
     * Test equals with different match key.
     */
    @Test
    @Order(1100)
    void testEqualsWithDifferentMatchKey() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "ADDR", "JOHN");

        assertNotEquals(key1, key2, "Different match keys should not be equal");
    }

    /**
     * Test equals with different principle.
     */
    @Test
    @Order(1200)
    void testEqualsWithDifferentPrinciple() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JANE");

        assertNotEquals(key1, key2, "Different principles should not be equal");
    }

    /**
     * Test hashCode consistency with equals.
     */
    @Test
    @Order(1300)
    void testHashCodeConsistentWithEquals() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(key1, key2, "Keys should be equal");
        assertEquals(key1.hashCode(), key2.hashCode(),
            "Equal objects must have equal hash codes");
    }

    /**
     * Test hashCode is stable.
     */
    @Test
    @Order(1400)
    void testHashCodeStable() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        int hashCode1 = key.hashCode();
        int hashCode2 = key.hashCode();

        assertEquals(hashCode1, hashCode2, "hashCode should be stable");
    }

    /**
     * Test compareTo with null parameter.
     * Documentation says it returns 1 when parameter is null.
     */
    @Test
    @Order(1500)
    void testCompareToWithNull() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(1, key.compareTo(null),
            "compareTo(null) should return 1");
    }

    /**
     * Test compareTo with same instance.
     */
    @Test
    @Order(1600)
    void testCompareToWithSameInstance() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key.compareTo(key),
            "compareTo same instance should return 0");
    }

    /**
     * Test compareTo with equal instance.
     */
    @Test
    @Order(1700)
    void testCompareToWithEqualInstance() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2),
            "Equal instances should compare as 0");
    }

    /**
     * Test compareTo sorts by match type first.
     * Documentation shows compareTo compares match types first.
     */
    @Test
    @Order(1800)
    void testCompareToSortsByMatchTypeFirst() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.AMBIGUOUS_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        // AM comes before PM in enum order
        assertTrue(key1.compareTo(key2) < 0,
            "AMBIGUOUS_MATCH should sort before POSSIBLE_MATCH");
        assertTrue(key2.compareTo(key1) > 0,
            "POSSIBLE_MATCH should sort after AMBIGUOUS_MATCH");
    }

    /**
     * Test compareTo sorts by match key when match types are equal.
     * Documentation shows compareTo compares match keys second.
     */
    @Test
    @Order(1900)
    void testCompareToSortsByMatchKeySecond() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "ADDR", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "ADDR should sort before NAME when match types are equal");
    }

    /**
     * Test compareTo sorts by principle when match type and key are equal.
     * Documentation shows compareTo compares principles last.
     */
    @Test
    @Order(2000)
    void testCompareToSortsByPrincipleThird() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "ALICE");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "BOB");

        assertTrue(key1.compareTo(key2) < 0,
            "ALICE should sort before BOB");
    }

    /**
     * Test compareTo with null match key sorts before non-null.
     * Documentation mentions null handling in compareTo.
     */
    @Test
    @Order(2100)
    void testCompareToNullMatchKeySortsFirst() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, null, "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null match key should sort before non-null");
    }

    /**
     * Test compareTo with null principle sorts before non-null.
     */
    @Test
    @Order(2200)
    void testCompareToNullPrincipleSortsFirst() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", null);
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertTrue(key1.compareTo(key2) < 0,
            "Null principle should sort before non-null");
    }

    /**
     * Test compareTo is consistent with equals.
     */
    @Test
    @Order(2300)
    void testCompareToConsistentWithEquals() {
        SourceRelationKey key1 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");
        SourceRelationKey key2 = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        assertEquals(0, key1.compareTo(key2), "Equal objects should compare as 0");
        assertEquals(key1, key2, "Objects that compare as 0 should be equal");
    }

    /**
     * Test sorting a collection.
     */
    @Test
    @Order(2400)
    void testSortingWithCompareTo() {
        List<SourceRelationKey> keys = new ArrayList<>();
        keys.add(new SourceRelationKey(SzMatchType.POSSIBLE_MATCH, "NAME", "CHARLIE"));
        keys.add(new SourceRelationKey(SzMatchType.AMBIGUOUS_MATCH, "NAME", "BOB"));
        keys.add(new SourceRelationKey(SzMatchType.DISCLOSED_RELATION, null, null));
        keys.add(new SourceRelationKey(SzMatchType.AMBIGUOUS_MATCH, "ADDR", "ALICE"));

        Collections.sort(keys);

        // First should be AMBIGUOUS_MATCH (comes first in enum)
        assertEquals(SzMatchType.AMBIGUOUS_MATCH, keys.get(0).getMatchType(),
            "First should be AMBIGUOUS_MATCH");
    }

    /**
     * Test toString format.
     * Documentation shows toString returns matchType:principle:matchKey.
     */
    @Test
    @Order(2500)
    void testToStringFormat() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, "NAME", "JOHN");

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("POSSIBLE_MATCH"), "toString should contain match type");
        assertTrue(result.contains("NAME"), "toString should contain match key");
        assertTrue(result.contains("JOHN"), "toString should contain principle");
        // Format: POSSIBLE_MATCH:JOHN:NAME
        assertEquals("POSSIBLE_MATCH:JOHN:NAME", result,
            "toString should return matchType:principle:matchKey");
    }

    /**
     * Test toString with null values.
     */
    @Test
    @Order(2600)
    void testToStringWithNulls() {
        SourceRelationKey key = new SourceRelationKey(
            SzMatchType.POSSIBLE_MATCH, null, null);

        String result = key.toString();

        assertNotNull(result, "toString should not return null");
        assertTrue(result.contains("POSSIBLE_MATCH"));
        assertTrue(result.contains("null"), "toString should show null values");
    }

    /**
     * Test variants() static method with different match and reverse match keys.
     * Documentation says it creates variant keys with/without match key and principle.
     */
    @Test
    @Order(2700)
    void testVariantsWithDifferentMatchKeys() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String revMatchKey = "NAME_REV";
        String principle = "JOHN";

        Set<SourceRelationKey> variants = SourceRelationKey.variants(
            matchType, matchKey, revMatchKey, principle);

        assertNotNull(variants, "Variants should not be null");
        // Should have 6 variants: (null,null), (NAME,null), (NAME,JOHN), (null,JOHN),
        // (NAME_REV,null), (NAME_REV,JOHN)
        assertEquals(6, variants.size(),
            "Should have 6 variants with different match keys");

        // Verify specific variants exist
        assertTrue(variants.contains(new SourceRelationKey(matchType, null, null)),
            "Should contain variant with both null");
        assertTrue(variants.contains(new SourceRelationKey(matchType, matchKey, null)),
            "Should contain variant with matchKey only");
        assertTrue(variants.contains(new SourceRelationKey(matchType, matchKey, principle)),
            "Should contain variant with both values");
        assertTrue(variants.contains(new SourceRelationKey(matchType, null, principle)),
            "Should contain variant with principle only");
        assertTrue(variants.contains(new SourceRelationKey(matchType, revMatchKey, null)),
            "Should contain variant with revMatchKey only");
        assertTrue(variants.contains(new SourceRelationKey(matchType, revMatchKey, principle)),
            "Should contain variant with revMatchKey and principle");
    }

    /**
     * Test variants() with same match and reverse match keys.
     * Documentation says it handles when match and reverse match keys are equal.
     */
    @Test
    @Order(2800)
    void testVariantsWithSameMatchKeys() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String principle = "JOHN";

        Set<SourceRelationKey> variants = SourceRelationKey.variants(
            matchType, matchKey, matchKey, principle);

        assertNotNull(variants, "Variants should not be null");
        // Should have 4 variants when match and reverse are the same:
        // (null,null), (NAME,null), (NAME,JOHN), (null,JOHN)
        assertEquals(4, variants.size(),
            "Should have 4 variants with same match keys");

        // Verify no duplicate keys
        assertTrue(variants.contains(new SourceRelationKey(matchType, null, null)));
        assertTrue(variants.contains(new SourceRelationKey(matchType, matchKey, null)));
        assertTrue(variants.contains(new SourceRelationKey(matchType, matchKey, principle)));
        assertTrue(variants.contains(new SourceRelationKey(matchType, null, principle)));
    }

    /**
     * Test variants() with null match type throws NullPointerException.
     */
    @Test
    @Order(2900)
    void testVariantsWithNullMatchType() {
        assertThrows(
            NullPointerException.class,
            () -> SourceRelationKey.variants(null, "NAME", "NAME_REV", "JOHN"),
            "variants() should throw NullPointerException for null match type"
        );
    }

    /**
     * Test variants() with null match keys and principle.
     */
    @Test
    @Order(3000)
    void testVariantsWithNullValues() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;

        Set<SourceRelationKey> variants = SourceRelationKey.variants(
            matchType, null, null, null);

        assertNotNull(variants, "Variants should not be null");
        // Should only have 1 variant: (null, null)
        assertEquals(1, variants.size(),
            "Should have 1 variant when all optional params are null");

        assertTrue(variants.contains(new SourceRelationKey(matchType, null, null)));
    }

    /**
     * Test variants() returns a TreeSet (sorted set).
     */
    @Test
    @Order(3100)
    void testVariantsReturnsSortedSet() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;

        Set<SourceRelationKey> variants = SourceRelationKey.variants(
            matchType, "NAME", "NAME_REV", "JOHN");

        // TreeSet maintains sorted order
        List<SourceRelationKey> list = new ArrayList<>(variants);

        // Verify the set is sorted
        for (int i = 1; i < list.size(); i++) {
            assertTrue(list.get(i - 1).compareTo(list.get(i)) < 0,
                "Variants should be in sorted order");
        }
    }

    /**
     * Test immutability - values don't change after construction.
     */
    @Test
    @Order(3200)
    void testImmutability() {
        SzMatchType matchType = SzMatchType.POSSIBLE_MATCH;
        String matchKey = "NAME";
        String principle = "JOHN";

        SourceRelationKey key = new SourceRelationKey(matchType, matchKey, principle);

        // Get values multiple times
        assertEquals(matchType, key.getMatchType());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());

        // Again
        assertEquals(matchType, key.getMatchType());
        assertEquals(matchKey, key.getMatchKey());
        assertEquals(principle, key.getPrinciple());
    }
}
