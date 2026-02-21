package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class SzBoundTypeTest {

    @Test
    void testInclusiveLowerExists() {
        assertNotNull(SzBoundType.INCLUSIVE_LOWER);
    }

    @Test
    void testExclusiveLowerExists() {
        assertNotNull(SzBoundType.EXCLUSIVE_LOWER);
    }

    @Test
    void testInclusiveUpperExists() {
        assertNotNull(SzBoundType.INCLUSIVE_UPPER);
    }

    @Test
    void testExclusiveUpperExists() {
        assertNotNull(SzBoundType.EXCLUSIVE_UPPER);
    }

    @Test
    void testValuesContainsAllExpected() {
        SzBoundType[] values = SzBoundType.values();
        assertEquals(4, values.length);
    }

    @ParameterizedTest
    @EnumSource(SzBoundType.class)
    void testValueOf(SzBoundType boundType) {
        assertEquals(boundType, SzBoundType.valueOf(boundType.name()));
    }

    @Test
    void testValueOfInclusiveLower() {
        assertEquals(SzBoundType.INCLUSIVE_LOWER, SzBoundType.valueOf("INCLUSIVE_LOWER"));
    }

    @Test
    void testValueOfExclusiveLower() {
        assertEquals(SzBoundType.EXCLUSIVE_LOWER, SzBoundType.valueOf("EXCLUSIVE_LOWER"));
    }

    @Test
    void testValueOfInclusiveUpper() {
        assertEquals(SzBoundType.INCLUSIVE_UPPER, SzBoundType.valueOf("INCLUSIVE_UPPER"));
    }

    @Test
    void testValueOfExclusiveUpper() {
        assertEquals(SzBoundType.EXCLUSIVE_UPPER, SzBoundType.valueOf("EXCLUSIVE_UPPER"));
    }

    @Test
    void testValueOfWithInvalidName() {
        assertThrows(IllegalArgumentException.class, () -> SzBoundType.valueOf("INVALID"));
    }

    @Test
    void testValueOfWithNullName() {
        assertThrows(NullPointerException.class, () -> SzBoundType.valueOf(null));
    }

    @ParameterizedTest
    @EnumSource(SzBoundType.class)
    void testToStringMatchesName(SzBoundType boundType) {
        assertEquals(boundType.name(), boundType.toString());
    }

    @Test
    void testOrdinalValues() {
        assertEquals(0, SzBoundType.INCLUSIVE_LOWER.ordinal());
        assertEquals(1, SzBoundType.EXCLUSIVE_LOWER.ordinal());
        assertEquals(2, SzBoundType.INCLUSIVE_UPPER.ordinal());
        assertEquals(3, SzBoundType.EXCLUSIVE_UPPER.ordinal());
    }

    // isInclusive() and isExclusive() tests
    @Test
    void testInclusiveLowerIsInclusive() {
        assertTrue(SzBoundType.INCLUSIVE_LOWER.isInclusive());
        assertFalse(SzBoundType.INCLUSIVE_LOWER.isExclusive());
    }

    @Test
    void testExclusiveLowerIsExclusive() {
        assertFalse(SzBoundType.EXCLUSIVE_LOWER.isInclusive());
        assertTrue(SzBoundType.EXCLUSIVE_LOWER.isExclusive());
    }

    @Test
    void testInclusiveUpperIsInclusive() {
        assertTrue(SzBoundType.INCLUSIVE_UPPER.isInclusive());
        assertFalse(SzBoundType.INCLUSIVE_UPPER.isExclusive());
    }

    @Test
    void testExclusiveUpperIsExclusive() {
        assertFalse(SzBoundType.EXCLUSIVE_UPPER.isInclusive());
        assertTrue(SzBoundType.EXCLUSIVE_UPPER.isExclusive());
    }

    // isLower() and isUpper() tests
    @Test
    void testInclusiveLowerIsLower() {
        assertTrue(SzBoundType.INCLUSIVE_LOWER.isLower());
        assertFalse(SzBoundType.INCLUSIVE_LOWER.isUpper());
    }

    @Test
    void testExclusiveLowerIsLower() {
        assertTrue(SzBoundType.EXCLUSIVE_LOWER.isLower());
        assertFalse(SzBoundType.EXCLUSIVE_LOWER.isUpper());
    }

    @Test
    void testInclusiveUpperIsUpper() {
        assertFalse(SzBoundType.INCLUSIVE_UPPER.isLower());
        assertTrue(SzBoundType.INCLUSIVE_UPPER.isUpper());
    }

    @Test
    void testExclusiveUpperIsUpper() {
        assertFalse(SzBoundType.EXCLUSIVE_UPPER.isLower());
        assertTrue(SzBoundType.EXCLUSIVE_UPPER.isUpper());
    }

    // checkSatisfies() tests with Comparable
    @Test
    void testInclusiveLowerCheckSatisfiesWithEqualValue() {
        assertTrue(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(5, 5));
    }

    @Test
    void testInclusiveLowerCheckSatisfiesWithGreaterValue() {
        assertTrue(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(10, 5));
    }

    @Test
    void testInclusiveLowerCheckSatisfiesWithLesserValue() {
        assertFalse(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(3, 5));
    }

    @Test
    void testExclusiveLowerCheckSatisfiesWithEqualValue() {
        assertFalse(SzBoundType.EXCLUSIVE_LOWER.checkSatisfies(5, 5));
    }

    @Test
    void testExclusiveLowerCheckSatisfiesWithGreaterValue() {
        assertTrue(SzBoundType.EXCLUSIVE_LOWER.checkSatisfies(10, 5));
    }

    @Test
    void testExclusiveLowerCheckSatisfiesWithLesserValue() {
        assertFalse(SzBoundType.EXCLUSIVE_LOWER.checkSatisfies(3, 5));
    }

    @Test
    void testInclusiveUpperCheckSatisfiesWithEqualValue() {
        assertTrue(SzBoundType.INCLUSIVE_UPPER.checkSatisfies(5, 5));
    }

    @Test
    void testInclusiveUpperCheckSatisfiesWithGreaterValue() {
        assertFalse(SzBoundType.INCLUSIVE_UPPER.checkSatisfies(10, 5));
    }

    @Test
    void testInclusiveUpperCheckSatisfiesWithLesserValue() {
        assertTrue(SzBoundType.INCLUSIVE_UPPER.checkSatisfies(3, 5));
    }

    @Test
    void testExclusiveUpperCheckSatisfiesWithEqualValue() {
        assertFalse(SzBoundType.EXCLUSIVE_UPPER.checkSatisfies(5, 5));
    }

    @Test
    void testExclusiveUpperCheckSatisfiesWithGreaterValue() {
        assertFalse(SzBoundType.EXCLUSIVE_UPPER.checkSatisfies(10, 5));
    }

    @Test
    void testExclusiveUpperCheckSatisfiesWithLesserValue() {
        assertTrue(SzBoundType.EXCLUSIVE_UPPER.checkSatisfies(3, 5));
    }

    @Test
    void testCheckSatisfiesWithNullValue() {
        assertThrows(NullPointerException.class,
                () -> SzBoundType.INCLUSIVE_LOWER.checkSatisfies(null, 5));
    }

    @Test
    void testCheckSatisfiesWithNullBoundValue() {
        assertThrows(NullPointerException.class,
                () -> SzBoundType.INCLUSIVE_LOWER.checkSatisfies(5, null));
    }

    // checkSatisfies() tests with Comparator
    @Test
    void testCheckSatisfiesWithComparatorAndEqualValue() {
        Comparator<Integer> comparator = Integer::compareTo;
        assertTrue(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(5, 5, comparator));
    }

    @Test
    void testCheckSatisfiesWithComparatorAndGreaterValue() {
        Comparator<Integer> comparator = Integer::compareTo;
        assertTrue(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(10, 5, comparator));
    }

    @Test
    void testCheckSatisfiesWithComparatorAndLesserValue() {
        Comparator<Integer> comparator = Integer::compareTo;
        assertFalse(SzBoundType.INCLUSIVE_LOWER.checkSatisfies(3, 5, comparator));
    }

    @Test
    void testCheckSatisfiesWithComparatorAndNullValue() {
        Comparator<Integer> comparator = Integer::compareTo;
        assertThrows(NullPointerException.class,
                () -> SzBoundType.INCLUSIVE_LOWER.checkSatisfies(null, 5, comparator));
    }

    @Test
    void testCheckSatisfiesWithComparatorAndNullBoundValue() {
        Comparator<Integer> comparator = Integer::compareTo;
        assertThrows(NullPointerException.class,
                () -> SzBoundType.INCLUSIVE_LOWER.checkSatisfies(5, null, comparator));
    }

    @Test
    void testCheckSatisfiesWithNullComparator() {
        assertThrows(NullPointerException.class,
                () -> SzBoundType.INCLUSIVE_LOWER.checkSatisfies(5, 5, null));
    }
}
