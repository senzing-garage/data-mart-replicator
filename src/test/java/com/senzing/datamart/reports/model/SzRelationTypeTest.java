package com.senzing.datamart.reports.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class SzRelationTypeTest {

    @Test
    void testAmbiguousMatchExists() {
        assertNotNull(SzRelationType.AMBIGUOUS_MATCH);
    }

    @Test
    void testPossibleMatchExists() {
        assertNotNull(SzRelationType.POSSIBLE_MATCH);
    }

    @Test
    void testPossibleRelationExists() {
        assertNotNull(SzRelationType.POSSIBLE_RELATION);
    }

    @Test
    void testDisclosedRelationExists() {
        assertNotNull(SzRelationType.DISCLOSED_RELATION);
    }

    @Test
    void testValuesContainsAllExpected() {
        SzRelationType[] values = SzRelationType.values();
        assertEquals(4, values.length);
    }

    @ParameterizedTest
    @EnumSource(SzRelationType.class)
    void testValueOf(SzRelationType relationType) {
        assertEquals(relationType, SzRelationType.valueOf(relationType.name()));
    }

    @Test
    void testValueOfAmbiguousMatch() {
        assertEquals(SzRelationType.AMBIGUOUS_MATCH, SzRelationType.valueOf("AMBIGUOUS_MATCH"));
    }

    @Test
    void testValueOfPossibleMatch() {
        assertEquals(SzRelationType.POSSIBLE_MATCH, SzRelationType.valueOf("POSSIBLE_MATCH"));
    }

    @Test
    void testValueOfPossibleRelation() {
        assertEquals(SzRelationType.POSSIBLE_RELATION, SzRelationType.valueOf("POSSIBLE_RELATION"));
    }

    @Test
    void testValueOfDisclosedRelation() {
        assertEquals(SzRelationType.DISCLOSED_RELATION, SzRelationType.valueOf("DISCLOSED_RELATION"));
    }

    @Test
    void testValueOfWithInvalidName() {
        assertThrows(IllegalArgumentException.class, () -> SzRelationType.valueOf("INVALID"));
    }

    @Test
    void testValueOfWithNullName() {
        assertThrows(NullPointerException.class, () -> SzRelationType.valueOf(null));
    }

    @ParameterizedTest
    @EnumSource(SzRelationType.class)
    void testToStringMatchesName(SzRelationType relationType) {
        assertEquals(relationType.name(), relationType.toString());
    }

    @Test
    void testOrdinalValues() {
        assertEquals(0, SzRelationType.AMBIGUOUS_MATCH.ordinal());
        assertEquals(1, SzRelationType.POSSIBLE_MATCH.ordinal());
        assertEquals(2, SzRelationType.POSSIBLE_RELATION.ordinal());
        assertEquals(3, SzRelationType.DISCLOSED_RELATION.ordinal());
    }
}
