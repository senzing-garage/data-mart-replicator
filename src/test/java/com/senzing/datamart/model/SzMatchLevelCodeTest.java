package com.senzing.datamart.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.*;

class SzMatchLevelCodeTest {

    @Test
    void testResolvedValue() {
        assertEquals("RESOLVED", SzMatchLevelCode.RESOLVED.name());
    }

    @Test
    void testPossiblySameValue() {
        assertEquals("POSSIBLY_SAME", SzMatchLevelCode.POSSIBLY_SAME.name());
    }

    @Test
    void testPossiblyRelatedValue() {
        assertEquals("POSSIBLY_RELATED", SzMatchLevelCode.POSSIBLY_RELATED.name());
    }

    @Test
    void testNameOnlyValue() {
        assertEquals("NAME_ONLY", SzMatchLevelCode.NAME_ONLY.name());
    }

    @Test
    void testDisclosedValue() {
        assertEquals("DISCLOSED", SzMatchLevelCode.DISCLOSED.name());
    }

    @Test
    void testValuesContainsAllExpected() {
        SzMatchLevelCode[] values = SzMatchLevelCode.values();
        assertEquals(5, values.length);
    }

    @ParameterizedTest
    @EnumSource(SzMatchLevelCode.class)
    void testValueOf(SzMatchLevelCode code) {
        assertEquals(code, SzMatchLevelCode.valueOf(code.name()));
    }

    @Test
    void testValueOfResolved() {
        assertEquals(SzMatchLevelCode.RESOLVED, SzMatchLevelCode.valueOf("RESOLVED"));
    }

    @Test
    void testValueOfPossiblySame() {
        assertEquals(SzMatchLevelCode.POSSIBLY_SAME, SzMatchLevelCode.valueOf("POSSIBLY_SAME"));
    }

    @Test
    void testValueOfPossiblyRelated() {
        assertEquals(SzMatchLevelCode.POSSIBLY_RELATED, SzMatchLevelCode.valueOf("POSSIBLY_RELATED"));
    }

    @Test
    void testValueOfNameOnly() {
        assertEquals(SzMatchLevelCode.NAME_ONLY, SzMatchLevelCode.valueOf("NAME_ONLY"));
    }

    @Test
    void testValueOfDisclosed() {
        assertEquals(SzMatchLevelCode.DISCLOSED, SzMatchLevelCode.valueOf("DISCLOSED"));
    }

    @Test
    void testValueOfWithInvalidName() {
        assertThrows(IllegalArgumentException.class, () ->
            SzMatchLevelCode.valueOf("INVALID"));
    }

    @Test
    void testValueOfWithNullName() {
        assertThrows(NullPointerException.class, () ->
            SzMatchLevelCode.valueOf(null));
    }

    @ParameterizedTest
    @EnumSource(SzMatchLevelCode.class)
    void testToStringMatchesName(SzMatchLevelCode code) {
        assertEquals(code.name(), code.toString());
    }

    @Test
    void testOrdinalValues() {
        assertEquals(0, SzMatchLevelCode.RESOLVED.ordinal());
        assertEquals(1, SzMatchLevelCode.POSSIBLY_SAME.ordinal());
        assertEquals(2, SzMatchLevelCode.POSSIBLY_RELATED.ordinal());
        assertEquals(3, SzMatchLevelCode.NAME_ONLY.ordinal());
        assertEquals(4, SzMatchLevelCode.DISCLOSED.ordinal());
    }
}
