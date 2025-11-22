package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Provides a key for a match key and principle pair that is tolerant of
 * <code>null</code> match key and/or principle values in its
 * {@link #equals(Object)}, {@link #hashCode()} and
 * {@link #compareTo(SzCountsKey)} functions including comparison against
 * <code>null</code> references to this class. Any <code>null</code> value is
 * sorted as "less-than" a value that is <b>not</b> <code>null</code>.
 */
class SzCountsKey implements Comparable<SzCountsKey>, Serializable {
    /**
     * The associated match key.
     */
    private String matchKey;

    /**
     * The associated principle.
     */
    private String principle;

    /**
     * Constructs with the specified match key and principle.
     * 
     * @param matchKey  The match key to associate with the new instance.
     * @param principle The principle to associate with the new instance.
     */
    protected SzCountsKey(String matchKey, String principle) {
        this.matchKey = matchKey;
        this.principle = principle;
    }

    /**
     * Gets the associated match key.
     * 
     * @return The associated match key.
     */
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Gets the associated principle.
     * 
     * @return The associated principle.
     */
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Implemented to return <code>true</code> if and only if the specified
     * parameter is a non-null reference to an object of the same class with an
     * equivalent match key and principle.
     * 
     * @param obj The object to compare with.
     * 
     * @return <code>true</code> if and only if the specified parameter is a
     *         non-null reference to an object of the same class with an equivalent
     *         match key and principle, otherwise <code>false</code>.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;
        if (this.getClass() != obj.getClass())
            return false;

        SzCountsKey smk = (SzCountsKey) obj;
        return Objects.equals(this.getMatchKey(), smk.getMatchKey())
                && Objects.equals(this.getPrinciple(), smk.getPrinciple());
    }

    /**
     * Implemented to return a hash code based on the match key and principle.
     * 
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getMatchKey(), this.getPrinciple());
    }

    /**
     * Implemented to return a negative number, zero (0) or a positive number
     * depending on whether this instance compares less-than, equal-to, or greater
     * than the specified parameter with sorting first on match key and then
     * principle with <code>null</code> values comparing less-than values that are
     * not <code>null</code>.
     * 
     * @param key The {@link SzCountsKey} to compare with.
     * 
     * @return A negative number, zero (0) or a positive number depending on whether
     *         this instance compares less-than, equal-to, or greater than the
     *         specified parameter.
     */
    @Override
    public int compareTo(SzCountsKey key) {
        if (key == null)
            return 1;
        String mk1 = this.getMatchKey();
        String mk2 = key.getMatchKey();
        if (!Objects.equals(mk1, mk2)) {
            if (mk1 == null)
                return -1;
            if (mk2 == null)
                return 1;
            return mk1.compareTo(mk2);
        }
        String p1 = this.getPrinciple();
        String p2 = key.getPrinciple();
        if (Objects.equals(p1, p2))
            return 0;
        if (p1 == null)
            return -1;
        if (p2 == null)
            return 1;
        return p1.compareTo(p2);
    }

    /**
     * Implemented to return a diagnostic {@link String} describing this instance
     * with the principle first and then the match key, separated by a colon.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    public String toString() {
        return this.getPrinciple() + ":" + this.getMatchKey();
    }

}
