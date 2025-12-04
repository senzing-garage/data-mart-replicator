package com.senzing.datamart.handlers;

import java.util.Objects;

/**
 * An object describing a principle and match key pair with proper
 * implementations of {@link #equals(Object)}, {@link #hashCode()} and
 * {@link #compareTo(MatchPairKey)}. This class can handle the match key and/or
 * principle having <code>null</code> values to indicate "any" match key or
 * principle, respectively.
 */
public class MatchPairKey implements Comparable<MatchPairKey> {
    /**
     * The match key associated with the relationships.
     */
    private String matchKey;

    /**
     * The principle associated with the relationships.
     */
    private String principle;

    /**
     * Constructs with the specified match key and principle. Both of the parameters
     * may be <code>null</code>.
     *
     * @param matchKey  The match key for the relationships.
     * @param principle The principle for the relationships.
     */
    public MatchPairKey(String matchKey, String principle) {
        this.matchKey = matchKey;
        this.principle = principle;
    }

    /**
     * Gets the match key. This may return <code>null</code> if constructed with a
     * <code>null</code> match key.
     * 
     * @return The associated match key, or <code>null</code> if no specific
     *         principle was associated.
     */
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Gets the associated principle. This may return <code>null</code> if
     * constructed with a <code>null</code> principle.
     * 
     * @return The associated principle, or <code>null</code> if no specific
     *         principle was associated.
     */
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Gets the hash code for this instance.
     *
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getMatchKey(), this.getPrinciple());
    }

    /**
     * Checks if this instance is equal to the specified object. This is implemented
     * to return <code>true</code> if and only if the specified value is a non-null
     * reference to an object of the same class with equivalent properties.
     *
     * @param obj The object to compare with.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        MatchPairKey key = (MatchPairKey) obj;
        return Objects.equals(this.getMatchKey(), key.getMatchKey())
                && Objects.equals(this.getPrinciple(), key.getPrinciple());
    }

    /**
     * Implemented to handle comparing such that we sort first on principles and
     * then on match keys with <code>null</code> values sorting less-than non-null
     * values. If the specified parameter is <code>null</code> then this returns one
     * (1).
     * 
     * @param key The {@link MatchPairKey} to compare with.
     * 
     * @return A negative number if this instance compares less-than the specified
     *         parameter, a positive number if it compares greater-than the
     *         specified parameter and zero (0) if they compare equal.
     */
    @Override
    public int compareTo(MatchPairKey key) {
        if (key == null) {
            return 1;
        }
        if (key == this) {
            return 0;
        }
        String matchKey1 = this.getMatchKey();
        String principle1 = this.getPrinciple();
        String matchKey2 = key.getMatchKey();
        String principle2 = key.getPrinciple();
        if (!Objects.equals(principle1, principle2)) {
            if (principle1 == null) {
                return -1;
            }
            if (principle2 == null) {
                return 1;
            }
            return principle1.compareTo(principle2);
        }
        if (Objects.equals(matchKey1, matchKey2)) {
            return 0;
        }
        if (matchKey1 == null) {
            return -1;
        }
        if (matchKey2 == null) {
            return 1;
        }
        return matchKey1.compareTo(matchKey2);
    }

    /**
     * Implemented to provide a diagnostic {@link String} describing this instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "[" + this.getPrinciple() + ":" + this.getMatchKey() + "]";
    }
}
