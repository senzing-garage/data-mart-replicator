package com.senzing.datamart.handlers;

import java.util.Objects;

/**
 * The key for a cross-source report statistic comprising two data sources.
 */
public class CrossSourceKey implements Comparable<CrossSourceKey> {
    /**
     * The first data source.
     */
    private String source1;

    /**
     * The second data source.
     */
    private String source2;

    /**
     * Constructs with the specified parameters.
     *
     * @param source The non-null data source code representing both the
     *               "from" and "to" data source.
     * 
     * @throws NullPointerException If the specified data source is <code>null</code>
     */
    public CrossSourceKey(String source) 
        throws NullPointerException 
    {
        this(source, source);
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param source1   The non-null first ("from") data source code.
     * @param source2   The non-null second ("to") data source code.
     * 
     * @throws NullPointerException If either of the data sources is
     *                              <code>null</code>
     */
    public CrossSourceKey(String source1, String source2)
        throws NullPointerException 
    {
        Objects.requireNonNull(source1, "The first data source cannot be null");
        Objects.requireNonNull(source2, "The second data source cannot be null");

        this.source1 = source1;
        this.source2 = source2;
    }

    /**
     * Gets the first ("from") data source code.
     *
     * @return The first ("from") data source code.
     */
    public String getSource1() {
        return this.source1;
    }

    /**
     * Gets the second ("to") data source code.
     *
     * @return The second ("to") data source code.
     */
    public String getSource2() {
        return this.source2;
    }

    /**
     * Gets the hash code for this instance.
     *
     * @return The hash code for this instance.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.getSource1(), this.getSource2());
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
        CrossSourceKey key = (CrossSourceKey) obj;
        return Objects.equals(this.getSource1(), key.getSource1())
                && Objects.equals(this.getSource2(), key.getSource2());
    }

    /**
     * Implemented to handle comparing such that we sort first on the associated
     * data sources, then on principles and then on match keys with
     * <code>null</code> values sorting less-than non-null values. If the specified
     * parameter is <code>null</code> then this returns one (1).
     * 
     * @param key The {@link CrossSourceKey} to compare with.
     * 
     * @return A negative number if this instance compares less-than the specified
     *         parameter, a positive number if it compares greater-than the
     *         specified parameter and zero (0) if they compare equal.
     */
    @Override
    public int compareTo(CrossSourceKey key) {
        if (key == null) {
            return 1;
        }
        if (key == this) {
            return 0;
        }

        // first compare the data sources which cannot be null
        int diff = this.getSource1().compareTo(key.getSource1());
        if (diff != 0) {
            return diff;
        }
        return this.getSource2().compareTo(key.getSource2());
    }

    /**
     * Implemented to provide a diagnostic {@link String} describing this instance.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return this.getSource1() + ":" + this.getSource2();
    }
}
