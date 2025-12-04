package com.senzing.datamart.reports.model;

import java.util.Comparator;
import java.util.Objects;

/**
 * Enumerates the various types of bounds that can be applied as criteria for a
 * value having an ordering.
 */
public enum SzBoundType {
    /**
     * Describes a bound that is a lower bound that includes the bound value. Values
     * that satisfy such a bound are greater than or equal to the bound value.
     */
    INCLUSIVE_LOWER(true, true),

    /**
     * Describes a bound that is a lower bound that excludes the bound value. Values
     * that satisfy such a bound are strictly greater than the bound value.
     */
    EXCLUSIVE_LOWER(false, true),

    /**
     * Describes a bound that is an upper bound that includes the bound value.
     * Values that satisfy such a bound are less than or equal to the bound value.
     */
    INCLUSIVE_UPPER(true, false),

    /**
     * Describes a bound that is an upper bound that excludes the bound value.
     * Values that satisfy such a bound are strictly less than the bound value.
     */
    EXCLUSIVE_UPPER(false, false);

    /**
     * Flag indicating if this instance is inclusive. If <code>true</code> then it
     * is inclusive, otherwise if <code>false</code> then the bound type is
     * exclusive.
     */
    private boolean inclusive = false;

    /**
     * Flag indicating fi this instance is a lower bound. If <code>true</code> then
     * it is a lower bound, otherwise if <code>false</code> then it is an upper
     * bound.
     */
    private boolean lower = false;

    /**
     * Private constructor that constructs with the inclusivity and direction of the
     * bound type.
     * 
     * @param inclusive <code>true</code> if the bound is inclusive, and
     *                  <code>false</code> if it is exclusive.
     * @param lower     <code>true</code> if the bound is a lower bound, and
     *                  <code>false</code> if it is an upper bound.
     */
    SzBoundType(boolean inclusive, boolean lower) {
        this.inclusive = inclusive;
        this.lower = lower;
    }

    /**
     * Checks if this instance describes an inclusive bound.
     * 
     * @return <code>true</code> if this instance describes an inclusive bound,
     *         otherwise <code>false</code>
     */
    public boolean isInclusive() {
        return this.inclusive;
    }

    /**
     * Checks if this instance describes an exclusive bound.
     * 
     * @return <code>true</code> if this instance describes an exclusive bound,
     *         otherwise <code>false</code>
     */
    public boolean isExclusive() {
        return (!this.isInclusive());
    }

    /**
     * Checks if this instance describes a lower bound.
     * 
     * @return <code>true</code> if this instance describes a
     *         lower bound, otherwise <code>false</code>.
     */
    public boolean isLower() {
        return this.lower;
    }

    /**
     * Checks if this instance describes an upper bound.
     * 
     * @return <code>true</code> if this instance describes an
     *         upper bound, otherwise <code>false</code>.
     */
    public boolean isUpper() {
        return (!this.isLower());
    }

    /**
     * Compares the two specified values and returns <code>true</code> if the
     * specified value satisfies the bound the defined by this {@link SzBoundType}
     * instance and the specified bound value.
     * 
     * @param <T> The type of the bound value.
     * @param value      The value to check if it satisfies the bound.
     * @param boundValue The bound value to couple with this {@link SzBoundType}
     *                   instance to define the bound.
     * 
     * @return <code>true</code> if the value satisfies the bound, otherwise
     *         <code>false</code>
     * 
     * @throws NullPointerException If either of the specified values is
     *                              <code>null</code>.
     */
    public <T extends Comparable<T>> boolean checkSatisfies(T value, T boundValue) throws NullPointerException {
        Objects.requireNonNull(value, "The specified value cannot be null");
        Objects.requireNonNull(boundValue, "The specified bound value cannot be null");
        int compare = value.compareTo(boundValue);
        if (compare == 0) {
            return this.isInclusive();
        }
        if (compare > 0) {
            return this.isLower();
        }
        return this.isUpper();
    }

    /**
     * Compares the two specified values and returns <code>true</code> if the
     * specified value satisfies the bound the defined by this {@link SzBoundType}
     * instance and the specified bound value.
     * 
     * @param value      The value to check if it satisfies the bound.
     * @param boundValue The bound value to couple with this {@link SzBoundType}
     *                   instance to define the bound.
     * @param comparator The {@link Comparator} to use for comparing the values.
     * 
     * @param <T>        The type of the objects being compared.
     * 
     * @return <code>true</code> if the value satisfies the bound, otherwise
     *         <code>false</code>
     * 
     * @throws NullPointerException If either of the specified values is
     *                              <code>null</code> or if the specified
     *                              {@link Comparator} is <code>null</code>.
     */
    public <T> boolean checkSatisfies(T value, T boundValue, Comparator<T> comparator) throws NullPointerException {
        Objects.requireNonNull(value, "The specified value cannot be null");
        Objects.requireNonNull(boundValue, "The specified bound value cannot be null");
        Objects.requireNonNull(comparator, "The specified comparator cannot be null");
        int compare = comparator.compare(value, boundValue);
        if (compare == 0) {
            return this.isInclusive();
        }
        if (compare > 0) {
            return this.isLower();
        }
        return this.isUpper();
    }
}
