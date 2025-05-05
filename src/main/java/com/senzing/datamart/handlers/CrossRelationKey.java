package com.senzing.datamart.handlers;

import java.util.Objects;
import com.senzing.datamart.model.SzMatchType;

/**
 * The key for a cross-relation report statistic comprising two data sources,
 * a match type, an optional principle and an optional match key.
 */
public class CrossRelationKey implements Comparable<CrossRelationKey> {
  /**
   * The first data source.
   */
  private String source1;

  /**
   * The second data source.
   */
  private String source2;

  /**
   * The {@link SzMatchType} describing the associated match type.
   */
  private SzMatchType matchType;

  /**
   * The match key associated with the relationships.
   */
  private String matchKey;

  /**
   * The principle associated with the relationships.
   */
  private String principle;

  /**
   * Constructs with the specified data source codes and {@link SzMatchType}.
   *
   * @param source    The data source code representing both the "from" and
   *                  "to" data source.
   * @param matchType The {@link SzMatchType} describing the match type.
   * @param matchKey  The optional match key for the relationships.
   * @param principle The optional principle for the relationships.
   * @throws NullPointerException If any of the parameter is <code>null</code>
   */
  public CrossRelationKey(String source,
      SzMatchType matchType,
      String matchKey,
      String principle)
      throws NullPointerException {
    this(source, source, matchType, matchKey, principle);
  }

  /**
   * Constructs with the specified data source codes and {@link SzMatchType}.
   *
   * @param source1   The first ("from") data source code.
   * @param source2   The second ("to") data source code.
   * @param matchType The {@link SzMatchType} describing the match type.
   * @param matchKey  The match key for the relationships.
   * @param principle The principle for the relationships.
   * @throws NullPointerException If any of the parameter is <code>null</code>
   */
  public CrossRelationKey(String source1,
      String source2,
      SzMatchType matchType,
      String matchKey,
      String principle)
      throws NullPointerException {
    Objects.requireNonNull(source1, "The first data source cannot be null");
    Objects.requireNonNull(source2, "The second data source cannot be null");
    Objects.requireNonNull(matchType, "The match type cannot be null");

    this.source1 = source1;
    this.source2 = source2;
    this.matchType = matchType;
    this.matchKey = matchKey;
    this.principle = principle;
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
   * Gets the associated {@link SzMatchType}.
   *
   * @return The associated {@link SzMatchType}.
   */
  public SzMatchType getMatchType() {
    return this.matchType;
  }

  /**
   * Gets the match key associated with the relationships.
   * 
   * @return The match key associated with the relationships.
   */
  public String getMatchKey() {
    return this.matchKey;
  }

  /**
   * Gets the principle associated with the relationships.
   * 
   * @return The principle associated with the relationships.
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
    return Objects.hash(this.getSource1(),
        this.getSource2(),
        this.getMatchType(),
        this.getMatchKey(),
        this.getPrinciple());
  }

  /**
   * Checks if this instance is equal to the specified object. This is
   * implemented to return <code>true</code> if and only if the specified
   * value is a non-null reference to an object of the same class with
   * equivalent properties.
   *
   * @param obj The object to compare with.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (this == obj)
      return true;
    if (this.getClass() != obj.getClass())
      return false;
    CrossRelationKey key = (CrossRelationKey) obj;
    return Objects.equals(this.getSource1(), key.getSource1())
        && Objects.equals(this.getSource2(), key.getSource2())
        && Objects.equals(this.getMatchType(), key.getMatchType())
        && Objects.equals(this.getMatchKey(), key.getMatchKey())
        && Objects.equals(this.getPrinciple(), key.getPrinciple());
  }

  /**
   * Implemented to handle comparing such that we sort first on the associated
   * data sources, then on match type, then principles and then on match keys
   * with <code>null</code> values sorting less-than non-null values. If the
   * specified parameter is <code>null</code> then this returns one (1).
   * 
   * @param key The {@link CrossRelationKey} to compare with.
   * 
   * @return A negative number if this instance compares less-than the
   *         specified parameter, a positive number if it compares greater-than
   *         the specified parameter and zero (0) if they compare equal.
   */
  @Override
  public int compareTo(CrossRelationKey key) {
    if (key == null)
      return 1;
    if (key == this)
      return 0;

    // first compare the data sources which cannot be null
    int diff = this.getSource1().compareTo(key.getSource1());
    if (diff != 0)
      return diff;
    diff = this.getSource2().compareTo(key.getSource2());
    if (diff != 0)
      return diff;

    // now compare on match types
    SzMatchType matchType1 = this.getMatchType();
    SzMatchType matchType2 = key.getMatchType();
    if (matchType1 != matchType2) {
      if (matchType1 == null)
        return -1;
      if (matchType2 == null)
        return 1;
      return matchType1.compareTo(matchType2);
    }

    // now compare the match keys and principles.
    String matchKey1 = this.getMatchKey();
    String principle1 = this.getPrinciple();
    String matchKey2 = key.getMatchKey();
    String principle2 = key.getPrinciple();
    if (!Objects.equals(principle1, principle2)) {
      if (principle1 == null)
        return -1;
      if (principle2 == null)
        return 1;
      return principle1.compareTo(principle2);
    }
    if (Objects.equals(matchKey1, matchKey2))
      return 0;
    if (matchKey1 == null)
      return -1;
    if (matchKey2 == null)
      return 1;
    return matchKey1.compareTo(matchKey2);
  }

  /**
   * Implemented to provide a diagnostic {@link String} describing this
   * instance.
   *
   * @return A diagnostic {@link String} describing this instance.
   */
  @Override
  public String toString() {
    return this.getSource1() + ":" + this.getSource2() + "["
        + this.getMatchType() + ":" + this.getPrinciple()
        + ":" + this.getMatchKey() + "]";
  }
}
