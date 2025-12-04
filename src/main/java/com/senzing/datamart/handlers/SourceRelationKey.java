package com.senzing.datamart.handlers;

import com.senzing.datamart.model.SzMatchType;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The key for categorizing relations from an entity to another entity or a data
 * source from which the related entities contain records.
 */
public class SourceRelationKey implements Comparable<SourceRelationKey> {
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
   * @param matchType The {@link SzMatchType} describing the match type.
   * @param matchKey  The match key for the relationships.
   * @param principle The principle for the relationships.
   * 
   * @throws NullPointerException If the specified {@link SzMatchType} is
   *                              <code>null</code>
   */
  public SourceRelationKey(SzMatchType matchType, String matchKey, String principle) throws NullPointerException {
    Objects.requireNonNull(matchType, "The match type cannot be null");

    this.matchType = matchType;
    this.matchKey = matchKey;
    this.principle = principle;
  }

  /**
   * Creates the variant {@link SourceRelationKey} instances for the specified
   * {@link SzMatchType} including one with no match key or principle, one with
   * only the match key and no principle, one with only the principle and not the
   * match key and one with both.
   * 
   * @param matchType The {@link SzMatchType} describing the match type.
   * @param matchKey  The match key for the relationships.
   * @param principle The principle for the relationships.
   * 
   * @return The {@link Set} of {@link SourceRelationKey} instances describing 
   *         the variant keys with the specified parameters.
   * 
   * @throws NullPointerException If the specified {@link SzMatchType} is
   *                              <code>null</code>
   */
  public static Set<SourceRelationKey> variants(SzMatchType matchType, String matchKey, String principle) throws NullPointerException {
    Set<SourceRelationKey> set = new TreeSet<>();
    set.add(new SourceRelationKey(matchType, null, null));
    set.add(new SourceRelationKey(matchType, matchKey, null));
    set.add(new SourceRelationKey(matchType, matchKey, principle));
    set.add(new SourceRelationKey(matchType, null, principle));
    return set;
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
    return Objects.hash(this.getMatchType(), this.getMatchKey(), this.getPrinciple());
  }

  /**
   * Compares this instance versus another instance of this class.
   * 
   * @param key The {@link SourceRelationKey} to compare with.
   * @return A negative number, zero (0) or a positive number depending on whether
   *         this instance is less-than, equal-to or greater-than the specified
   *         instance, respectively.
   */
  public int compareTo(SourceRelationKey key) {
    if (key == null) {
      return 1;
    }
    if (key == this) {
      return 0;
    }

    // compare the match types
    SzMatchType mt1 = this.getMatchType();
    SzMatchType mt2 = key.getMatchType();
    if (!Objects.equals(mt1, mt2)) {
      if (mt1 == null) {
        return -1;
      }
      if (mt2 == null) {
        return 1;
      }
      int diff = mt1.compareTo(mt2);
      if (diff != 0) {
        return diff;
      }
    }

    // compare the match keys
    String mk1 = this.getMatchKey();
    String mk2 = key.getMatchKey();
    if (!Objects.equals(mk1, mk2)) {
      if (mk1 == null) {
        return -1;
      }
      if (mk2 == null) {
        return 1;
      }
      int diff = mk1.compareTo(mk2);
      if (diff != 0) {
        return diff;
      }
    }

    // compare the principles
    String p1 = this.getPrinciple();
    String p2 = key.getPrinciple();
    if (!Objects.equals(p1, p2)) {
      if (p1 == null) {
        return -1;
      }
      if (p2 == null) {
        return 1;
      }
      int diff = p1.compareTo(p2);
      if (diff != 0) {
        return diff;
      }
    }

    // if we get here then return zero (0)
    return 0;
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
    SourceRelationKey key = (SourceRelationKey) obj;
    return (this.getMatchType() == key.getMatchType()) && Objects.equals(this.getMatchKey(), key.getMatchKey())
        && Objects.equals(this.getPrinciple(), key.getPrinciple());
  }

  /**
   * Implemented to provide a diagnostic {@link String} describing this instance.
   *
   * @return A diagnostic {@link String} describing this instance.
   */
  @Override
  public String toString() {
    return this.getMatchType() + ":" + this.getPrinciple() + ":" + this.getMatchKey();
  }
}
