package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Describes the source summary and all cross-summaries with 
 * that data source.
 */
public class SzSummaryStats implements Serializable {
  /**
   * The {@link SortedMap} of {@link String} "versus" data source code keys to
   * {@link SzSourceSummary} values describing the count statistics for that
   * data source.
   */
  private SortedMap<String, SzSourceSummary> sourceSummaries = null;

  /**
   * Default constructor.
   */
  public SzSummaryStats() {
    this.sourceSummaries = new TreeMap<>();
  }

  /**
   * Gets the {@link List} of {@link SzSourceSummary} instances describing
   * the summary statistics for all configured data sources.
   * 
   * @return The {@link List} of {@link SzSourceSummary} instances describing
   *         the summary statistics for all configured data sources.
   */
  public List<SzSourceSummary> getSourceSummaries() {
    return new ArrayList<>(this.sourceSummaries.values());
  }

  /**
   * Sets the {@link List} of {@link SzSourceSummary} instances describing
   * describing the summary statistics for all configured data sources.
   * 
   * @param summaries The {@link List} of {@link SzSourceSummary} instances
   *                  describing the summary statistics for all configured
   *                  data sources.
   */
  public void setSourceSummaries(Collection<SzSourceSummary> summaries) {
    this.sourceSummaries.clear();
    if (summaries != null) {
      summaries.forEach(summary -> {
        if (summary != null) {
          this.sourceSummaries.put(summary.getDataSource(), summary);
        }
      });
    }
  }

  /**
   * Adds the specified {@link SzSourceSummary} describing the summary 
   * statistics for a specific data source.  This will replace any existing
   * statistics for the same data source.
   * 
   * @param summary The {@link SzSourceSummary} describing the summary statistics
   *                for a specific data source.
   */
  public void addSourceSummary(SzSourceSummary summary) {
    if (summary == null) {
      return;
    }
    this.sourceSummaries.put(summary.getDataSource(), summary);
  }

  /**
   * Removes the {@link SzSourceSummary} associated with the specified
   * data source if any exists for that data source.
   * 
   * @param dataSourceCode The data source code for which to remove
   *                       the source summary.
   */
  public void removeSourceSummary(String dataSourceCode) {
    this.sourceSummaries.remove(dataSourceCode);
  }

  /**
   * Removes all source summary statistics for all the data sources.
   */
  public void removeAllSourceSummaries() {
    this.sourceSummaries.clear();
  }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "sourceSummaries=[ " + this.getSourceSummaries() + " ]";
    }

}
