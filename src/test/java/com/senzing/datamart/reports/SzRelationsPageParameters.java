package com.senzing.datamart.reports;

import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzRelationsPage;

/**
 * A simple container class for holding parameters used to test entity page retrieval.
 * This class is used as a data vehicle and does not implement {@link #equals(Object)}
 * or {@link #hashCode()} since instances are not compared for equality.
 */
public class SzRelationsPageParameters {
    /**
     * The repository type to use for the test.
     */
    private RepositoryType repoType;

    /**
     * The relation bound value for pagination.
     */
    private String relationBound;

    /**
     * The bound type indicating how the bound value should be applied.
     */
    private SzBoundType boundType;

    /**
     * The page size indicating the maximum number of entities to retrieve.
     */
    private Integer pageSize;

    /**
     * The sample size for random sampling of results, or {@code null} for no sampling.
     */
    private Integer sampleSize;

    /**
     * The expected page result for validation purposes.
     */
    private SzRelationsPage expectedPage;

    /**
     * Default constructor.
     */
    public SzRelationsPageParameters() {
        this.repoType = null;
        this.relationBound = null;
        this.boundType = null;
        this.pageSize = null;
        this.sampleSize = null;
        this.expectedPage = null;
    }

    /**
     * Gets the repository type to use for the test.
     *
     * @return The repository type to use for the test.
     */
    public RepositoryType getRepositoryType() {
        return this.repoType;
    }

    /**
     * Sets the repository type to use for the test.
     *
     * @param repoType The repository type to use for the test.
     */
    public void setRepositoryType(RepositoryType repoType) {
        this.repoType = repoType;
    }

    /**
     * Gets the entity ID bound value for pagination.
     *
     * @return The entity ID bound value for pagination.
     */
    public String getRelationBound() {
        return this.relationBound;
    }

    /**
     * Sets the entity ID bound value for pagination.
     *
     * @param entityIdBound The entity ID bound value for pagination.
     */
    public void setRelationBound(String entityIdBound) {
        this.relationBound = entityIdBound;
    }

    /**
     * Gets the bound type indicating how the bound value should be applied.
     *
     * @return The bound type indicating how the bound value should be applied.
     */
    public SzBoundType getBoundType() {
        return this.boundType;
    }

    /**
     * Sets the bound type indicating how the bound value should be applied.
     *
     * @param boundType The bound type indicating how the bound value should be applied.
     */
    public void setBoundType(SzBoundType boundType) {
        this.boundType = boundType;
    }

    /**
     * Gets the page size indicating the maximum number of entities to retrieve.
     *
     * @return The page size indicating the maximum number of entities to retrieve.
     */
    public Integer getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the page size indicating the maximum number of entities to retrieve.
     *
     * @param pageSize The page size indicating the maximum number of entities to retrieve.
     */
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Gets the sample size for random sampling of results.
     *
     * @return The sample size for random sampling of results, or {@code null} for no sampling.
     */
    public Integer getSampleSize() {
        return this.sampleSize;
    }

    /**
     * Sets the sample size for random sampling of results.
     *
     * @param sampleSize The sample size for random sampling of results, or {@code null}
     *                   for no sampling.
     */
    public void setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize;
    }

    /**
     * Gets the expected page result for validation purposes.
     *
     * @return The expected page result for validation purposes.
     */
    public SzRelationsPage getExpectedPage() {
        return this.expectedPage;
    }

    /**
     * Sets the expected page result for validation purposes.
     *
     * @param expectedPage The expected page result for validation purposes.
     */
    public void setExpectedPage(SzRelationsPage expectedPage) {
        this.expectedPage = expectedPage;
    }

    /**
     * Returns a diagnostic {@link String} describing this instance.
     * The expected page is not included in the output.
     *
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "SzRelationsPageParameters { "
            + "repoType=[ " + this.repoType
            + " ], entityIdBound=[ " + this.relationBound
            + " ], boundType=[ " + this.boundType
            + " ], pageSize=[ " + this.pageSize
            + " ], sampleSize=[ " + this.sampleSize
            + " ] }";
    }
}
