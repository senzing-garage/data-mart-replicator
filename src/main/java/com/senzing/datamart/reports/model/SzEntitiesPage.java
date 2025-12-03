package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes a page of entities from the data mart.
 */
public class SzEntitiesPage implements Serializable {
    /**
     * The entity ID bound value that bounds the included entity ID's.
     */
    private String bound = null;

    /**
     * The {@link SzBoundType} that describes how the bound value was applied.
     */
    private SzBoundType boundType = null;

    /**
     * The requested page size representing the maximum number of entity ID's that
     * were requested to be included in the page.
     */
    private int pageSize = 0;

    /**
     * The requested sample size representing the number of entity ID's to be
     * randomly selected from the page of results.
     */
    private Integer sampleSize = null;

    /**
     * The page-minimum value that has been set.
     */
    private Long pageMinimumValue = null;

    /**
     * The page-maximum value that has been set.
     */
    private Long pageMaximumValue = null;

    /**
     * The total number of entities representing the set of all possible results
     * across all pages.
     */
    private long totalEntityCount = 0L;

    /**
     * The number of entities in the set that exist on pages before this page.
     */
    private long beforePageCount = 0L;

    /**
     * The number of entities in the set that exist on pages after this page.
     */
    private long afterPageCount = 0L;

    /**
     * The {@link Set} of {@link Long} entity ID's identifying the entities on this
     * page.
     */
    private SortedMap<Long, SzReportEntity> entities = null;

    /**
     * Default constructor
     */
    public SzEntitiesPage() {
        this.bound = null;
        this.boundType = null;
        this.pageSize = 0;
        this.sampleSize = null;
        this.pageMinimumValue = null;
        this.pageMaximumValue = null;
        this.totalEntityCount = 0L;
        this.beforePageCount = 0L;
        this.afterPageCount = 0L;
        this.entities = new TreeMap<>();
    }

    /**
     * Gets the entity ID bound value that bounds the included entity ID's. This
     * will return an integer {@link String} or <code>"max"</code> to indicate the
     * maximum legal value for an entity ID.
     *
     * @return The entity ID bound value that bounds the included entity ID's.
     */
    public String getBound() {
        return this.bound;
    }

    /**
     * Sets the entity ID bound value that bounds the included entity ID's. Set to
     * an integer {@link String} or <code>"max"</code> to indicate the maximum legal
     * value for an entity ID.
     *
     * @param bound The entity ID bound value that bounds the included entity ID's.
     */
    public void setBound(String bound) {
        this.bound = bound;
    }

    /**
     * Gets the {@link SzBoundType} that describes how the associated
     * {@linkplain #getBound() bound value} was applied.
     *
     * @return The the {@link SzBoundType} that describes how the associated
     *         {@linkplain #getBound() bound value} was applied.
     */
    public SzBoundType getBoundType() {
        return this.boundType;
    }

    /**
     * Sets the {@link SzBoundType} that describes how the associated
     * {@linkplain #getBound() bound value} was applied.
     *
     * @param boundType The the {@link SzBoundType} that describes how the
     *                  associated {@linkplain #getBound() bound value} was applied.
     */
    public void setBoundType(SzBoundType boundType) {
        this.boundType = boundType;
    }

    /**
     * Gets the requested page size representing the maximum number of entity ID's
     * that were requested to be included in the page.
     * 
     * @return The requested page size representing the maximum number of entity
     *         ID's that were requested to be included in the page.
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets the requested page size representing the maximum number of entity ID's
     * that were requested to be included in the page.
     * 
     * @param pageSize The requested page size representing the maximum number of
     *                 entity ID's that were requested to be included in the page.
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Gets the requested sample size representing the number of entity ID's to be
     * randomly selected from the page of results.
     * 
     * @return The requested page size representing the number of entity ID's to be
     *         randomly selected from the page of results.
     */
    @JsonInclude(NON_NULL)
    public Integer getSampleSize() {
        return this.sampleSize;
    }

    /**
     * Sets the requested sample size representing the number of entity ID's to be
     * randomly selected from the page of results.
     * 
     * @param sampleSize The requested sample size representing the number of entity
     *                   ID's to be randomly selected from the page of results.
     */
    public void setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize;
    }

    /**
     * Gets the minimum entity ID of the returned results. This returns
     * <code>null</code> if there are no results.
     * 
     * @return The minimum entity ID of the returned results, or <code>null</code>
     *         if there are no results.
     */
    @JsonInclude(NON_NULL)
    public Long getMinimumValue() {
        if (this.entities.size() == 0)
            return null;
        return this.entities.firstKey();
    }

    /**
     * Gets the maximum entity ID of the returned results. This returns
     * <code>null</code> if there are no results.
     * 
     * @return The maximum entity ID of the returned results, or <code>null</code>
     *         if there are no results.
     */
    @JsonInclude(NON_NULL)
    public Long getMaximumValue() {
        if (this.entities.size() == 0)
            return null;
        return this.entities.lastKey();
    }

    /**
     * Gets the minimum entity ID of the entire entity page. This will be the same
     * as the {@linkplain #getMinimumValue() minimum value} the
     * {@linkplain #getSampleSize() sample size} was not specified, however, if
     * sample size was specified then this will be the minimum entity ID value of
     * all the candidate entities on the page that were used for random sample
     * selection even if that entity was not randomly selected. This returns
     * <code>null</code> if there are no results.
     * 
     * @return The minimum entity ID of the entire entity page, or <code>null</code>
     *         if there are no results.
     */
    @JsonInclude(NON_NULL)
    public Long getPageMinimumValue() {
        if (this.getSampleSize() == null && this.pageMinimumValue == null) {
            return this.getMinimumValue();
        }
        return this.pageMinimumValue;
    }

    /**
     * Sets the minimum entity ID of the entire entity page. This will be the same
     * as the {@linkplain #getMinimumValue() minimum value} the
     * {@linkplain #getSampleSize() sample size} was not specified, however, if
     * sample size was specified then this will be the minimum entity ID value of
     * all the candidate entities on the page that were used for random sample
     * selection even if that entity was not randomly selected. Set this to
     * <code>null</code> if there are no results.
     * 
     * @param minValue The minimum entity ID of the entire entity page, or
     *                 <code>null</code> if there are no results.
     */
    public void setPageMinimumValue(Long minValue) {
        this.pageMinimumValue = minValue;
    }

    /**
     * Gets the maximum entity ID of the entire entity page. This will be the same
     * as the {@linkplain #getMaximumValue() maximum value} the
     * {@linkplain #getSampleSize() sample size} was not specified, however, if
     * sample size was specified then this will be the maximum entity ID value of
     * all the candidate entities on the page that were used for random sample
     * selection even if that entity was not randomly selected. This returns
     * <code>null</code> if there are no results.
     * 
     * @return The maximum entity ID of the entire entity page, or <code>null</code>
     *         if there are no results.
     */
    @JsonInclude(NON_NULL)
    public Long getPageMaximumValue() {
        if (this.getSampleSize() == null && this.pageMaximumValue == null) {
            return this.getMaximumValue();
        }
        return this.pageMaximumValue;
    }

    /**
     * Sets the maximum entity ID of the entire entity page. This will be the same
     * as the {@linkplain #getMaximumValue() maximum value} the
     * {@linkplain #getSampleSize() sample size} was not specified, however, if
     * sample size was specified then this will be the maximum entity ID value of
     * all the candidate entities on the page that were used for random sample
     * selection even if that entity was not randomly selected. Set this to
     * <code>null</code> if there are no results.
     * 
     * @param maxValue The maximum entity ID of the entire entity page, or
     *                 <code>null</code> if there are no results.
     */
    public void setPageMaximumValue(Long maxValue) {
        this.pageMaximumValue = maxValue;
    }

    /**
     * Gets the total number of entities in the set representing the set of all
     * possible results across all pages.
     *
     * @return The the total number of entities in the set representing the set of
     *         all possible results across all pages.
     */
    public long getTotalEntityCount() {
        return this.totalEntityCount;
    }

    /**
     * Sets the total number of entities in the set representing the set of all
     * possible results across all pages.
     *
     * @param entityCount The the total number of entities in the set representing
     *                    the set of all possible results across all pages.
     */
    public void setTotalEntityCount(long entityCount) {
        this.totalEntityCount = entityCount;
    }

    /**
     * Gets the number of entities in the set that exist on pages that occur before
     * this page.
     * 
     * @return The the number of entities in the set that exist on pages that occur
     *         before this page.
     */
    public long getBeforePageCount() {
        return this.beforePageCount;
    }

    /**
     * Sets the number of entities in the set that exist on pages that occur before
     * this page.
     * 
     * @param entityCount The the number of entities in the set that exist on pages
     *                    that occur before this page.
     */
    public void setBeforePageCount(long entityCount) {
        this.beforePageCount = entityCount;
    }

    /**
     * Gets the number of entities in the set that exist on pages that occur after
     * this page.
     * 
     * @return The the number of entities in the set that exist on pages that occur
     *         after this page.
     */
    public long getAfterPageCount() {
        return this.afterPageCount;
    }

    /**
     * Sets the number of entities in the set that exist on pages that occur after
     * this page.
     * 
     * @param entityCount The the number of entities in the set that exist on pages
     *                    that occur after this page.
     */
    public void setAfterPageCount(long entityCount) {
        this.afterPageCount = entityCount;
    }

    /**
     * Gets the {@link List} of {@link SzReportEntity} instances describing the entities
     * in this page of results. The returned {@link List} will be in ascending order
     * of entity ID.
     * 
     * @return The {@link List} of {@link SzReportEntity} instances describing the
     *         entities in this page of results (in ascending order).
     */
    public List<SzReportEntity> getEntities() {
        return new ArrayList<>(this.entities.values());
    }

    /**
     * Sets the {@link List} of {@link SzReportEntity} instances describing the entities
     * in this page of results. The entities will be sorted in ascending order of
     * entity ID and deduplicated when added with entities with duplicate entity
     * ID's being replaced by the one that occurs last in the specified
     * {@link Collection}.
     * 
     * @param entities The {@link List} of {@link SzReportEntity} instances describing
     *                 the entities in this page of results.
     */
    public void setEntities(Collection<SzReportEntity> entities) {
        this.entities.clear();
        if (entities != null) {
            entities.forEach(entity -> {
                if (entity != null) {
                    this.entities.put(entity.getEntityId(), entity);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzReportEntity} to the list of entities for this page. If
     * an entity with the same entity ID already exists on the page then the
     * specified one replaces the existing one.
     * 
     * @param entity The {@link SzReportEntity} to add the page of entities.
     */
    public void addEntity(SzReportEntity entity) {
        this.entities.put(entity.getEntityId(), entity);
    }

    /**
     * Removes the entity with the specified entity ID from the list of entities
     * associated with this instance. If no entity has the specified entity ID then
     * this method has no effect.
     * 
     * @param entityId The entity ID of the entity to be removed.
     */
    public void removeEntity(long entityId) {
        this.entities.remove(entityId);
    }

    /**
     * Removes all entities from the page of entities.
     */
    public void removeAllEntities() {
        this.entities.clear();
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "bound=[ " + this.getBound() 
                + " ], boundType=[ " + this.getBoundType()
                + " ], pageSize=[ " + this.getPageSize()
                + " ], sampleSize=[ " + this.getSampleSize()
                + " ], pageMinimumValue=[ " + this.getPageMinimumValue()
                + " ], pageMaximumValue=[ " + this.getPageMaximumValue()
                + " ], totalEntityCount=[ " + this.getTotalEntityCount()
                + " ], beforePageCount=[ " + this.getBeforePageCount()
                + " ], afterPageCount=[ " + this.getAfterPageCount()
                + " ], entities=[ " + this.getEntities() + " ]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(bound, boundType, pageSize, sampleSize, pageMinimumValue, pageMaximumValue,
                totalEntityCount, beforePageCount, afterPageCount, entities);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzEntitiesPage)) {
            return false;
        }
        SzEntitiesPage other = (SzEntitiesPage) obj;
        return Objects.equals(bound, other.bound) && boundType == other.boundType && pageSize == other.pageSize
                && Objects.equals(sampleSize, other.sampleSize)
                && Objects.equals(pageMinimumValue, other.pageMinimumValue)
                && Objects.equals(pageMaximumValue, other.pageMaximumValue)
                && totalEntityCount == other.totalEntityCount && beforePageCount == other.beforePageCount
                && afterPageCount == other.afterPageCount && Objects.equals(entities, other.entities);
    }
}
