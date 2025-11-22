package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes a page of relations from the data mart.
 */
public class SzRelationsPage implements Serializable {
    /**
     * The relation bound value that bounds the included relations.
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
    private String pageMinimumValue = null;

    /**
     * The page-maximum value that has been set.
     */
    private String pageMaximumValue = null;

    /**
     * The total number of relations representing the set of all possible results
     * across all pages.
     */
    private long totalRelationCount = 0L;

    /**
     * The number of relations in the set that exist on pages before this page.
     */
    private long beforePageCount = 0L;

    /**
     * The number of relations in the set that exist on pages after this page.
     */
    private long afterPageCount = 0L;

    /**
     * The {@link Map} of {@link String} encoded relationship entity ID's keys
     * identifying the related entities to {@link SzReportRelation} values for those
     * entities.
     */
    private SortedMap<SzRelationKey, SzReportRelation> relations = null;

    /**
     * Default constructor
     */
    public SzRelationsPage() {
        this.bound = "0:0";
        this.boundType = null;
        this.pageSize = 0;
        this.sampleSize = null;
        this.pageMinimumValue = null;
        this.pageMaximumValue = null;
        this.totalRelationCount = 0L;
        this.beforePageCount = 0L;
        this.afterPageCount = 0L;
        this.relations = new TreeMap<>();
    }

    /**
     * Gets the relation bound value that bounds the included relations. The
     * relationship bound value contains two (2) entity ID values separated by a
     * colon (e.g.: <code>1000:5005</code>). The first entity ID value identifies
     * the first entity in the relationship and the second entity ID value
     * identifies the related entity in the relationship.
     *
     * @return The relations bound value that bounds the included relations.
     */
    public String getBound() {
        return this.bound;
    }

    /**
     * Sets the relation bound value that bounds the included relations. The
     * relationship bound value contains two (2) entity ID values separated by a
     * colon (e.g.: <code>1000:5005</code>). The first entity ID value identifies
     * the first entity in the relationship and the second entity ID value
     * identifies the related entity in the relationship.
     *
     * @param bound The encoded relations bound value that bounds the included
     *              relations.
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
     * Gets requested page size representing the maximum number of relations that
     * were requested to be included in the page.
     * 
     * @return The requested page size representing the maximum number of relations
     *         that were requested to be included in the page.
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * Sets requested page size representing the maximum number of relations that
     * were requested to be included in the page.
     * 
     * @param pageSize The requested page size representing the maximum number of
     *                 relations that were requested to be included in the page.
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Gets requested sample size representing the number of relations to be
     * randomly selected from the page of results.
     * 
     * @return The requested page size representing the number of relations to be
     *         randomly selected from the page of results.
     */
    @JsonInclude(NON_NULL)
    public Integer getSampleSize() {
        return this.sampleSize;
    }

    /**
     * Sets requested sample size representing the number of relations to be
     * randomly selected from the page of results.
     * 
     * @param sampleSize The requested sample size representing the number of
     *                   relations to be randomly selected from the page of results.
     */
    public void setSampleSize(Integer sampleSize) {
        this.sampleSize = sampleSize;
    }

    /**
     * Gets the minimum relation value of the returned results. This is encoded the
     * same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the least value of first entity in the relationship and
     * the second entity ID value identifies the least value of those entity ID's
     * related to the first entity. This returns <code>null</code> if there are no
     * results.
     * 
     * @return The minimum relation value of the returned results, or
     *         <code>null</code> if there are no results.
     */
    @JsonInclude(NON_NULL)
    public String getMinimumValue() {
        if (this.relations.size() == 0)
            return null;
        SzRelationKey key = this.relations.firstKey();
        return key.toString();
    }

    /**
     * Gets the maximum relation value of the returned results. This is encoded the
     * same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the greatest value of first entity in the relationship
     * and the second entity ID value identifies the greatest value of those entity
     * ID's related to the first entity. This returns <code>null</code> if there are
     * no results.
     * 
     * @return The maximum relation value of the returned results, or
     *         <code>null</code> if there are no results.
     */
    @JsonInclude(NON_NULL)
    public String getMaximumValue() {
        if (this.relations.size() == 0)
            return null;
        SzRelationKey key = this.relations.lastKey();
        return key.toString();
    }

    /**
     * Gets the minimum relation value of the entire relations page. This will be
     * the same as {@linkplain #getMinimumValue() minimum value} if
     * {@linkplain #getSampleSize() sample size} was not specified, however, if the
     * sample size was specified then this will be the minimum relation value of all
     * the candidate relations on the page that were used for random sample
     * selection even if that relation was not randomly selected. This is encoded
     * the same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the least value of first entity in the relationship and
     * the second entity ID value identifies the least value of those entity ID's
     * related to the first entity. This returns <code>null</code> if there are no
     * results.
     * 
     * @return The minimum relation value of the entire relations page, or
     *         <code>null</code> if there are no results.
     */
    @JsonInclude(NON_NULL)
    public String getPageMinimumValue() {
        if (this.getSampleSize() == null && this.pageMinimumValue == null) {
            return this.getMinimumValue();
        }
        return this.pageMinimumValue;
    }

    /**
     * Sets the minimum relation value of the entire relations page. This should be
     * the same as {@linkplain #getMinimumValue() minimum value} if
     * {@linkplain #getSampleSize() sample size} was not specified, however, if the
     * sample size was specified then this will be the minimum relation value of all
     * the candidate relations on the page that were used for random sample
     * selection even if that relation was not randomly selected. This is encoded
     * the same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the least value of the first entity in the relationship
     * and the second entity ID value identifies the least value of those entity
     * ID's related to the first entity. Set this to <code>null</code> if there are
     * no results.
     * 
     * @param minValue The minimum relation value of the entire relations page, or
     *                 <code>null</code> if there are no results.
     */
    public void setPageMinimumValue(String minValue) {
        this.pageMinimumValue = minValue;
    }

    /**
     * Gets the maximum relation value of the entire relations page. This will be
     * the same as {@linkplain #getMaximumValue() maximum value} if
     * {@linkplain #getSampleSize() sample size} was not specified, however, if the
     * sample size was specified then this will be the maximum relation value of all
     * the candidate relations on the page that were used for random sample
     * selection even if that relation was not randomly selected. This is encoded
     * the same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the greatest value of the first entity in the
     * relationship and the second entity ID value identifies the greatest value of
     * those entity ID's related to the first entity. This returns <code>null</code>
     * if there are no results.
     * 
     * @return The maximum relation value of the entire relations page, or
     *         <code>null</code> if there are no results.
     */
    @JsonInclude(NON_NULL)
    public String getPageMaximumValue() {
        if (this.getSampleSize() == null && this.pageMaximumValue == null) {
            return this.getMaximumValue();
        }
        return this.pageMaximumValue;
    }

    /**
     * Sets the maximum relation value of the entire relations page. This should be
     * the same as {@linkplain #getMaximumValue() maximum value} if
     * {@linkplain #getSampleSize() sample size} was not specified, however, if the
     * sample size was specified then this should be the maximum relation value of
     * all the candidate relations on the page that were used for random sample
     * selection even if that relation was not randomly selected. This is encoded
     * the same as the {@linkplain #getBound() bound} value with two (2) entity ID
     * values separated by a colon (e.g.: <code>1000:5005</code>). The first entity
     * ID value identifies the greatest value of the first entity in the
     * relationship and the second entity ID value identifies the greatest value of
     * those entity ID's related to the first entity. Set this to <code>null</code>
     * if there are no results.
     * 
     * @param maxValue The maximum relation value of the entire relations page, or
     *                 <code>null</code> if there are no results.
     */
    public void setPageMaximumValue(String maxValue) {
        this.pageMaximumValue = maxValue;
    }

    /**
     * Gets the total number of relations in the set representing the set of all
     * possible results across all pages.
     *
     * @return The the total number of relations in the set representing the set of
     *         all possible results across all pages.
     */
    public long getTotalRelationCount() {
        return this.totalRelationCount;
    }

    /**
     * Sets the total number of relations in the set representing the set of all
     * possible results across all pages.
     *
     * @param relationCount The the total number of relations in the set 
     *                      representing the set of all possible results across
     *                      all pages.
     */
    public void setTotalRelationCount(long relationCount) {
        this.totalRelationCount = relationCount;
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
     * Gets the {@link List} of {@link SzReportRelation} instances describing the
     * relationships for the page. The {@link SzReportRelation} instances will be in
     * ascending order of the first entity ID and then the second related entity ID.
     * 
     * @return The {@link List} of {@link SzReportRelation} instances describing the
     *         relationships for the page.
     */
    public List<SzReportRelation> getRelations() {
        List<SzReportRelation> relations = new ArrayList<>(this.relations.values());
        return relations;
    }

    /**
     * Sets the {@link List} of {@link SzReportRelation} instances describing the
     * relationships for the page. The {@link SzReportRelation} instances will be in
     * ascending order of the first entity ID and then the second related entity ID.
     * 
     * @param relations The {@link List} of {@link SzReportRelation} instances describing
     *                  the relationships for the page.
     */
    public void setRelations(Collection<SzReportRelation> relations) {
        this.relations.clear();
        if (relations != null) {
            relations.forEach(relation -> {
                if (relation != null) {
                    long entityId = relation.getEntity().getEntityId();
                    long relatedId = relation.getRelatedEntity().getEntityId();
                    SzRelationKey key = new SzRelationKey(entityId, relatedId);

                    this.relations.put(key, relation);
                }
            });
        }
    }

    /**
     * Adds the specified {@link SzReportRelation} to the list of relations for this page.
     * If an {@link SzReportRelation} is already included for the same entity ID and
     * related ID then the specified {@link SzReportRelation} replaces the already
     * existing one.
     * 
     * @param relation The {@link SzReportRelation} to add to the list of relations
     *                 entities for this page.
     */
    public void addRelation(SzReportRelation relation) {
        long entityId = relation.getEntity().getEntityId();
        long relatedId = relation.getRelatedEntity().getEntityId();
        SzRelationKey key = new SzRelationKey(entityId, relatedId);

        this.relations.put(key, relation);
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "bound=[ " + this.getBound() + " ], boundType=[ " + this.getBoundType() + " ], pageSize=[ "
                + this.getPageSize() + " ], sampleSize=[ " + this.getSampleSize() + " ], pageMinimumValue=[ "
                + this.getPageMinimumValue() + " ], pageMaximumValue=[ " + this.getPageMaximumValue()
                + " ], totalRelationCount=[ " + this.getTotalRelationCount() + " ], beforePageCount=[ "
                + this.getBeforePageCount() + " ], afterPageCount=[ " + this.getAfterPageCount() + " ], relations=[ "
                + this.getRelations() + " ]";
    }
}
