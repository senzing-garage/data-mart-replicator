package com.senzing.datamart.reports.model;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * Describes a relationship between two entities.
 */
public class SzReportRelation implements Serializable {
    /**
     * The {@link SzReportEntity} describing the first entity in the relation.
     */
    private SzReportEntity entity = null;

    /**
     * The {@link SzReportEntity} describing the second entity in the relation.
     */
    private SzReportEntity relatedEntity = null;

    /**
     * The {@link SzRelationType} describing the relationship type for the relation.
     */
    private SzRelationType relationType = null;

    /**
     * The match key for the relation.
     */
    private String matchKey = null;

    /**
     * The principle for the relation.
     */
    private String principle = null;

    /**
     * Default constructor.
     */
    public SzReportRelation() {
        this.entity = null;
        this.relatedEntity = null;
        this.relationType = null;
        this.matchKey = null;
        this.principle = null;
    }

    /**
     * Gets the {@link SzReportEntity} describing the first entity in the relationship.
     * 
     * @return The {@link SzReportEntity} describing the first entity in the relationship.
     */
    public SzReportEntity getEntity() {
        return this.entity;
    }

    /**
     * Sets the {@link SzReportEntity} describing the first entity in the relationship.
     * 
     * @param entity The {@link SzReportEntity} describing the first entity in the
     *               relationship.
     */
    public void setEntity(SzReportEntity entity) {
        this.entity = entity;
    }

    /**
     * Gets the {@link SzReportEntity} describing the second entity in the relationship.
     * 
     * @return The {@link SzReportEntity} describing the second entity in the
     *         relationship.
     */
    public SzReportEntity getRelatedEntity() {
        return this.relatedEntity;
    }

    /**
     * Sets the {@link SzReportEntity} describing the second entity in the relationship.
     * 
     * @param related The {@link SzReportEntity} describing the second entity in the
     *                relationship.
     */
    public void setRelatedEntity(SzReportEntity related) {
        this.relatedEntity = related;
    }

    /**
     * Gets the {@link SzRelationType} describing the relationship type for the
     * relation.
     * 
     * @return The {@link SzRelationType} describing the relationship type for the
     *         relation.
     */
    @JsonInclude(NON_NULL)
    public SzRelationType getRelationType() {
        return this.relationType;
    }

    /**
     * Sets the {@link SzRelationType} describing the relationship type for the
     * relation.
     * 
     * @param relationType The {@link SzRelationType} describing the relationship
     *                     type for the relation.
     */
    public void setRelationType(SzRelationType relationType) {
        this.relationType = relationType;
    }

    /**
     * Gets the match key for the relation.
     * 
     * @return The match key for the relation.
     */
    @JsonInclude(NON_NULL)
    public String getMatchKey() {
        return this.matchKey;
    }

    /**
     * Sets the match key for the relation.
     * 
     * @param matchKey The match key for the relation.
     */
    public void setMatchKey(String matchKey) {
        this.matchKey = matchKey;
    }

    /**
     * Gets the principle for the relation.
     * 
     * @return The principle for the relation.
     */
    @JsonInclude(NON_NULL)
    public String getPrinciple() {
        return this.principle;
    }

    /**
     * Sets the principle for the relation.
     * 
     * @param principle The principle for the relation.
     */
    public void setPrinciple(String principle) {
        this.principle = principle;
    }

    /**
     * Overridden to return a diagnostic {@link String} describing this instance.
     * 
     * @return A diagnostic {@link String} describing this instance.
     */
    @Override
    public String toString() {
        return "entity=[ " + this.getEntity() 
                + " ], relatedEntity=[ " + this.getRelatedEntity()
                + " ], relationType=[ " + this.getRelationType()
                + " ], matchKey=[ " + this.getMatchKey()
                + " ], principle=[ " + this.getPrinciple() + " ]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity, relatedEntity, relationType, matchKey, principle);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SzReportRelation)) {
            return false;
        }
        SzReportRelation other = (SzReportRelation) obj;
        return Objects.equals(entity, other.entity) && Objects.equals(relatedEntity, other.relatedEntity)
                && relationType == other.relationType && Objects.equals(matchKey, other.matchKey)
                && Objects.equals(principle, other.principle);
    }
}
