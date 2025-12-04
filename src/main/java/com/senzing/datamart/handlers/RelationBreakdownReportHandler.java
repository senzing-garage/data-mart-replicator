package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;

import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;

/**
 * Handles updates to the entity size breakdown (ERB) report statistics.
 *
 * @see com.senzing.datamart.model.SzReportCode#ENTITY_RELATION_BREAKDOWN
 */
public class RelationBreakdownReportHandler extends UpdateReportHandler {
  /**
   * Constructs with the specified {@link SzReplicationProvider}.  This
   * constructs the super class with {@link
   * com.senzing.datamart.SzReplicationProvider.TaskAction#UPDATE_ENTITY_RELATION_BREAKDOWN} 
   * as the supported action.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   */
  public RelationBreakdownReportHandler(SzReplicationProvider provider) {
    super(provider, UPDATE_ENTITY_RELATION_BREAKDOWN);
  }
}
