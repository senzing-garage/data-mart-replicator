package com.senzing.datamart.handlers;

import com.senzing.datamart.SzReplicationProvider;
import com.senzing.datamart.model.SzReportCode;

import static com.senzing.datamart.SzReplicationProvider.TaskAction;
import static com.senzing.datamart.SzReplicationProvider.TaskAction.*;

/**
 * Handles updates to the entity size breakdown (ESB) report statistics.
 *
 * @see SzReportCode#ENTITY_SIZE_BREAKDOWN
 */
public class SizeBreakdownReportHandler extends UpdateReportHandler {
  /**
   * Constructs with the specified {@link SzReplicationProvider}.  This
   * constructs the super class with {@link
   * TaskAction#UPDATE_ENTITY_SIZE_BREAKDOWN} as the supported action.
   *
   * @param provider The {@link SzReplicationProvider} to use.
   */
  public SizeBreakdownReportHandler(SzReplicationProvider provider) {
    super(provider, UPDATE_ENTITY_SIZE_BREAKDOWN);
  }
}
