package com.senzing.datamart;

import java.util.Set;

public final class SzReplicatorConstants {
  /**
   * The default concurrency setting used by replicator instances if
   * an explicit concurrency is not provided.  The default value is {@value}.
   */
  public static final int DEFAULT_CONCURRENCY = 8;

  /**
   * The default concurrency as a string.
   */
  static final String DEFAULT_CONCURRENCY_PARAM
      = String.valueOf(DEFAULT_CONCURRENCY);

  /**
   * The default module name ({@value}).
   */
  public static final String DEFAULT_MODULE_NAME
      = "senzing-datamart-replicator";

  /**
   * The prefix for environment variables used that are specific to the
   * Senzing data-mart replicator.
   */
  static final String ENV_PREFIX = "SENZING_DATA_MART_";

  /**
   * The prefix for environment variables pertaining to the Senzing engine.
   */
  static final String ENGINE_ENV_PREFIX = "SENZING_ENGINE_";

}
