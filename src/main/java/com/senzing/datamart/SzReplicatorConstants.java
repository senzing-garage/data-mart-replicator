package com.senzing.datamart;

import java.time.Duration;

public final class SzReplicatorConstants {
    /**
     * The default module name ({@value}).
     */
    public static final String DEFAULT_INSTANCE_NAME = "senzing-datamart-replicator";

    /**
     * The default core concurrency setting used by API server
     * instances if an explicit core concurrency is not provided.
     */
    public static final int DEFAULT_CORE_CONCURRENCY
        = Runtime.getRuntime().availableProcessors();

    /**
     * The default core concurrency as a string.
     */
    static final String DEFAULT_CORE_CONCURRENCY_PARAM
        = String.valueOf(DEFAULT_CORE_CONCURRENCY);

    /**
     * The default number of seconds to wait in between checking for changes in the
     * configuration and automatically refreshing the configuration.
     */
    public static final long DEFAULT_REFRESH_CONFIG_SECONDS = (Duration.ofHours(12).toMillis()) / 1000;

    /**
     * The config auto refresh period as a string.
     */
    static final String DEFAULT_REFRESH_CONFIG_SECONDS_PARAM = String.valueOf(DEFAULT_REFRESH_CONFIG_SECONDS);

    /**
     * The prefix for environment variables used that are specific to the Senzing
     * REST API Server.
     */
    static final String ENV_PREFIX = "SENZING_TOOLS_";

}
