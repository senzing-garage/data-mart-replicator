package com.senzing.datamart;

import java.util.Map;

/**
 * Enumerates the processing rates for the replicator.
 */
public enum ProcessingRate {
    /**
     * The slowest rate of processing that favors batching large
     * groups of updates together to conserve system resources for
     * other processes or to allow the CPU to sleep between batches.
     */
    LEISURELY,

    /**
     * A balance between aggressive and leisurely processing that
     * processes updates in a timely manner while conserving 
     * system resources.  This is the default.
     */
    STANDARD,

    /**
     * A setting whereby processing is handled aggressively,
     * prioritizing frequent updates over conservation of 
     * system resources.
     */
    AGGRESSIVE;

    public Map<String, Long> getScedulingServiceOptions() {
        return null;
    }

    public Map<String, Long> getReplicatorServiceOptions() {
        return null;
    }
}
