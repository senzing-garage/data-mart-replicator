package com.senzing.datamart;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.senzing.listener.service.scheduling.AbstractSchedulingService;

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

    /**
     * The scheduling service options to use for this processing rate.
     */
    private Map<String, Long> schedulingOptions = null;

    /**
     * The replicator service options to use for this processing rate.
     */
    private Map<String, Long> replicatorOptions = null;

    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_DELAY_KEY
     * scheduling follow-up delay} for {@link #LEISURELY} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long LEISURELY_FOLLOW_UP_DELAY 
        = AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY * 5;
        
    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_TIMEOUT_KEY
     * scheduling follow-up timeout} for {@link #LEISURELY} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long LEISURELY_FOLLOW_UP_TIMEOUT 
        = AbstractSchedulingService.DEFAULT_FOLLOW_UP_TIMEOUT * 5;
    
    /**
     * The {@linkplain SzReplicatorService#REPORT_UPDATE_PERIOD_KEY
     * replicator report update period} for {@link #LEISURELY} 
     * processing rate.  Its value is <code>{@value}</code>.
     */
    private static final long LEISURELY_REPORT_UPDATE_PERIOD 
        = SzReplicatorService.DEFAULT_REPORT_UPDATE_PERIOD * 5;

    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_DELAY_KEY
     * scheduling follow-up delay} for {@link #STANDARD} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long STANDARD_FOLLOW_UP_DELAY 
        = AbstractSchedulingService.DEFAULT_FOLLOW_UP_DELAY;
 
    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_TIMEOUT_KEY
     * scheduling follow-up timeout} for {@link #STANDARD} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long STANDARD_FOLLOW_UP_TIMEOUT 
        = AbstractSchedulingService.DEFAULT_FOLLOW_UP_TIMEOUT;

    /**
     * The {@linkplain SzReplicatorService#REPORT_UPDATE_PERIOD_KEY
     * replicator report update period} for {@link #STANDARD} 
     * processing rate.  Its value is <code>{@value}</code>.
     */
    private static final long STANDARD_REPORT_UPDATE_PERIOD 
        = SzReplicatorService.DEFAULT_REPORT_UPDATE_PERIOD;

    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_DELAY_KEY
     * scheduling follow-up delay} for {@link #AGGRESSIVE} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long AGGRESSIVE_FOLLOW_UP_DELAY = 100L;
 
    /**
     * The {@linkplain AbstractSchedulingService#FOLLOW_UP_TIMEOUT_KEY
     * scheduling follow-up timeout} for {@link #AGGRESSIVE} processing
     * rate.  Its value is <code>{@value}</code>.
     */
    private static final long AGGRESSIVE_FOLLOW_UP_TIMEOUT = 300L;

    /**
     * The {@linkplain SzReplicatorService#REPORT_UPDATE_PERIOD_KEY
     * replicator report update period} for {@link #AGGRESSIVE} 
     * processing rate.  Its value is <code>{@value}</code>.
     */
    private static final long AGGRESSIVE_REPORT_UPDATE_PERIOD = 1L;

    static {
        ProcessingRate[] values = ProcessingRate.values();
        for (ProcessingRate value : values) {
            value.schedulingOptions = new LinkedHashMap<>();
            value.replicatorOptions = new LinkedHashMap<>();

            switch (value) {
                case LEISURELY:
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_DELAY_KEY,
                    LEISURELY_FOLLOW_UP_DELAY);
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY,
                    LEISURELY_FOLLOW_UP_TIMEOUT);
                value.replicatorOptions.put(
                    SzReplicatorService.REPORT_UPDATE_PERIOD_KEY,
                    LEISURELY_REPORT_UPDATE_PERIOD); 
                break;
                case STANDARD:
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_DELAY_KEY,
                    STANDARD_FOLLOW_UP_DELAY);
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY,
                    STANDARD_FOLLOW_UP_TIMEOUT);
                value.replicatorOptions.put(
                    SzReplicatorService.REPORT_UPDATE_PERIOD_KEY,
                    STANDARD_REPORT_UPDATE_PERIOD);
                break;
                case AGGRESSIVE:
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_DELAY_KEY,
                    AGGRESSIVE_FOLLOW_UP_DELAY);
                value.schedulingOptions.put(
                    AbstractSchedulingService.FOLLOW_UP_TIMEOUT_KEY,
                    AGGRESSIVE_FOLLOW_UP_TIMEOUT);
                value.replicatorOptions.put(
                    SzReplicatorService.REPORT_UPDATE_PERIOD_KEY,
                    AGGRESSIVE_REPORT_UPDATE_PERIOD);
                break;
                default:
                    throw new IllegalStateException(
                        "Unhandled value: " + value);
            }
            value.schedulingOptions 
                = Collections.unmodifiableMap(value.schedulingOptions);
            value.replicatorOptions
                = Collections.unmodifiableMap(value.replicatorOptions);
        }
    }

    /**
     * Merges the {@link AbstractSchedulingService} options from this
     * instance with the options described by the specified {@link 
     * JsonObject}.  This method will <b>not</b> replace existing
     * options specified in the specified {@link JsonObject}.
     * 
     * @param options The {@link JsonObject} describing the original
     *                options, or <code>null</code> if a new {@link 
     *                JsonObject} should be created.
     * 
     * @return The {@link JsonObject} describing the merged options.
     */
    public JsonObject mergeSchedulingServiceOptions(JsonObject options) {
        return this.mergeSchedulingServiceOptions(options, false);
    }

    /**
     * Merges the {@link AbstractSchedulingService} options from this
     * instance with the options described by the specified {@link 
     * JsonObject}.  This method will <b>optionally</b> replace existing
     * options specified in the specified {@link JsonObject} depending
     * on the specified <code>overwrite</code> parameter.
     * 
     * @param options The {@link JsonObject} describing the original
     *                options, or <code>null</code> if a new {@link 
     *                JsonObject} should be created.
     * 
     * @param overwrite <code>true</code> if existing options should be
     *                  overwritten, otherwise <code>false</code>.
     * 
     * @return The {@link JsonObject} describing the merged options.
     */
    public JsonObject mergeSchedulingServiceOptions(JsonObject  options,
                                                    boolean     overwrite)
    {
        JsonObjectBuilder job = (options == null) ? Json.createObjectBuilder()
            : Json.createObjectBuilder(options);

        this.schedulingOptions.forEach((key, value) -> {
            if (overwrite || options == null || !options.containsKey(key)) {
                job.add(key, value);
            }
        });

        return job.build();
    }

    /**
     * Adds the {@link AbstractSchedulingService} options from this
     * instance to the specified {@link JsonObjectBuilder}, overwriting
     * an options with the same value that may already exist.
     * 
     * @param builder The {@link JsonObjectBuilder} for building the
     *                options, or <code>null</code> if a new {@link 
     *                JsonObjectBuilder} should be created.
     * 
     * @return The {@link JsonObjectBuilder} that was modified.
     */
    public JsonObjectBuilder addSchedulingServiceOptions(JsonObjectBuilder builder) {
        if (builder == null) {
            builder = Json.createObjectBuilder();
        }

        final JsonObjectBuilder job = builder;

        this.schedulingOptions.forEach((key, value) -> {
            job.add(key, value);
        });

        return builder;
    }

    /**
     * Merges the {@link AbstractSchedulingService} options from this
     * instance with the options described by the specified {@link
     * JsonObject}.  This method will <b>not</b> replace existing
     * options specified in the specified {@link JsonObject}.
     * 
     * @param options The {@link JsonObject} describing the original
     *                options, or <code>null</code> if a new {@link 
     *                JsonObject} should be created.
     * 
     * @return The {@link JsonObject} describing the merged options.
     */
    public JsonObject mergeReplicatorServiceOptions(JsonObject options) {
        return this.mergeReplicatorServiceOptions(options, false);
    }

    /**
     * Merges the {@link SzReplicatorService} options from this instance 
     * with the options described by the specified {@link JsonObject}.
     * This method will <b>optionally</b> replace existing options
     * specified in the specified {@link JsonObject} depending on the
     * specified <code>overwrite</code> parameter.
     * 
     * @param options The {@link JsonObject} describing the original
     *                options, or <code>null</code> if a new {@link 
     *                JsonObject} should be created.
     * 
     * @param overwrite <code>true</code> if existing options should be
     *                  overwritten, otherwise <code>false</code>.
     * 
     * @return The {@link JsonObject} describing the merged options.
     */
    public JsonObject mergeReplicatorServiceOptions(JsonObject  options,
                                                    boolean     overwrite)
    {
        JsonObjectBuilder job = (options == null) ? Json.createObjectBuilder()
            : Json.createObjectBuilder(options);

        this.replicatorOptions.forEach((key, value) -> {
            if (overwrite || options == null || !options.containsKey(key)) {
                job.add(key, value);
            }
        });

        return job.build();
    }

    /**
     * Adds the {@link AbstractSchedulingService} options from this
     * instance to the specified {@link JsonObjectBuilder}, overwriting
     * an options with the same value that may already exist.
     * 
     * @param builder The {@link JsonObjectBuilder} for building the
     *                options, or <code>null</code> if a new {@link 
     *                JsonObjectBuilder} should be created.
     * 
     * @return The {@link JsonObjectBuilder} that was modified.
     */
    public JsonObjectBuilder addReplicatorServiceOptions(JsonObjectBuilder builder) {
        if (builder == null) {
            builder = Json.createObjectBuilder();
        }

        final JsonObjectBuilder job = builder;

        this.replicatorOptions.forEach((key, value) -> {
            job.add(key, value);
        });
        
        return builder;
    }


}
