package com.senzing.datamart.model;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import java.util.Objects;

import static com.senzing.io.IOUtilities.*;

/**
 * Describes a report key for updating a statistic.
 */
public class SzReportKey implements Serializable {
    /**
     * The report code.
     */
    private SzReportCode reportCode;

    /**
     * The report statistic.
     */
    private String statistic;

    /**
     * The first data source, or <code>null</code> if none.
     */
    private String dataSource1;

    /**
     * The second data source, or <code>null</code> if none.
     */
    private String dataSource2;

    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode The report code for the report.
     * @param statistic  The statistic for the report key.
     */
    public SzReportKey(SzReportCode reportCode, String statistic) {
        this(reportCode, statistic, null, null);
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode The report code for the report.
     * @param statistic  The statistic for the report key.
     */
    public SzReportKey(SzReportCode reportCode, Number statistic) {
        this(reportCode, 
             (statistic == null ? null : statistic.toString()), 
             null,
             null);
    }
    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode The report code for the report.
     * @param statistic  The statistic for the report key.
     * @param dataSource The data source, or <code>null</code> if no data source.
     */
    public SzReportKey(SzReportCode reportCode, String statistic, String dataSource) {
        this(reportCode, statistic, dataSource, null);
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode  The report code for the report.
     * @param statistic   The statistic for the report key.
     * @param dataSource1 The first data source, or <code>null</code> if no first
     *                    data source.
     * @param dataSource2 The second data source, or <code>null</code> if no second
     *                    data source or no first data source.
     */
    public SzReportKey(SzReportCode reportCode, String statistic, String dataSource1, String dataSource2) {
        if (dataSource1 == null && dataSource2 != null) {
            throw new IllegalArgumentException("A second data source cannot be specified if the first data source "
                    + "is null.  reportCode=[ " + reportCode + " ], statistic=[ " + statistic + "], dataSource1=[ "
                    + dataSource1 + " ], dataSource2=[ " + dataSource2 + " ]");
        }
        this.reportCode = reportCode;
        this.statistic = statistic;
        this.dataSource1 = dataSource1;
        this.dataSource2 = dataSource2;
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode The report code for the report.
     * @param statistic  The statistic for the report key.
     */
    public SzReportKey(SzReportCode reportCode, SzReportStatistic statistic) {
        this(reportCode, (statistic == null ? null : statistic.toString()));
    }

    /**
     * Constructs with the specified parameters.
     *
     * @param reportCode  The report code for the report.
     * @param statistic   The statistic for the report key.
     * @param dataSource1 The first data source, or <code>null</code> if no first
     *                    data source.
     * @param dataSource2 The second data source, or <code>null</code> if no second
     *                    data source.
     */
    public SzReportKey(SzReportCode reportCode, SzReportStatistic statistic, String dataSource1, String dataSource2) {
        this(reportCode, 
             (statistic == null ? null : statistic.toString()), 
             dataSource1, 
             dataSource2);
    }

    /**
     * Gets the report code.
     *
     * @return The report code.
     */
    public SzReportCode getReportCode() {
        return this.reportCode;
    }

    /**
     * Gets the statistic.
     *
     * @return The statistic.
     */
    public String getStatistic() {
        return this.statistic;
    }

    /**
     * Gets the first data source, if any. This returns <code>null</code> if there
     * is no first data source.
     *
     * @return The first data source.
     */
    public String getDataSource1() {
        return this.dataSource1;
    }

    /**
     * Gets the second data source, if any. This returns <code>null</code> if there
     * is no second data source.
     *
     * @return The second data source.
     */
    public String getDataSource2() {
        return this.dataSource2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SzReportKey that = (SzReportKey) o;
        return this.getReportCode().equals(that.getReportCode()) && this.getStatistic().equals(that.getStatistic())
                && Objects.equals(this.getDataSource1(), that.getDataSource1())
                && Objects.equals(this.getDataSource2(), that.getDataSource2());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getReportCode(), this.getStatistic(), this.getDataSource1(), this.getDataSource2());
    }

    @Override
    public String toString() {
        try {
            StringBuilder sb = new StringBuilder();

            String stat = URLEncoder.encode(this.getStatistic(), UTF_8);
            sb.append(this.getReportCode().getCode()).append(":").append(stat);

            if (this.getDataSource1() != null) {
                String src1 = URLEncoder.encode(this.getDataSource1(), UTF_8);
                sb.append(":").append(src1);

                if (this.getDataSource2() != null) {
                    String src2 = URLEncoder.encode(this.getDataSource2(), UTF_8);
                    sb.append(":").append(src2);
                }
            }
            return sb.toString();

        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding is not supported", e);
        }
    }

    /**
     * Parses the specified text as an encoded {@link SzReportKey} that has been
     * encoded via the {@link #toString()} implementation of this class.
     *
     * @param keyText The encoded text to parse.
     * @return The newly created {@link SzReportKey} decoded from the specified
     *         text.
     * @throws IllegalArgumentException If the specified text is not formatted as
     *                                  expected.
     */
    public static SzReportKey parse(String keyText) {
        try {
            String[] tokens = keyText.split(":");
            if (tokens.length < 2 || tokens.length > 4) {
                throw new IllegalArgumentException("The specified text is not an encoded report key: " + keyText);
            }
            SzReportCode code = SzReportCode.lookup(tokens[0]);
            String stat = URLDecoder.decode(tokens[1], UTF_8);

            if (tokens.length == 2)
                return new SzReportKey(code, stat);

            String src1 = URLDecoder.decode(tokens[2], UTF_8);

            if (tokens.length == 3)
                return new SzReportKey(code, stat, src1);

            String src2 = URLDecoder.decode(tokens[3], UTF_8);

            return new SzReportKey(code, stat, src1, src2);

        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 encoding is not supported", e);
        }
    }
}
