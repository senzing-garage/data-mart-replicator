package com.senzing.datamart.reports;

import static com.senzing.datamart.reports.model.SzBoundType.INCLUSIVE_LOWER;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.senzing.datamart.DataMartTestExtension;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntitySizeBreakdown;
import com.senzing.datamart.reports.model.SzEntitySizeCount;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class EntitySizeReportsTest extends AbstractReportsTest {
    
    private Map<RepositoryType, SzEntitySizeBreakdown> breakdownMap = null;

    @Override
    @BeforeAll
    public void setup() throws Exception {
        super.setup();
        this.breakdownMap = this.getEntitySizeBreakdowns();
    }

    public List<Arguments> getBreakdownParameters() {
        List<Arguments> result = new ArrayList<>(this.breakdownMap.size());
        
        this.breakdownMap.forEach((repoType, breakdown) -> {
            result.add(Arguments.of(repoType, this.getConnectionProvider(repoType), breakdown));
        });

        return result;
    }

    @ParameterizedTest()
    @MethodSource("getBreakdownParameters")
    public void testEntitySizeBreakdown(RepositoryType          repoType,
                                        ConnectionProvider      connProvider,
                                        SzEntitySizeBreakdown   expected)
    {
        String testInfo = "repoType=[ " + repoType + " ]";

        try {
            SzEntitySizeBreakdown actual
                = this.getEntitySizeBreakdown(repoType, connProvider);

            validateReport(expected, actual, testInfo);

        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);
        }
    }

    /**
     * Gets the {@link SzEntitySizeBreakdown} for the specified repository type.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @return The {@link SzEntitySizeBreakdown} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitySizeBreakdown getEntitySizeBreakdown(RepositoryType     repoType,
                                                           ConnectionProvider connProvider)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return EntitySizeReports.getEntitySizeBreakdown(conn, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getSizeCountParameters() {
        List<Arguments> result = new LinkedList<>();
        
        this.breakdownMap.forEach((repoType, breakdown) -> {
            int greatestSize = breakdown.getEntitySizeCounts().get(0).getEntitySize();

            breakdown.getEntitySizeCounts().forEach((sizeCount) -> {
                result.add(Arguments.of(repoType, 
                                        this.getConnectionProvider(repoType),
                                        sizeCount.getEntitySize(),
                                        sizeCount,
                                        null));
            });

            SzEntitySizeCount zeroCount = new SzEntitySizeCount();
            zeroCount.setEntitySize(greatestSize + 1);
            zeroCount.setEntityCount(0);
            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    zeroCount.getEntitySize(),
                                    zeroCount,
                                    null));

            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    -1,
                                    null,
                                    IllegalArgumentException.class));
        });

        return result;
    }

    @ParameterizedTest()
    @MethodSource("getSizeCountParameters")
    public void testEntitySizeCount(RepositoryType      repoType,
                                    ConnectionProvider  connProvider,
                                    int                 entitySize,
                                    SzEntitySizeCount   expected,
                                    Class<?>            exceptionType)
    {
        String testInfo = "repoType=[ " + repoType + " ], entitySize=[ "
            + entitySize + " ], expectedException=[ " + exceptionType + " ]";

        try {
            SzEntitySizeCount actual
                = this.getEntitySizeCount(repoType, connProvider, entitySize);

            if (exceptionType != null) {
                fail("Method unexpectedly succeeded.  " + testInfo);
            }

            validateReport(expected, actual, testInfo);

        } catch (Exception e) {
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                fail("Unexpected exception (" + e.getClass().getName()
                     + ") when expecting "
                     + (exceptionType == null ? "none" : exceptionType.getName())
                     + ": repoType=[ " + repoType + " ], entitySize=[ "
                     + entitySize + " ]", e);
            }
        }
    }

    /**
     * Gets the {@link SzEntitySizeCount} for the specified entity size.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param entitySize The entity size for which the count is being requested.
     * @return The {@link SzEntitySizeCount} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitySizeCount getEntitySizeCount(RepositoryType     repoType,
                                                   ConnectionProvider connProvider,
                                                   int                entitySize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return EntitySizeReports.getEntitySizeCount(conn, entitySize, null);
        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getEntitiesForSizeParameters() {
        List<Arguments> result = new LinkedList<>();

        this.breakdownMap.forEach((repoType, breakdown) -> {
            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    -1,
                                    "0",
                                    INCLUSIVE_LOWER,
                                    5000,
                                    null,
                                    null,
                                    IllegalArgumentException.class));
            
            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    2,
                                    "0",
                                    INCLUSIVE_LOWER,
                                    -1,
                                    null,
                                    null,
                                    IllegalArgumentException.class));

            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    2,
                                    "0",
                                    INCLUSIVE_LOWER,
                                    5000,
                                    -10,
                                    null,
                                    IllegalArgumentException.class));

            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    2,
                                    "0",
                                    INCLUSIVE_LOWER,
                                    50,
                                    100,
                                    null,
                                    IllegalArgumentException.class));
            
            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    2,
                                    "NOT A NUMBER",
                                    INCLUSIVE_LOWER,
                                    500,
                                    null,
                                    null,
                                    IllegalArgumentException.class));
            
            for (SzEntitySizeCount sizeCount : breakdown.getEntitySizeCounts()) {
                int entitySize = sizeCount.getEntitySize();

                List<SzEntitiesPageParameters> params 
                    = this.generateEntitiesPageParameters(
                        repoType, (e) -> e.getRecords().size() == entitySize);

                for (SzEntitiesPageParameters p : params) {
                    result.add(Arguments.of(repoType,
                                            this.getConnectionProvider(repoType),
                                            entitySize,
                                            p.getEntityIdBound(),
                                            p.getBoundType(),
                                            p.getPageSize(),
                                            p.getSampleSize(),
                                            p.getExpectedPage(),
                                            null));
                }
            }
        });
        return result;
    }

    @ParameterizedTest
    @MethodSource("getEntitiesForSizeParameters")
    public void testEntitiesForSize(RepositoryType      repoType,
                                    ConnectionProvider  connProvider,
                                    int                 entitySize,
                                    String              entityIdBound,
                                    SzBoundType         boundType,
                                    Integer             pageSize,
                                    Integer             sampleSize,
                                    SzEntitiesPage      expected,
                                    Class<?>            exceptionType)
    {
        String testInfo = "repoType=[ " + repoType + " ], entitySize=[ "
            + entitySize + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ "
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";

        try {
            SzEntitiesPage actual = this.getEntityIdsForEntitySize(
                repoType,
                connProvider,
                entitySize,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize);

            if (exceptionType != null) {
                fail("Method unexpectedly succeeded.  " + testInfo);
            }

            this.validateEntitiesPage(repoType,
                                      testInfo,
                                      entityIdBound,
                                      boundType,
                                      pageSize,
                                      sampleSize,
                                      expected,
                                      actual);

        } catch (Exception e) {
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                fail("Unexpected exception (" + e.getClass().getName()
                     + ") when expecting "
                     + (exceptionType == null ? "none" : exceptionType.getName())
                     + ": " + testInfo, e);
            }
        }
    }

    /**
     * Gets the {@link SzEntitiesPage} for the specified entity size.
     * This method can be overridden by subclasses to obtain the result differently.
     *
     * @param repoType The {@link RepositoryType} for the test.
     * @param connProvider The {@link ConnectionProvider} to use.
     * @param entitySize The entity size for which entities are being requested.
     * @param entityIdBound The bound value for the entity ID's.
     * @param boundType The {@link SzBoundType} describing how to apply the bound.
     * @param pageSize The maximum number of entity ID's to return.
     * @param sampleSize The optional sample size.
     * @return The {@link SzEntitiesPage} result.
     * @throws Exception If an error occurs.
     */
    protected SzEntitiesPage getEntityIdsForEntitySize(RepositoryType     repoType,
                                                       ConnectionProvider connProvider,
                                                       int                entitySize,
                                                       String             entityIdBound,
                                                       SzBoundType        boundType,
                                                       Integer            pageSize,
                                                       Integer            sampleSize)
        throws Exception
    {
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
            return EntitySizeReports.getEntityIdsForEntitySize(
                conn,
                entitySize,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);
        } finally {
            SQLUtilities.close(conn);
        }
    }
}
