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
import com.senzing.datamart.SzEntitiesPageParameters;
import com.senzing.datamart.DataMartTestExtension.RepositoryType;
import com.senzing.datamart.reports.model.SzBoundType;
import com.senzing.datamart.reports.model.SzEntitiesPage;
import com.senzing.datamart.reports.model.SzEntityRelationsBreakdown;
import com.senzing.datamart.reports.model.SzEntityRelationsCount;
import com.senzing.sql.ConnectionProvider;
import com.senzing.sql.SQLUtilities;

@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(DataMartTestExtension.class)
public class EntityRelationsReportsTest extends AbstractReportsTest {
    
    private Map<RepositoryType, SzEntityRelationsBreakdown> breakdownMap = null;

    @Override
    @BeforeAll
    public void setup() throws Exception {
        super.setup();
        this.breakdownMap = this.getEntityRelationsBreakdowns();
    }

    public List<Arguments> getBreakdownParameters() {
        List<Arguments> result = new ArrayList<>(this.breakdownMap.size());
        
        this.breakdownMap.forEach((repoType, breakdown) -> {
            result.add(Arguments.of(
                repoType, this.getConnectionProvider(repoType), breakdown));
        });

        return result;
    }

    @ParameterizedTest()
    @MethodSource("getBreakdownParameters")
    public void testEntityRelationsBreakdown(RepositoryType             repoType,
                                             ConnectionProvider         connProvider,
                                             SzEntityRelationsBreakdown expected)                                
    {
        String testInfo = "repoType=[ " + repoType + " ]";

        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntityRelationsBreakdown actual 
                = EntityRelationsReports.getEntityRelationsBreakdown(conn, null);

            validateReport(expected, actual, testInfo);
            
        } catch (Exception e) {
            fail("Failed test with an unexpected exception: repoType=[ "
                 + repoType + " ]", e);

        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getRelationsCountParameters() {
        List<Arguments> result = new LinkedList<>();
        
        this.breakdownMap.forEach((repoType, breakdown) -> {
            int greatestCount = breakdown.getEntityRelationsCounts().get(0).getRelationsCount();

            breakdown.getEntityRelationsCounts().forEach((relationCount) -> {
                result.add(Arguments.of(repoType, 
                                        this.getConnectionProvider(repoType),
                                        relationCount.getRelationsCount(),
                                        relationCount,
                                        null));
            });

            SzEntityRelationsCount zeroCount = new SzEntityRelationsCount();
            zeroCount.setRelationsCount(greatestCount + 1);
            zeroCount.setEntityCount(0);
            result.add(Arguments.of(repoType, 
                                    this.getConnectionProvider(repoType),
                                    zeroCount.getRelationsCount(),
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
    @MethodSource("getRelationsCountParameters")
    public void testEntityRelationsCount(RepositoryType         repoType,
                                         ConnectionProvider     connProvider,
                                         int                    relationsCount,
                                         SzEntityRelationsCount expected,
                                         Class<?>               exceptionType)
    {
        String testInfo = "repoType=[ " + repoType + " ], relationsCount=[ "
            + relationsCount + " ], expectedException=[ " + exceptionType + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntityRelationsCount actual 
                = EntityRelationsReports.getEntityRelationsCount(
                    conn, relationsCount, null);

            if (exceptionType != null) {
                fail("Method unexpectedly succeeded.  " + testInfo);
            }

            validateReport(expected, actual, testInfo);

        } catch (Exception e) {
            if ((exceptionType == null) || (!exceptionType.isInstance(e))) {
                    fail("Unexpected exception (" + e.getClass().getName() 
                         + ") when expecting " 
                         + (exceptionType == null ? "none" : exceptionType.getName())
                         + ": " + testInfo, e);
            }

        } finally {
            SQLUtilities.close(conn);
        }
    }

    public List<Arguments> getEntitiesForRelationCountParameters() {
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
            
            for (SzEntityRelationsCount relationCount : breakdown.getEntityRelationsCounts()) 
            {
                int relationsCount = relationCount.getRelationsCount();

                List<SzEntitiesPageParameters> params 
                    = this.generateEntitiesPageParameters(
                        repoType, (e) -> e.getRelatedEntities().size() == relationsCount);

                for (SzEntitiesPageParameters p : params) {
                    result.add(Arguments.of(repoType,
                                            this.getConnectionProvider(repoType),
                                            relationsCount,
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
    @MethodSource("getEntitiesForRelationCountParameters")
    public void testEntitiesForRelationCount(RepositoryType     repoType,
                                             ConnectionProvider connProvider,
                                             int                relationsCount,
                                             String             entityIdBound,
                                             SzBoundType        boundType,
                                             Integer            pageSize,
                                             Integer            sampleSize,
                                             SzEntitiesPage     expected,
                                             Class<?>           exceptionType)
    {                              
        String testInfo = "repoType=[ " + repoType + " ], relationsCount=[ "
            + relationsCount + " ], entityIdBound=[ " + entityIdBound
            + " ], boundType=[ " + boundType + " ], pageSize=[ " 
            + pageSize + " ], sampleSize=[ " + sampleSize + " ]";
        
        Connection conn = null;
        try {
            conn = connProvider.getConnection();
        
            SzEntitiesPage actual = EntityRelationsReports.getEntityIdsForRelationCount(
                conn,
                relationsCount,
                entityIdBound,
                boundType,
                pageSize,
                sampleSize,
                null);

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

        } finally {
            SQLUtilities.close(conn);
        }
    }
}
