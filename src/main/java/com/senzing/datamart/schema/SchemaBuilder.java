package com.senzing.datamart.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.senzing.sql.SQLUtilities.close;

/**
 * Provides a base class for building the schema for the data mart replicator.
 */
public abstract class SchemaBuilder {
  /**
   * Default constructor.
   */
  protected SchemaBuilder() {
    // do nothing
  }

  /**
   * Ensures the schema exists and optionally drops the schema before
   * recreating it.
   *
   * @param conn The JDBC {@link Connection} to use for creating the schema.
   *
   * @param recreate <code>true</code> if the schema should be dropped and
   *                 recreated, or <code>false</code> if any existing schema
   *                 should be left in place.
   *
   * @throws SQLException If a JDBC failure occurs.
   *
   */
  public abstract void ensureSchema(Connection conn, boolean recreate)
    throws SQLException;

  /**
   * Utility method to execute a {@link List} of SQL statements.
   *
   * @param conn The {@link Connection} with which to execute the statements.
   * @param sqlList The {@link List} of SQL statements to execute.
   *
   * @throws SQLException If a JDBC failure occurs.
   */
  protected void executeStatements(Connection conn, List<String> sqlList)
      throws SQLException
  {
    Statement stmt = null;
    try {
      stmt = conn.createStatement();

      // execute the SQL statements
      for (String sql : sqlList) {
        try {
          stmt.execute(sql);
        } catch (SQLException e) {
          System.err.println();
          System.err.println(sql);
          e.printStackTrace();
          throw e;
        }
      }
    } finally {
      stmt = close(stmt);
    }
  }

}
