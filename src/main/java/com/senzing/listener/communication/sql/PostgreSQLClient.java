package com.senzing.listener.communication.sql;

import java.util.List;
import java.util.ArrayList;

import java.sql.Connection;
import java.sql.SQLException;

import com.senzing.sql.DatabaseType;

/**
 * Provides a PostgreSQL implementation of {@link SQLClient}.
 */
public class PostgreSQLClient implements SQLClient {
    /**
     * Default constructor.
     */
    public PostgreSQLClient() {
        // do nothing   
        
    }
    /**
     * {@inheritDoc}
     * <p>
     * Implemented to return {@link DatabaseType#POSTGRESQL}.
     * </p>
     */
    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRESQL;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implemented to create the schema for the PostgreSQL database.
     * </p>
     */
    @Override
    public void ensureSchema(Connection conn, boolean recreate) 
        throws SQLException 
    {
        String createTableSql = "CREATE TABLE IF NOT EXISTS sz_message_queue ("
            + "message_id BIGSERIAL PRIMARY KEY, "
            + "lease_id TEXT, "
            + "expire_lease_at TIMESTAMP, "
            + "message_text TEXT NOT NULL, "
            + "created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "modified_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);";

        String dropTableSql = "DROP TABLE IF EXISTS sz_message_queue;";

        String createIndexSql = "CREATE INDEX IF NOT EXISTS sz_msg_queue_lease "
            + "ON sz_message_queue (lease_id);";
        
        String dropIndexSql = "DROP INDEX IF EXISTS sz_msg_queue_lease;";

        String createTriggerFunctionSql =
            "CREATE OR REPLACE FUNCTION sz_msg_queue_timestamps() "
                + "RETURNS TRIGGER "
                + "LANGUAGE PLPGSQL "
                + "AS $$ "
                + "BEGIN "
                + "  IF (TG_OP = 'UPDATE') THEN "
                + "  BEGIN "
                + "    NEW.created_on := OLD.created_on; "
                + "    NEW.modified_on := CURRENT_TIMESTAMP; "
                + "    return NEW; "
                + "  END; "
                + "ELSIF (TG_OP = 'INSERT') THEN "
                + "  BEGIN "
                + "    NEW.created_on := CURRENT_TIMESTAMP; "
                + "    NEW.modified_on := CURRENT_TIMESTAMP; "
                + "    return NEW; "
                + "  END; "
                + "END IF; "
                + "RETURN NULL; "
                + "END; "
                + "$$;";
    
        String createTriggerSql =
            "CREATE TRIGGER sz_msg_queue_trigger "
                + "  BEFORE INSERT OR UPDATE "
                + "  ON sz_message_queue "
                + "  FOR EACH ROW "
                + "  WHEN (pg_trigger_depth() = 0) "
                + "  EXECUTE PROCEDURE sz_msg_queue_timestamps();";
    
        String dropTriggerFunctionSql =
            "DROP FUNCTION IF EXISTS sz_msg_queue_timestamps;";

        String dropTriggerSql =
            "DROP TRIGGER IF EXISTS sz_msg_queue_trigger "
                + "ON sz_message_queue;";


        List<String> sqlList = new ArrayList<>();

        if (recreate) {
            sqlList.add(dropTriggerSql);
            sqlList.add(dropTriggerFunctionSql);
            sqlList.add(dropIndexSql);
            sqlList.add(dropTableSql);
        }
        sqlList.add(createTableSql);
        sqlList.add(createIndexSql);
        sqlList.add(createTriggerFunctionSql);
        sqlList.add(dropTriggerSql);
        sqlList.add(createTriggerSql);

        // execute the statements
        this.executeSqlStatements(conn, sqlList);
    }
}
