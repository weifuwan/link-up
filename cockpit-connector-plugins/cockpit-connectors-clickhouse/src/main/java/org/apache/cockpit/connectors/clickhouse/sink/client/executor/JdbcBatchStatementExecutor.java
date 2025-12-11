package org.apache.cockpit.connectors.clickhouse.sink.client.executor;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcBatchStatementExecutor extends AutoCloseable {

    void prepareStatements(Connection connection) throws SQLException;

    void addToBatch(SeaTunnelRow record) throws SQLException;

    void executeBatch() throws SQLException;

    void closeStatements() throws SQLException;

    @Override
    default void close() throws SQLException {
        closeStatements();
    }
}
