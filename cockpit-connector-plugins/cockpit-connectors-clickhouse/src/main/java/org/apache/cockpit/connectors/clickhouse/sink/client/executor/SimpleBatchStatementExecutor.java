package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@RequiredArgsConstructor
public class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor {
    @NonNull private final StatementFactory statementFactory;
    @NonNull private final JdbcRowConverter converter;
    private transient PreparedStatement statement;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statement = statementFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        converter.toExternal(record, statement);
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statement.executeBatch();
        statement.clearBatch();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }
}
