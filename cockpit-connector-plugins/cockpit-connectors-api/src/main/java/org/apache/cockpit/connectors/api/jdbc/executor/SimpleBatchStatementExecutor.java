package org.apache.cockpit.connectors.api.jdbc.executor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@RequiredArgsConstructor
public class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull private final StatementFactory statementFactory;
    @NonNull private final TableSchema tableSchema;
    @Nullable private final TableSchema databaseTableSchema;
    @NonNull private final JdbcRowConverter converter;
    private transient PreparedStatement statement;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statement = statementFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        converter.toExternal(tableSchema, databaseTableSchema, record, statement);
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
