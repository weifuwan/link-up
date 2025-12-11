package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
public class BufferedBatchStatementExecutor implements JdbcBatchStatementExecutor {
    @NonNull
    private final JdbcBatchStatementExecutor statementExecutor;
    @NonNull
    private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;
    @NonNull
    private final List<SeaTunnelRow> buffer = new ArrayList<>();

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        statementExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        buffer.add(valueTransform.apply(record));
    }

    @Override
    public void executeBatch() throws SQLException {
        if (!buffer.isEmpty()) {
            for (SeaTunnelRow row : buffer) {
                statementExecutor.addToBatch(row);
            }
            statementExecutor.executeBatch();
            buffer.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        statementExecutor.closeStatements();
    }
}
