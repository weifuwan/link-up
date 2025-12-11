package org.apache.cockpit.connectors.api.jdbc.executor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
public class BufferedBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull
    private final JdbcBatchStatementExecutor<SeaTunnelRow> statementExecutor;
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
        try {
            if (!buffer.isEmpty()) {
                executeBatch();
            }
        } finally {
            statementExecutor.closeStatements();
        }
    }
}
