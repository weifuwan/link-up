package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.type.RowKind;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@AllArgsConstructor
@RequiredArgsConstructor
public class ReduceBufferedBatchStatementExecutor implements JdbcBatchStatementExecutor {
    @NonNull private final JdbcBatchStatementExecutor insertOrUpdateExecutor;
    @NonNull private final JdbcBatchStatementExecutor deleteExecutor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull private final Function<SeaTunnelRow, SeaTunnelRow> valueTransform;
    private boolean ignoreUpdateBefore;

    @NonNull private final LinkedHashMap<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> buffer =
            new LinkedHashMap<>();

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        insertOrUpdateExecutor.prepareStatements(connection);
        deleteExecutor.prepareStatements(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        if (RowKind.UPDATE_BEFORE.equals(record.getRowKind()) && ignoreUpdateBefore) {
            return;
        }

        SeaTunnelRow key = keyExtractor.apply(record);
        boolean changeFlag = changeFlag(record.getRowKind());
        SeaTunnelRow value = valueTransform.apply(record);
        buffer.put(key, Pair.of(changeFlag, value));
    }

    @Override
    public void executeBatch() throws SQLException {
        Boolean preChangeFlag = null;
        Set<Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>>> entrySet = buffer.entrySet();
        for (Map.Entry<SeaTunnelRow, Pair<Boolean, SeaTunnelRow>> entry : entrySet) {
            Boolean currentChangeFlag = entry.getValue().getKey();
            if (currentChangeFlag) {
                if (preChangeFlag != null && !preChangeFlag) {
                    deleteExecutor.executeBatch();
                }
                insertOrUpdateExecutor.addToBatch(entry.getValue().getValue());
            } else {
                if (preChangeFlag != null && preChangeFlag) {
                    insertOrUpdateExecutor.executeBatch();
                }
                deleteExecutor.addToBatch(entry.getKey());
            }
            preChangeFlag = currentChangeFlag;
        }

        if (preChangeFlag != null) {
            if (preChangeFlag) {
                insertOrUpdateExecutor.executeBatch();
            } else {
                deleteExecutor.executeBatch();
            }
        }
        buffer.clear();
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!buffer.isEmpty()) {
            executeBatch();
        }
        insertOrUpdateExecutor.closeStatements();
        deleteExecutor.closeStatements();
    }

    private boolean changeFlag(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                return true;
            case DELETE:
            case UPDATE_BEFORE:
                return false;
            default:
                throw new UnsupportedOperationException("Unsupported rowKind: " + rowKind);
        }
    }
}
