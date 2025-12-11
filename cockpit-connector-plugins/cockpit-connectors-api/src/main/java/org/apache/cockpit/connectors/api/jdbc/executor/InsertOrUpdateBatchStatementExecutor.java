package org.apache.cockpit.connectors.api.jdbc.executor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

@RequiredArgsConstructor
public class InsertOrUpdateBatchStatementExecutor
        implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    private static final Logger LOG = LoggerFactory.getLogger(InsertOrUpdateBatchStatementExecutor.class);
    private final StatementFactory existStmtFactory;
    @NonNull
    private final StatementFactory insertStmtFactory;
    @NonNull
    private final StatementFactory updateStmtFactory;
    private final TableSchema keyTableSchema;
    private final Function<SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull
    private final TableSchema valueTableSchema;
    @Nullable
    private final TableSchema databaseTableSchema;
    @NonNull
    private final JdbcRowConverter rowConverter;
    private transient PreparedStatement existStatement;
    private transient PreparedStatement insertStatement;
    private transient PreparedStatement updateStatement;
    private transient Boolean preExistFlag;
    private transient boolean submitted;

    public InsertOrUpdateBatchStatementExecutor(
            StatementFactory insertStmtFactory,
            StatementFactory updateStmtFactory,
            TableSchema valueTableSchema,
            TableSchema databaseTableSchema,
            JdbcRowConverter rowConverter) {
        this(
                null,
                insertStmtFactory,
                updateStmtFactory,
                null,
                null,
                valueTableSchema,
                databaseTableSchema,
                rowConverter);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        if (upsertMode()) {
            existStatement = existStmtFactory.createStatement(connection);
        }
        insertStatement = insertStmtFactory.createStatement(connection);
        updateStatement = updateStmtFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        boolean exist = existRow(record);
        if (exist) {
            if (preExistFlag != null && !preExistFlag) {
                insertStatement.executeBatch();
                insertStatement.clearBatch();
            }
            rowConverter.toExternal(valueTableSchema, databaseTableSchema, record, updateStatement);
            updateStatement.addBatch();
        } else {
            if (preExistFlag != null && preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
            rowConverter.toExternal(valueTableSchema, databaseTableSchema, record, insertStatement);
            insertStatement.addBatch();
        }

        preExistFlag = exist;
        submitted = false;
    }

    @Override
    public void executeBatch() throws SQLException {
        long startTime = System.currentTimeMillis();
        if (preExistFlag != null) {
            if (preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            } else {
                int[] insertCounts = insertStatement.executeBatch();
                long endTime = System.currentTimeMillis();
                long elapsedTime = endTime - startTime;
                // 获取批处理的数量
                int batchSize = insertCounts.length;
                LOG.info("\nSink端：SQL执行统计 - 数量: {}, 耗时: {}ms",
                        batchSize, elapsedTime);

                insertStatement.clearBatch();
            }
        }
        submitted = true;

    }

    @Override
    public void closeStatements() throws SQLException {
        try {
            if (!submitted) {
                executeBatch();
            }
        } finally {
            for (PreparedStatement statement :
                    Arrays.asList(existStatement, insertStatement, updateStatement)) {
                if (statement != null) {
                    statement.close();
                }
            }
        }
    }

    private boolean upsertMode() {
        return existStmtFactory != null;
    }

    private boolean existRow(SeaTunnelRow record) throws SQLException {
        if (upsertMode()) {
            return exist(keyExtractor.apply(record));
        }
        switch (record.getRowKind()) {
            case INSERT:
                return false;
            case UPDATE_AFTER:
                return true;
            default:
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "unsupported row kind: " + record.getRowKind());
        }
    }

    private boolean exist(SeaTunnelRow pk) throws SQLException {
        rowConverter.toExternal(keyTableSchema, databaseTableSchema, pk, existStatement);
        try (ResultSet resultSet = existStatement.executeQuery()) {
            return resultSet.next();
        }
    }
}
