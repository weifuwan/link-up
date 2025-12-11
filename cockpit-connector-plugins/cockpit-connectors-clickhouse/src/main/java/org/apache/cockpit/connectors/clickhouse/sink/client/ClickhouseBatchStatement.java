package org.apache.cockpit.connectors.clickhouse.sink.client;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import org.apache.cockpit.connectors.clickhouse.sink.client.executor.JdbcBatchStatementExecutor;
import org.apache.cockpit.connectors.clickhouse.util.IntHolder;


public class ClickhouseBatchStatement {

    private final ClickHouseConnectionImpl clickHouseConnection;
    private final JdbcBatchStatementExecutor jdbcBatchStatementExecutor;
    private final IntHolder intHolder;

    public ClickhouseBatchStatement(
            ClickHouseConnectionImpl clickHouseConnection,
            JdbcBatchStatementExecutor jdbcBatchStatementExecutor,
            IntHolder intHolder) {
        this.clickHouseConnection = clickHouseConnection;
        this.jdbcBatchStatementExecutor = jdbcBatchStatementExecutor;
        this.intHolder = intHolder;
    }

    public ClickHouseConnectionImpl getClickHouseConnection() {
        return clickHouseConnection;
    }

    public JdbcBatchStatementExecutor getJdbcBatchStatementExecutor() {
        return jdbcBatchStatementExecutor;
    }

    public IntHolder getIntHolder() {
        return intHolder;
    }
}
