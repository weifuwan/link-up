package org.apache.cockpit.connectors.clickhouse.sink.client;

import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.common.Common;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.clickhouse.config.ReaderOption;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.sink.client.executor.JdbcBatchStatementExecutor;
import org.apache.cockpit.connectors.clickhouse.sink.client.executor.JdbcBatchStatementExecutorBuilder;
import org.apache.cockpit.connectors.clickhouse.state.CKCommitInfo;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.IntHolder;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public class ClickhouseSinkWriter
        implements SinkWriter<SeaTunnelRow> {

    private final Context context;
    private final ReaderOption option;
    private final ShardRouter shardRouter;
    private final transient ClickhouseProxy proxy;
    private final Map<Shard, ClickhouseBatchStatement> statementMap;

    ClickhouseSinkWriter(ReaderOption option, Context context) {
        this.option = option;
        this.context = context;

        this.proxy = new ClickhouseProxy(option.getShardMetadata().getDefaultShard().getNode());
        this.shardRouter = new ShardRouter(proxy, option.getShardMetadata());
        this.statementMap = initStatementMap();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        Object shardKey = null;
        if (StringUtils.isNotEmpty(this.option.getShardMetadata().getShardKey())) {
            int i =
                    this.option
                            .getSeaTunnelRowType()
                            .indexOf(this.option.getShardMetadata().getShardKey());
            shardKey = element.getField(i);
        }
        ClickhouseBatchStatement statement = statementMap.get(shardRouter.getShard(shardKey));
        JdbcBatchStatementExecutor clickHouseStatement = statement.getJdbcBatchStatementExecutor();
        IntHolder sizeHolder = statement.getIntHolder();
        // add into batch
        addIntoBatch(element, clickHouseStatement);
        sizeHolder.setValue(sizeHolder.getValue() + 1);
        // flush batch
        if (sizeHolder.getValue() >= option.getBulkSize()) {
            flush(clickHouseStatement);
            sizeHolder.setValue(0);
        }
    }

//    @Override
    public Optional<CKCommitInfo> prepareCommit() throws IOException {
        for (ClickhouseBatchStatement batchStatement : statementMap.values()) {
            JdbcBatchStatementExecutor statement = batchStatement.getJdbcBatchStatementExecutor();
            IntHolder intHolder = batchStatement.getIntHolder();
            if (intHolder.getValue() > 0) {
                flush(statement);
                intHolder.setValue(0);
            }
        }
        return Optional.empty();
    }

    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        this.proxy.close();
        flush();
    }

    private void addIntoBatch(SeaTunnelRow row, JdbcBatchStatementExecutor clickHouseStatement) {
        try {
            clickHouseStatement.addToBatch(row);
        } catch (SQLException e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "Add row data into batch error",
                    e);
        }
    }

    private void flush(JdbcBatchStatementExecutor clickHouseStatement) {
        try {
            clickHouseStatement.executeBatch();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Clickhouse execute batch statement error",
                    e);
        }
    }

    private void flush() {
        for (ClickhouseBatchStatement batchStatement : statementMap.values()) {
            try (ClickHouseConnectionImpl needClosedConnection =
                            batchStatement.getClickHouseConnection();
                    JdbcBatchStatementExecutor needClosedStatement =
                            batchStatement.getJdbcBatchStatementExecutor()) {
                IntHolder intHolder = batchStatement.getIntHolder();
                if (intHolder.getValue() > 0) {
                    flush(needClosedStatement);
                    intHolder.setValue(0);
                }
            } catch (SQLException e) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                        "Failed to close prepared statement.",
                        e);
            }
        }
    }

    private Map<Shard, ClickhouseBatchStatement> initStatementMap() {
        Map<Shard, ClickhouseBatchStatement> result = new HashMap<>(Common.COLLECTION_SIZE);
        shardRouter
                .getShards()
                .forEach(
                        (weight, s) -> {
                            try {
                                ClickHouseConnectionImpl clickhouseConnection =
                                        new ClickHouseConnectionImpl(
                                                s.getJdbcUrl(), this.option.getProperties());

                                String[] orderByKeys = null;
                                if (!Strings.isNullOrEmpty(shardRouter.getSortingKey())) {
                                    orderByKeys =
                                            Stream.of(shardRouter.getSortingKey().split(","))
                                                    .map(key -> StringUtils.trim(key))
                                                    .toArray(value -> new String[value]);
                                }
                                JdbcBatchStatementExecutor jdbcBatchStatementExecutor =
                                        new JdbcBatchStatementExecutorBuilder()
                                                .setTable(shardRouter.getShardTable())
                                                .setTableEngine(shardRouter.getShardTableEngine())
                                                .setRowType(option.getSeaTunnelRowType())
                                                .setPrimaryKeys(option.getPrimaryKeys())
                                                .setOrderByKeys(orderByKeys)
                                                .setClickhouseTableSchema(option.getTableSchema())
                                                .setAllowExperimentalLightweightDelete(
                                                        option
                                                                .isAllowExperimentalLightweightDelete())
                                                .setClickhouseServerEnableExperimentalLightweightDelete(
                                                        clickhouseServerEnableExperimentalLightweightDelete(
                                                                clickhouseConnection))
                                                .setSupportUpsert(option.isSupportUpsert())
                                                .build();
                                jdbcBatchStatementExecutor.prepareStatements(clickhouseConnection);
                                IntHolder intHolder = new IntHolder();
                                ClickhouseBatchStatement batchStatement =
                                        new ClickhouseBatchStatement(
                                                clickhouseConnection,
                                                jdbcBatchStatementExecutor,
                                                intHolder);
                                result.put(s, batchStatement);
                            } catch (SQLException e) {
                                throw new ClickhouseConnectorException(
                                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                                        "Clickhouse prepare statement error: " + e.getMessage(),
                                        e);
                            }
                        });
        return result;
    }

    private boolean clickhouseServerEnableExperimentalLightweightDelete(
            ClickHouseConnectionImpl clickhouseConnection) {
        if (!option.isAllowExperimentalLightweightDelete()) {
            return false;
        }
        String configKey = "allow_experimental_lightweight_delete";
        try (Statement stmt = clickhouseConnection.createStatement();
                ResultSet resultSet =
                        stmt.executeQuery("SHOW SETTINGS ILIKE '%" + configKey + "%'")) {
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                if (name.equalsIgnoreCase(configKey)) {
                    return resultSet.getBoolean("value");
                }
            }
            return false;
        } catch (SQLException e) {
            log.warn("Failed to get clickhouse server config: {}", configKey, e);
            return false;
        }
    }
}
