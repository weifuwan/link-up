package org.apache.cockpit.connectors.clickhouse.source;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseSourceConfig;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseTableConfig;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.sink.file.ClickhouseTable;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseUtil;
import org.apache.cockpit.connectors.clickhouse.util.TypeConvertUtil;
import org.apache.commons.lang3.StringUtils;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSourceOptions.*;


@AutoService(Factory.class)
public class ClickhouseSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        ClickhouseSourceConfig clickhouseSourceConfig =
                ClickhouseSourceConfig.of(context.getOptions());

        List<ClickhouseTableConfig> tableConfigs = clickhouseSourceConfig.getTableconfigList();

        Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables = new HashMap<>();
        Map<TablePath, List<ClickHouseNode>> nodesMap = new HashMap<>();

        for (ClickhouseTableConfig tableConfig : tableConfigs) {

            String sql = tableConfig.getSql();
            TablePath tablePath = tableConfig.getTableIdentifier();

            List<ClickHouseNode> nodes =
                    ClickhouseUtil.createNodes(
                            clickhouseSourceConfig.getHost(),
                            tablePath.getDatabaseName(),
                            clickhouseSourceConfig.getServerTimeZone(),
                            clickhouseSourceConfig.getUsername(),
                            clickhouseSourceConfig.getPassword(),
                            clickhouseSourceConfig.getClickhouseConfig());

            ClickHouseNode currentServer =
                    nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));

            try (ClickhouseProxy proxy = new ClickhouseProxy(currentServer);
                 ClickHouseResponse response =
                            proxy.getClickhouseConnection()
                                    .query(
                                            generateQuerySql(
                                                    sql,
                                                    tablePath.getDatabaseName(),
                                                    tablePath.getTableName()))
                                    .executeAndWait()) {

                TableSchema.Builder builder = TableSchema.builder();
                List<ClickHouseColumn> columns = response.getColumns();

                columns.forEach(
                        column -> {
                            PhysicalColumn physicalColumn =
                                    PhysicalColumn.of(
                                            column.getColumnName(),
                                            TypeConvertUtil.convert(column),
                                            (long) column.getEstimatedLength(),
                                            column.getScale(),
                                            column.isNullable(),
                                            null,
                                            null);
                            builder.column(physicalColumn);
                        });

                String catalogName = "clickhouse_catalog";

                CatalogTable catalogTable =
                        CatalogTable.of(
                                TableIdentifier.of(
                                        catalogName,
                                        tablePath.getDatabaseName(),
                                        tablePath.getTableName()),
                                builder.build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "",
                                catalogName);

                boolean isComplexSql =
                        StringUtils.isNotEmpty(sql)
                                && (tablePath == TablePath.DEFAULT || proxy.isComplexSql(sql));

                ClickhouseTable clickhouseTable =
                        isComplexSql
                                ? null
                                : proxy.getClickhouseTable(
                                        proxy.getClickhouseConnection(),
                                        tablePath.getDatabaseName(),
                                        tablePath.getTableName());

                ClickhouseSourceTable clickhouseSourceTable =
                        ClickhouseSourceTable.builder()
                                .tablePath(tablePath)
                                .clickhouseTable(clickhouseTable)
                                .originQuery(sql)
                                .filterQuery(tableConfig.getFilterQuery())
                                .splitSize(tableConfig.getSplitSize())
                                .batchSize(tableConfig.getBatchSize())
                                .partitionList(tableConfig.getPartitionList())
                                .isSqlStrategyRead(tableConfig.isSqlStrategyRead())
                                .isComplexSql(isComplexSql)
                                .catalogTable(catalogTable)
                                .build();

                clickhouseSourceTables.put(tablePath, clickhouseSourceTable);
                // The database may be different for each tableConfig
                // so create a separate nodes for each tablePath
                nodesMap.put(tablePath, nodes);

            } catch (ClickHouseException e) {
                throw new ClickhouseConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s",
                                factoryIdentifier(), PluginType.SOURCE, e.getMessage()));
            }
        }

        return () ->
                (SeaTunnelSource<T, SplitT>)
                        new ClickhouseSource(
                                nodesMap, clickhouseSourceTables, clickhouseSourceConfig);
    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }

    private String generateQuerySql(String sql, String database, String table) {
        if (StringUtils.isNotEmpty(sql)) {
            return modifySQLToLimit1(sql);
        }

        return String.format("SELECT * FROM %s.%s LIMIT 1", database, table);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, USERNAME, PASSWORD)
                .optional(
                        TABLE_PATH,
                        CLICKHOUSE_CONFIG,
                        SERVER_TIME_ZONE,
                        SQL,
                        CLICKHOUSE_SPLIT_SIZE,
                        CLICKHOUSE_PARTITION_LIST,
                        CLICKHOUSE_BATCH_SIZE,
                        CLICKHOUSE_FILTER_QUERY)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ClickhouseSource.class;
    }
}
