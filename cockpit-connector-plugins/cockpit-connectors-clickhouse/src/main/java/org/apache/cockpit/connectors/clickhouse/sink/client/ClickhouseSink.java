package org.apache.cockpit.connectors.clickhouse.sink.client;

import com.clickhouse.client.ClickHouseNode;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.clickhouse.catalog.ClickhouseCatalog;
import org.apache.cockpit.connectors.clickhouse.catalog.ClickhouseCatalogFactory;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions;
import org.apache.cockpit.connectors.clickhouse.config.ReaderOption;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.shard.ShardMetadata;
import org.apache.cockpit.connectors.clickhouse.sink.file.ClickhouseTable;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseUtil;

import java.io.IOException;
import java.util.*;

import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions.*;


public class ClickhouseSink
        implements SeaTunnelSink<SeaTunnelRow>,
        SupportSaveMode {

    private ReaderOption option;
    private CatalogTable catalogTable;

    private ReadonlyConfig readonlyConfig;

    public ClickhouseSink(CatalogTable catalogTable, ReadonlyConfig readonlyConfig) {
        this.catalogTable = catalogTable;
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public ClickhouseSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);
        Properties clickhouseProperties = new Properties();
        readonlyConfig
                .get(CLICKHOUSE_CONFIG)
                .forEach((key, value) -> clickhouseProperties.put(key, String.valueOf(value)));

        clickhouseProperties.put("user", readonlyConfig.get(USERNAME));
        clickhouseProperties.put("password", readonlyConfig.get(PASSWORD));
        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));

        Map<String, String> tableSchema = proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE));
        if (readonlyConfig.get(SPLIT_MODE)) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "split mode only support table which engine is "
                                + "'Distributed' engine at now");
            }
            if (readonlyConfig.getOptional(SHARDING_KEY).isPresent()) {
                shardKey = readonlyConfig.get(SHARDING_KEY);
                shardKeyType = tableSchema.get(shardKey);
            }
        }
        ShardMetadata metadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        table.getSortingKey(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE),
                        table.getEngine(),
                        readonlyConfig.get(SPLIT_MODE),
                        new Shard(1, 1, nodes.get(0)),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD));
        proxy.close();
        String[] primaryKeys = null;
        if (readonlyConfig.getOptional(PRIMARY_KEY).isPresent()) {
            String primaryKey = readonlyConfig.get(PRIMARY_KEY);
            if (primaryKey == null || primaryKey.trim().isEmpty()) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, "primary_key can not be empty");
            }
            if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
            }
            primaryKeys = primaryKey.replaceAll("\\s+", "").split(",");
        }
        boolean supportUpsert = readonlyConfig.get(SUPPORT_UPSERT);
        boolean allowExperimentalLightweightDelete =
                readonlyConfig.get(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE);

        ReaderOption option =
                ReaderOption.builder()
                        .shardMetadata(metadata)
                        .properties(clickhouseProperties)
                        .seaTunnelRowType(catalogTable.getSeaTunnelRowType())
                        .tableEngine(table.getEngine())
                        .tableSchema(tableSchema)
                        .bulkSize(readonlyConfig.get(BULK_SIZE))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .build();
        return new ClickhouseSinkWriter(option, context);
    }


    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        TablePath tablePath = TablePath.of(readonlyConfig.get(DATABASE), readonlyConfig.get(TABLE));
        ClickhouseCatalog clickhouseCatalog =
                new ClickhouseCatalog(readonlyConfig, ClickhouseCatalogFactory.IDENTIFIER);
        SchemaSaveMode schemaSaveMode = readonlyConfig.get(ClickhouseSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = readonlyConfig.get(ClickhouseSinkOptions.DATA_SAVE_MODE);
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        clickhouseCatalog,
                        tablePath,
                        catalogTable,
                        readonlyConfig.get(CUSTOM_SQL)));
    }
}
