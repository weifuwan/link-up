package org.apache.cockpit.connectors.clickhouse.sink.file;

import com.clickhouse.client.ClickHouseNode;
import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.cockpit.connectors.clickhouse.config.FileReaderOption;
import org.apache.cockpit.connectors.clickhouse.config.NodePassConfig;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.shard.ShardMetadata;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseFileSinkOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions.*;


@AutoService(Factory.class)
public class ClickhouseFileSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "ClickhouseFile";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, TABLE, DATABASE, USERNAME, PASSWORD, CLICKHOUSE_LOCAL_PATH)
                .optional(
                        COPY_METHOD,
                        SHARDING_KEY,
                        NODE_FREE_PASSWORD,
                        NODE_PASS,
                        COMPATIBLE_MODE,
                        FILE_FIELDS_DELIMITER,
                        FILE_TEMP_PATH,
                        KEY_PATH,
                        SERVER_TIME_ZONE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();

        List<ClickHouseNode> nodes =
                ClickhouseUtil.createNodes(
                        readonlyConfig.get(HOST),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(SERVER_TIME_ZONE),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD),
                        readonlyConfig.get(CLICKHOUSE_CONFIG));

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema = proxy.getClickhouseTableSchema(readonlyConfig.get(TABLE));
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE));
        String shardKey = null;
        String shardKeyType = null;
        if (readonlyConfig.getOptional(SHARDING_KEY).isPresent()) {
            shardKey = readonlyConfig.getOptional(SHARDING_KEY).get();
            shardKeyType = tableSchema.get(shardKey);
        }

        ShardMetadata shardMetadata =
                new ShardMetadata(
                        shardKey,
                        shardKeyType,
                        readonlyConfig.get(DATABASE),
                        readonlyConfig.get(TABLE),
                        table.getEngine(),
                        true,
                        new Shard(1, 1, nodes.get(0)),
                        readonlyConfig.get(USERNAME),
                        readonlyConfig.get(PASSWORD));
        List<String> fields = new ArrayList<>(tableSchema.keySet());

        Map<String, String> nodeUser =
                readonlyConfig.toConfig().getObjectList(NODE_PASS.key()).stream()
                        .collect(
                                Collectors.toMap(
                                        configObject ->
                                                configObject.toConfig().getString(NODE_ADDRESS),
                                        configObject ->
                                                configObject.toConfig().hasPath(USERNAME.key())
                                                        ? configObject
                                                                .toConfig()
                                                                .getString(USERNAME.key())
                                                        : "root"));

        Map<String, String> nodePassword =
                readonlyConfig.get(NODE_PASS).stream()
                        .collect(
                                Collectors.toMap(
                                        NodePassConfig::getNodeAddress,
                                        NodePassConfig::getPassword));

        proxy.close();

        if (readonlyConfig.get(FILE_FIELDS_DELIMITER).length() != 1) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    FILE_FIELDS_DELIMITER.key() + " must be a single character");
        }
        FileReaderOption readerOption =
                new FileReaderOption(
                        shardMetadata,
                        tableSchema,
                        fields,
                        readonlyConfig.get(CLICKHOUSE_LOCAL_PATH),
                        ClickhouseFileCopyMethod.from(readonlyConfig.get(COPY_METHOD).getName()),
                        nodeUser,
                        readonlyConfig.get(NODE_FREE_PASSWORD),
                        nodePassword,
                        readonlyConfig.get(COMPATIBLE_MODE),
                        readonlyConfig.get(FILE_TEMP_PATH),
                        readonlyConfig.get(FILE_FIELDS_DELIMITER),
                        readonlyConfig.get(KEY_PATH));

        readerOption.setSeaTunnelRowType(catalogTable.getSeaTunnelRowType());
        return () -> new ClickhouseFileSink(readerOption);
    }
}
