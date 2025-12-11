package org.apache.cockpit.connectors.clickhouse.sink.client;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions;

import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions.*;

@AutoService(Factory.class)
public class ClickhouseSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Clickhouse";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new ClickhouseSink(catalogTable, readonlyConfig);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOST, DATABASE, TABLE, USERNAME, PASSWORD)
                .optional(
                        SERVER_TIME_ZONE,
                        CLICKHOUSE_CONFIG,
                        BULK_SIZE,
                        SPLIT_MODE,
                        SHARDING_KEY,
                        PRIMARY_KEY,
                        SUPPORT_UPSERT,
                        ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE,
                        SCHEMA_SAVE_MODE,
                        DATA_SAVE_MODE,
                        CUSTOM_SQL,
                        SAVE_MODE_CREATE_TEMPLATE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }
}
