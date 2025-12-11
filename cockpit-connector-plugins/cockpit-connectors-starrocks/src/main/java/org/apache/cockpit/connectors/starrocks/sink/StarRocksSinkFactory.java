package org.apache.cockpit.connectors.starrocks.sink;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TableIdentifier;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.starrocks.config.SinkConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksBaseOptions;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSinkOptions;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.cockpit.connectors.starrocks.config.StarRocksSinkOptions.DATA_SAVE_MODE;


@AutoService(Factory.class)
public class StarRocksSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(StarRocksSinkOptions.USERNAME, StarRocksSinkOptions.PASSWORD)
                .required(StarRocksSinkOptions.DATABASE, StarRocksSinkOptions.BASE_URL)
                .required(StarRocksSinkOptions.NODE_URLS)
                .optional(
                        StarRocksSinkOptions.TABLE,
                        StarRocksSinkOptions.LABEL_PREFIX,
                        StarRocksSinkOptions.BATCH_MAX_SIZE,
                        StarRocksSinkOptions.BATCH_MAX_BYTES,
                        StarRocksSinkOptions.MAX_RETRIES,
                        StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS,
                        StarRocksSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        StarRocksSinkOptions.STARROCKS_CONFIG,
                        StarRocksSinkOptions.ENABLE_UPSERT_DELETE,
                        StarRocksSinkOptions.SCHEMA_SAVE_MODE,
                        DATA_SAVE_MODE,
                        MULTI_TABLE_SINK_REPLICA,
                        StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE,
                        StarRocksSinkOptions.HTTP_SOCKET_TIMEOUT_MS)
                .conditional(
                        DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        StarRocksSinkOptions.CUSTOM_SQL)
                .build();
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Arrays.asList(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTable();
        SinkConfig sinkConfig = SinkConfig.of(context.getOptions());
        if (StringUtils.isBlank(sinkConfig.getTable())) {
            sinkConfig.setTable(catalogTable.getTableId().getTableName());
        }

        TableIdentifier rewriteTableId =
                TableIdentifier.of(
                        catalogTable.getTableId().getCatalogName(),
                        sinkConfig.getDatabase(),
                        null,
                        sinkConfig.getTable());
        CatalogTable finalCatalogTable =
                CatalogTable.of(
                        rewriteTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getComment());

        return () -> new StarRocksSink(sinkConfig, finalCatalogTable);
    }
}
