package org.apache.cockpit.connectors.doris.sink;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TableIdentifier;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.doris.config.DorisSinkOptions;
import org.apache.cockpit.connectors.doris.util.UnsupportedTypeConverterUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static org.apache.cockpit.connectors.doris.config.DorisBaseOptions.DATABASE;
import static org.apache.cockpit.connectors.doris.config.DorisBaseOptions.TABLE;
import static org.apache.cockpit.connectors.doris.config.DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING;


@AutoService(Factory.class)
public class DorisSinkFactory implements TableSinkFactory {


    @Override
    public String factoryIdentifier() {
        return DbType.DORIS.getCode();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSinkOptions.FENODES,
                        DorisSinkOptions.USERNAME,
                        DorisSinkOptions.PASSWORD,
                        DorisSinkOptions.SINK_LABEL_PREFIX,
                        DorisSinkOptions.DORIS_SINK_CONFIG_PREFIX,
                        DorisSinkOptions.DATA_SAVE_MODE,
                        DorisSinkOptions.SCHEMA_SAVE_MODE)
                .optional(
                        DATABASE,
                        TABLE,
                        DorisSinkOptions.TABLE_IDENTIFIER,
                        DorisSinkOptions.QUERY_PORT,
                        DorisSinkOptions.DORIS_BATCH_SIZE,
                        DorisSinkOptions.SINK_ENABLE_2PC,
                        DorisSinkOptions.SINK_ENABLE_DELETE,
                        DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE,
                        NEEDS_UNSUPPORTED_TYPE_CASTING,
                        DorisSinkOptions.SINK_CHECK_INTERVAL,
                        DorisSinkOptions.SINK_MAX_RETRIES,
                        DorisSinkOptions.SINK_BUFFER_SIZE,
                        DorisSinkOptions.SINK_BUFFER_COUNT,
                        DorisSinkOptions.DEFAULT_DATABASE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        DorisSinkOptions.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        DorisSinkOptions.CUSTOM_SQL)
                .build();
    }

    @Override
    public List<String> excludeTablePlaceholderReplaceKeys() {
        return Arrays.asList(DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
    }

    @Override
    public TableSink<SeaTunnelRow> createSink(
            TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable =
                config.get(NEEDS_UNSUPPORTED_TYPE_CASTING)
                        ? UnsupportedTypeConverterUtils.convertCatalogTable(
                        context.getCatalogTable())
                        : context.getCatalogTable();
        final CatalogTable finalCatalogTable = this.renameCatalogTable(config, catalogTable);
        return () -> new DorisSink(config, finalCatalogTable);
    }

    private CatalogTable renameCatalogTable(ReadonlyConfig options, CatalogTable catalogTable) {
        TableIdentifier tableId = catalogTable.getTableId();
        String tableName;
        String databaseName;
        String tableIdentifier = options.get(DorisSinkOptions.TABLE_IDENTIFIER);
        if (StringUtils.isNotEmpty(tableIdentifier)) {
            tableName = tableIdentifier.split("\\.")[1];
            databaseName = tableIdentifier.split("\\.")[0];
        } else {
            if (StringUtils.isNotEmpty(options.get(TABLE))) {
                tableName = options.get(TABLE);
            } else {
                tableName = tableId.getTableName();
            }
            if (StringUtils.isNotEmpty(options.get(DATABASE))) {
                databaseName = options.get(DATABASE);
            } else {
                databaseName = tableId.getDatabaseName();
            }
        }
        TableIdentifier newTableId =
                TableIdentifier.of(tableId.getCatalogName(), databaseName, null, tableName);
        return CatalogTable.of(newTableId, catalogTable);
    }
}
