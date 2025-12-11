package org.apache.cockpit.connectors.elasticsearch.sink;

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
import org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchBaseOptions;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions;

import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchBaseOptions.HOSTS;
import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions.*;

@AutoService(Factory.class)
public class ElasticsearchSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return DbType.ELASTICSEARCH.getCode();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        HOSTS,
                        ElasticsearchBaseOptions.INDEX,
                        ElasticsearchSinkOptions.SCHEMA_SAVE_MODE,
                        ElasticsearchSinkOptions.DATA_SAVE_MODE)
                .optional(
                        INDEX_TYPE,
                        PRIMARY_KEYS,
                        KEY_DELIMITER,
                        USERNAME,
                        PASSWORD,
                        MAX_RETRY_COUNT,
                        MAX_BATCH_SIZE,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        String original = readonlyConfig.get(ElasticsearchBaseOptions.INDEX);
        CatalogTable newTable =
                CatalogTable.of(
                        TableIdentifier.of(
                                context.getCatalogTable().getCatalogName(),
                                context.getCatalogTable().getTablePath().getDatabaseName(),
                                original),
                        context.getCatalogTable());
        return () -> new ElasticsearchSink(readonlyConfig, newTable);
    }
}
