package org.apache.cockpit.connectors.elasticsearch.sink;


import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions;

import java.util.Optional;

import static org.apache.cockpit.connectors.api.factory.FactoryUtil.discoverFactory;
import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions.MAX_BATCH_SIZE;
import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions.MAX_RETRY_COUNT;


public class ElasticsearchSink
        implements SeaTunnelSink<
        SeaTunnelRow>,
        SupportSaveMode {

    private ReadonlyConfig config;
    private CatalogTable catalogTable;

    private final int maxBatchSize;

    private final int maxRetryCount;

    public ElasticsearchSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        maxBatchSize = config.get(MAX_BATCH_SIZE);
        maxRetryCount = config.get(MAX_RETRY_COUNT);
    }

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public ElasticsearchSinkWriter createWriter(SinkWriter.Context context) {
        return new ElasticsearchSinkWriter(
                context, catalogTable, config, maxBatchSize, maxRetryCount);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        getPluginName());
        if (catalogFactory == null) {
            return Optional.empty();
        }
        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        SchemaSaveMode schemaSaveMode = config.get(ElasticsearchSinkOptions.SCHEMA_SAVE_MODE);
        DataSaveMode dataSaveMode = config.get(ElasticsearchSinkOptions.DATA_SAVE_MODE);

        TablePath tablePath = TablePath.of("", catalogTable.getTableId().getTableName());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode, dataSaveMode, catalog, tablePath, null, null));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

}
