package org.apache.cockpit.connectors.doris.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.doris.config.DorisSinkConfig;
import org.apache.cockpit.connectors.doris.config.DorisSinkOptions;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;
import org.apache.cockpit.connectors.doris.sink.writer.DorisSinkWriter;

import java.io.IOException;
import java.util.Optional;

import static org.apache.cockpit.connectors.api.factory.FactoryUtil.discoverFactory;


@Slf4j
public class DorisSink
        implements SeaTunnelSink<SeaTunnelRow>,
        SupportSaveMode {

    private final DorisSinkConfig dorisSinkConfig;
    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private String jobId;

    public DorisSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.dorisSinkConfig = DorisSinkConfig.of(config);
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
        }
    }

    @Override
    public String getPluginName() {
        return "Doris";
    }


    @Override
    public DorisSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
        }
        return new DorisSinkWriter(context, catalogTable, dorisSinkConfig, jobId);
    }


    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
        }
        CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        CatalogFactory.class,
                        "Doris");
        if (catalogFactory == null) {
            throw new DorisConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, "Cannot find Doris catalog factory"));
        }

        Catalog catalog = catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), config);
        return Optional.of(
                new DefaultSaveModeHandler(
                        config.get(DorisSinkOptions.SCHEMA_SAVE_MODE),
                        config.get(DorisSinkOptions.DATA_SAVE_MODE),
                        catalog,
                        catalogTable,
                        config.get(DorisSinkOptions.CUSTOM_SQL)));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

}
