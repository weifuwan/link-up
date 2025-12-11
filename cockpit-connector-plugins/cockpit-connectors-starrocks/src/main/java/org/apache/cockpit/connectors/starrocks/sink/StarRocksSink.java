package org.apache.cockpit.connectors.starrocks.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.common.sink.AbstractSimpleSink;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.starrocks.catalog.StarRocksCatalog;
import org.apache.cockpit.connectors.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.cockpit.connectors.starrocks.config.SinkConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksBaseOptions;

import java.util.Optional;

@Slf4j
public class StarRocksSink extends AbstractSimpleSink<SeaTunnelRow>
        implements SupportSaveMode {

    private final TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final DataSaveMode dataSaveMode;
    private final SchemaSaveMode schemaSaveMode;
    private final CatalogTable catalogTable;

    public StarRocksSink(SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = catalogTable.getTableSchema();
        this.catalogTable = catalogTable;
        this.dataSaveMode = sinkConfig.getDataSaveMode();
        this.schemaSaveMode = sinkConfig.getSchemaSaveMode();
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
    }

    @Override
    public StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new StarRocksSinkWriter(sinkConfig, tableSchema, sinkTablePath);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName());
        Catalog catalog =
                new StarRocksCatalog(
                        StarRocksBaseOptions.CONNECTOR_IDENTITY,
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getSaveModeCreateTemplate());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        catalog,
                        tablePath,
                        catalogTable,
                        sinkConfig.getCustomSql()));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

}
