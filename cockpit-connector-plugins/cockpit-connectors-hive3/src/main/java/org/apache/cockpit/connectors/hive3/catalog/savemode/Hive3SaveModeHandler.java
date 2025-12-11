package org.apache.cockpit.connectors.hive3.catalog.savemode;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode.SINK_TABLE_NOT_EXIST;
import static org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode.SOURCE_ALREADY_HAS_DATA;

@Slf4j
public class Hive3SaveModeHandler implements SaveModeHandler {

    @Nonnull
    public SchemaSaveMode schemaSaveMode;

    @Nonnull
    public DataSaveMode dataSaveMode;

    @Nonnull
    public Catalog catalog;

    @Nonnull
    public TablePath tablePath;

    private boolean isNewTableCreated = false;

    private final CatalogTable catalogTable;

    public Hive3SaveModeHandler(SchemaSaveMode schemaSaveMode,
                                DataSaveMode dataSaveMode,
                                Catalog catalog,
                                TablePath tablePath,
                                CatalogTable catalogTable) {
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.catalog = catalog;
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;

    }

    @Override
    public void open() {
        catalog.open();
    }

    @Override
    public void handleSchemaSaveMode() {
        switch (schemaSaveMode) {
            case RECREATE_SCHEMA:
                recreateSchema();
                break;
            case CREATE_SCHEMA_WHEN_NOT_EXIST:
                createSchemaWhenNotExist();
                break;
            case ERROR_WHEN_SCHEMA_NOT_EXIST:
                errorWhenSchemaNotExist();
                break;
            case IGNORE:
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + schemaSaveMode);
        }
    }

    protected void errorWhenSchemaNotExist() {
        if (!tableExists()) {
            throw new SeaTunnelRuntimeException(SINK_TABLE_NOT_EXIST, "The sink table not exist");
        }
    }

    protected void createSchemaWhenNotExist() {
        if (!tableExists()) {
            createTable();
        }
    }

    protected void recreateSchema() {
        if (tableExists()) {
            dropTable();
        }
        createTable();
    }

    protected void createTable() {
        createTablePreCheck();
        catalog.createTable(tablePath, catalogTable, true);
        isNewTableCreated = true;
    }

    protected void createTablePreCheck() {
        if (!catalog.databaseExists(tablePath.getDatabaseName())) {
            try {
                log.info(
                        "Creating database {} with action {}",
                        tablePath.getDatabaseName(),
                        catalog.previewAction(
                                Catalog.ActionType.CREATE_DATABASE, tablePath, Optional.empty()));
            } catch (UnsupportedOperationException ignore) {
                log.info("Creating database {}", tablePath.getDatabaseName());
            }
            catalog.createDatabase(tablePath, true);
        }
        try {
            log.info(
                    "Creating table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.CREATE_TABLE,
                            tablePath,
                            Optional.ofNullable(catalogTable)));
        } catch (UnsupportedOperationException ignore) {
            log.info("Creating table {}", tablePath);
        }
    }

    protected void keepSchemaDropData() {
        if (tableExists() && !isNewTableCreated) {
            truncateTable();
        }
    }

    protected void truncateTable() {
        try {
            log.info(
                    "Truncating table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.TRUNCATE_TABLE, tablePath, Optional.empty()));
        } catch (UnsupportedOperationException ignore) {
            log.info("Truncating table {}", tablePath);
        }
        catalog.truncateTable(tablePath, true);
    }

    protected void dropTable() {
        try {
            log.info(
                    "Dropping table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.DROP_TABLE, tablePath, Optional.empty()));
        } catch (UnsupportedOperationException ignore) {
            log.info("Dropping table {}", tablePath);
        }
        catalog.dropTable(tablePath, true);
    }

    protected boolean tableExists() {
        return catalog.tableExists(tablePath);
    }

    @Override
    public void handleDataSaveMode() {
        switch (dataSaveMode) {
            case DROP_DATA:
                keepSchemaDropData();
                break;
            case APPEND_DATA:
                keepSchemaAndData();
                break;
            case CUSTOM_PROCESSING:
//                customProcessing();
                break;
            case ERROR_WHEN_DATA_EXISTS:
                errorWhenDataExists();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported save mode: " + dataSaveMode);
        }
    }

    protected void errorWhenDataExists() {
        if (dataExists()) {
            throw new SeaTunnelRuntimeException(
                    SOURCE_ALREADY_HAS_DATA, "The target data source already has data");
        }
    }

    protected boolean dataExists() {
        return catalog.isExistsData(tablePath);
    }

    protected void keepSchemaAndData() {}


    @Override
    public void handleSchemaSaveModeWithRestore() {

    }

    @Override
    public TablePath getHandleTablePath() {
        return tablePath;
    }

    @Override
    public Catalog getHandleCatalog() {
        return catalog;
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return dataSaveMode;
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
