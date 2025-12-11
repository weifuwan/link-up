package org.apache.cockpit.connectors.cache.catalog.savemode;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

@Slf4j
public class CacheSaveModeHandler extends DefaultSaveModeHandler {
    public boolean createIndex;

    public CacheSaveModeHandler(
            @Nonnull SchemaSaveMode schemaSaveMode,
            @Nonnull DataSaveMode dataSaveMode,
            @Nonnull Catalog catalog,
            @Nonnull TablePath tablePath,
            @Nullable CatalogTable catalogTable,
            @Nullable String customSql,
            boolean createIndex) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql);
        this.createIndex = createIndex;
    }

    @Override
    protected void createTable() {
        try {
            log.info(
                    "Creating table {} with action {}",
                    tablePath,
                    catalog.previewAction(
                            Catalog.ActionType.CREATE_TABLE,
                            tablePath,
                            Optional.ofNullable(catalogTable)));
            catalog.createTable(tablePath, catalogTable, true, createIndex);
        } catch (UnsupportedOperationException ignore) {
            log.info("Creating table {}", tablePath);
        }
    }
}
