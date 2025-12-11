package org.apache.cockpit.connectors.api.jdbc.sink.savemode;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.sink.DefaultSaveModeHandler;

@Slf4j
public class JdbcSaveModeHandler extends DefaultSaveModeHandler {
    public boolean createIndex;

    public JdbcSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            TablePath tablePath,
            CatalogTable catalogTable,
            String customSql,
            boolean createIndex) {
        super(schemaSaveMode, dataSaveMode, catalog, tablePath, catalogTable, customSql);
        this.createIndex = createIndex;
    }

    @Override
    protected void createTable() {
        super.createTablePreCheck();
        catalog.createTable(tablePath, catalogTable, true, createIndex);
    }
}
