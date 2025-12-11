package org.apache.cockpit.connectors.api.sink;


import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;

public interface SaveModeHandler extends AutoCloseable {

    void open();

    void handleSchemaSaveMode();

    void handleDataSaveMode();

    void handleSchemaSaveModeWithRestore();

    SchemaSaveMode getSchemaSaveMode();

    DataSaveMode getDataSaveMode();

    TablePath getHandleTablePath();

    Catalog getHandleCatalog();

    default void handleSaveMode() {
        handleSchemaSaveMode();
        handleDataSaveMode();
    }
}
