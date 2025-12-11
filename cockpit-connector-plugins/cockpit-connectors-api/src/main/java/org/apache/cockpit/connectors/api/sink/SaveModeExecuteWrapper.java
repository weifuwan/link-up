package org.apache.cockpit.connectors.api.sink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SaveModeExecuteWrapper {

    public SaveModeExecuteWrapper(SaveModeHandler handler) {
        this.handler = handler;
    }

    public void execute() {
        log.info(
                "Executing save mode for table: {}, with SchemaSaveMode: {}, DataSaveMode: {} using Catalog: {}",
                handler.getHandleTablePath(),
                handler.getSchemaSaveMode(),
                handler.getDataSaveMode(),
                handler.getHandleCatalog().name());
        handler.handleSaveMode();
    }

    private final SaveModeHandler handler;
}
