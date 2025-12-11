package org.apache.cockpit.connectors.api.jdbc.sink;

import org.apache.cockpit.connectors.api.sink.SaveModeHandler;

import java.util.Optional;

/** The Sink Connectors which support schema and data SaveMode should implement this interface */
public interface SupportSaveMode {

    String DATA_SAVE_MODE_KEY = "data_save_mode";

    String SCHEMA_SAVE_MODE_KEY = "schema_save_mode";

    // This method defines the return of a specific save_mode handler
    Optional<SaveModeHandler> getSaveModeHandler();
}
