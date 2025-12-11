package org.apache.cockpit.connectors.api.event;

import java.io.Serializable;

public interface EventHandler extends Serializable, AutoCloseable {

    /**
     * Receive and handle the event data.
     *
     * @param event
     */
    void handle(Event event);

    @Override
    default void close() throws Exception {}
}
