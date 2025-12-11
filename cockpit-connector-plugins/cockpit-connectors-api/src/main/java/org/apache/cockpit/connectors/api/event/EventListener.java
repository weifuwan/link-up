package org.apache.cockpit.connectors.api.event;

import java.io.Serializable;

public interface EventListener extends Serializable {
    void onEvent(Event event);
}
