package org.apache.cockpit.connectors.api.event;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public interface EventProcessor extends AutoCloseable {
    void process(Event event);

    static List<EventHandler> loadEventHandlers(ClassLoader classLoader) {
        try {
            List<EventHandler> result = new LinkedList<>();
            ServiceLoader.load(EventHandler.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            throw new RuntimeException("Could not load service provider for event handlers.", e);
        }
    }

    static void close(List<EventHandler> handlers) throws Exception {
        if (handlers != null) {
            for (EventHandler handler : handlers) {
                handler.close();
            }
        }
    }
}
