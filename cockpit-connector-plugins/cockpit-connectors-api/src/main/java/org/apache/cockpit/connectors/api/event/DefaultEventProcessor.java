
package org.apache.cockpit.connectors.api.event;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@AllArgsConstructor
public class DefaultEventProcessor implements EventListener, EventProcessor {
    private final String jobId;
    private final List<EventHandler> handlers;

    public DefaultEventProcessor() {
        this(DefaultEventProcessor.class.getClassLoader());
    }

    public DefaultEventProcessor(String jobId) {
        this(jobId, EventProcessor.loadEventHandlers(DefaultEventProcessor.class.getClassLoader()));
    }

    public DefaultEventProcessor(ClassLoader classLoader) {
        this(null, EventProcessor.loadEventHandlers(classLoader));
    }

    @Override
    public void process(Event event) {
        handlers.forEach(listener -> listener.handle(event));
    }

    @Override
    public void onEvent(Event event) {
        if (jobId != null) {
            event.setJobId(jobId);
        }
        process(event);
    }

    @Override
    public void close() throws Exception {
        log.info("Closing event handlers.");
        EventProcessor.close(handlers);
    }
}
