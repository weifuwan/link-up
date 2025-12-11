package org.apache.cockpit.connectors.api.event;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(EventHandler.class)
public class LoggingEventHandler implements EventHandler {

    @Override
    public void handle(Event event) {
        log.info("log event: {}", event);
    }
}
