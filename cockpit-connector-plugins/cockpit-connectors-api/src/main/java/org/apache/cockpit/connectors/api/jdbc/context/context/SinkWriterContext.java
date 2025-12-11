package org.apache.cockpit.connectors.api.jdbc.context.context;


import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.event.EventListener;
import org.apache.cockpit.connectors.api.sink.SinkWriter;

public class SinkWriterContext implements SinkWriter.Context {


    public SinkWriterContext() {
    }


    @Override
    public MetricsContext getMetricsContext() {
        return null;
    }

    @Override
    public EventListener getEventListener() {
        return null;
    }

}
