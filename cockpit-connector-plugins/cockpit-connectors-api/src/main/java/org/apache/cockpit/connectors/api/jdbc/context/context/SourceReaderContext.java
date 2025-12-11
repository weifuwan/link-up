package org.apache.cockpit.connectors.api.jdbc.context.context;


import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.event.EventListener;
import org.apache.cockpit.connectors.api.jdbc.flow.SourceFlowLifeCycle;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SourceReader;

public class SourceReaderContext implements SourceReader.Context {


    private final Boundedness boundedness;

    private final SourceFlowLifeCycle<?, ?> sourceActionLifeCycle;
    private final MetricsContext metricsContext;

    public SourceReaderContext(
            Boundedness boundedness,
            SourceFlowLifeCycle<?, ?> sourceActionLifeCycle,
            MetricsContext metricsContext) {
        this.boundedness = boundedness;
        this.sourceActionLifeCycle = sourceActionLifeCycle;
        this.metricsContext = metricsContext;
    }

    @Override
    public int getIndexOfSubtask() {
        return 0;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public void signalNoMoreElement() {

    }


    @Override
    public MetricsContext getMetricsContext() {
        return metricsContext;
    }

    @Override
    public EventListener getEventListener() {
        return null;
    }
}
