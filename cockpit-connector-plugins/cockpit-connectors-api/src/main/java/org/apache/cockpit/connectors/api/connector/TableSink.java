package org.apache.cockpit.connectors.api.connector;


import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;

public interface TableSink<IN> {

    SeaTunnelSink<IN> createSink();
}
