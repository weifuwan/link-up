package org.apache.cockpit.connectors.api.common.sink;


import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;

import java.io.IOException;

public abstract class AbstractSimpleSink<T>
        implements SeaTunnelSink<T> {

    @Override
    public abstract AbstractSinkWriter<T> createWriter(SinkWriter.Context context)
            throws IOException;
}
