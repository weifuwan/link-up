package org.apache.cockpit.connectors.api.sink;


import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.event.EventListener;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * The sink writer use to write data to third party data receiver. This class will run on
 * taskManger/Worker.
 *
 * @param <T>           The data class by sink accept. Only support {@link
 * @param <CommitInfoT> The type of commit message.
 * @param <StateT>      The type of state.
 */
public interface SinkWriter<T> {


    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    void close() throws IOException;

    /**
     * write row
     *
     * @param row row
     */
    void write(T row) throws IOException;

    default Optional prepareCommit() throws IOException {
        return Optional.empty();
    }

    interface Context extends Serializable {


        /**
         * @return metricsContext of this reader.
         */
        MetricsContext getMetricsContext();

        /**
         * Get the {@link EventListener} of this writer.
         *
         * @return
         */
        EventListener getEventListener();

    }
}
