package org.apache.cockpit.connectors.api.source;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.common.metrics.MetricsContext;
import org.apache.cockpit.connectors.api.event.EventListener;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;

import java.io.IOException;
import java.util.Map;

/**
 * The {@link SourceReader} is used to generate source record, and it will be running at worker.
 *
 * @param <T> record type.
 * @param <SplitT> source split type.
 */
public interface SourceReader<T, SplitT extends SourceSplit>
        extends AutoCloseable {

    /** Open the source reader. */
    void open() throws Exception;


    CatalogTable getJdbcSourceTables();

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    void close() throws IOException;

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    void pollNext(Collector<T> output) throws Exception;



    interface Context {

        /** @return The index of this subtask. */
        int getIndexOfSubtask();

        /** @return boundedness of this reader. */
        Boundedness getBoundedness();

        /** Indicator that the input has reached the end of data. Then will cancel this reader. */
        void signalNoMoreElement();


        /** @return metricsContext of this reader. */
        MetricsContext getMetricsContext();

        /**
         * Get the {@link EventListener} of this reader.
         *
         * @return
         */
        EventListener getEventListener();
    }
}
