package org.apache.cockpit.connectors.api.sink;


import com.typesafe.config.Config;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.common.PluginIdentifierInterface;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

public interface SeaTunnelSink<IN>
        extends Serializable,
        PluginIdentifierInterface {

    /**
     * Set the row type info of sink row data. This method will be automaticallyMySQLSinkFactory called by
     * translation.
     *
     * @param seaTunnelRowType The row type info of sink.
     */
    @Deprecated
    default void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        throw new UnsupportedOperationException("setTypeInfo method is not supported");
    }

    default void open() {
    }

    /**
     * Get the data type of the records consumed by this sink.
     *
     * @return SeaTunnel data type.
     */
    @Deprecated
    default SeaTunnelDataType<IN> getConsumedType() {
        throw new UnsupportedOperationException("getConsumedType method is not supported");
    }

    /**
     * This method will be called to creat {@link SinkWriter}
     *
     * @param context The sink context
     * @return Return sink writer instance
     * @throws IOException throws IOException when createWriter failed.
     */
    SinkWriter<IN> createWriter(SinkWriter.Context context) throws IOException;


    /**
     * Get the catalog table of the sink.
     *
     * @return Optional of catalog table.
     */
    default Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.empty();
    }

    default void prepare(Config config) {

    }
}
