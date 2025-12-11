package org.apache.cockpit.connectors.api.serialization;

import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.io.IOException;
import java.io.Serializable;

public interface DeserializationSchema<T> extends Serializable {

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an SeaTunnel Row (null if the message cannot be
     * deserialized).
     */
    T deserialize(byte[] message) throws IOException;

    default void deserialize(byte[] message, Collector<T> out) throws Exception {
        T deserialize = deserialize(message);
        if (deserialize != null) {
            out.collect(deserialize);
        }
    }

    SeaTunnelDataType<T> getProducedType();
}
