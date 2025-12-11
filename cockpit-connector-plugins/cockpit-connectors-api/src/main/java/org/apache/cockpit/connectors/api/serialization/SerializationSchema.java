package org.apache.cockpit.connectors.api.serialization;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.Serializable;

public interface SerializationSchema extends Serializable {
    /**
     * Serializes the incoming element to a specified type.
     *
     * @param element The incoming element to be serialized
     * @return The serialized element.
     */
    byte[] serialize(SeaTunnelRow element);
}
