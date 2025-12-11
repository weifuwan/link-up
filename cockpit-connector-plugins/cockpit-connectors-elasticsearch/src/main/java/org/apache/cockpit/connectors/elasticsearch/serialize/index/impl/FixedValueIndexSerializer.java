package org.apache.cockpit.connectors.elasticsearch.serialize.index.impl;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.elasticsearch.serialize.index.IndexSerializer;

/** index is a fixed value,not a variable */
public class FixedValueIndexSerializer implements IndexSerializer {

    private final String index;

    public FixedValueIndexSerializer(String index) {
        this.index = index;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        return index;
    }
}
