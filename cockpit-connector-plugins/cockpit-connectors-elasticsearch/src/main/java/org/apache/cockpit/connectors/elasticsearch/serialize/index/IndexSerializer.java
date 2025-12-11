package org.apache.cockpit.connectors.elasticsearch.serialize.index;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

/** index is a variable */
public interface IndexSerializer {

    String serialize(SeaTunnelRow row);
}
