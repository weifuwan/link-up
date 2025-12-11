package org.apache.cockpit.connectors.elasticsearch.serialize;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

public interface SeaTunnelRowSerializer {

    String serializeRow(SeaTunnelRow row);
}
