package org.apache.cockpit.connectors.influxdb.serialize;

import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.influxdb.dto.Point;

public interface Serializer {
    Point serialize(SeaTunnelRow seaTunnelRow);
}
