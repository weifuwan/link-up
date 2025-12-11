package org.apache.cockpit.connectors.starrocks.serialize;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.Serializable;

public interface StarRocksISerializer extends Serializable {

    String serialize(SeaTunnelRow seaTunnelRow);
}
