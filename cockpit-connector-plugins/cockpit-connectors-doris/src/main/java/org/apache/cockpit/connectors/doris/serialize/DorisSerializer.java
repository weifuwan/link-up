package org.apache.cockpit.connectors.doris.serialize;



import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.IOException;
import java.io.Serializable;

public interface DorisSerializer extends Serializable {

    void open() throws IOException;

    byte[] serialize(SeaTunnelRow seaTunnelRow) throws IOException;

    void close() throws IOException;
}
