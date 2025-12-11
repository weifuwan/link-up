package org.apache.cockpit.connectors.elasticsearch.serialize.type;

import java.util.Map;

public interface IndexTypeSerializer {

    void fillType(Map<String, String> indexInner);
}
