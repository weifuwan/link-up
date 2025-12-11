package org.apache.cockpit.connectors.elasticsearch.serialize.type.impl;


import org.apache.cockpit.connectors.elasticsearch.serialize.type.IndexTypeSerializer;

import java.util.Map;

/** not need an index type for elasticsearch version:6.*,7.*,8.* */
public class NotIndexTypeSerializer implements IndexTypeSerializer {

    @Override
    public void fillType(Map<String, String> indexInner) {}
}
