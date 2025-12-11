package org.apache.cockpit.connectors.elasticsearch.serialize.type.impl;

import org.apache.cockpit.connectors.elasticsearch.serialize.type.IndexTypeSerializer;

import java.util.Map;

/** generate an index type for elasticsearch version:2.*,5.*,6.* */
public class RequiredIndexTypeSerializer implements IndexTypeSerializer {

    private final String type;

    public RequiredIndexTypeSerializer(String type) {
        this.type = type;
    }

    @Override
    public void fillType(Map<String, String> indexInner) {
        indexInner.put("_type", type);
    }
}
