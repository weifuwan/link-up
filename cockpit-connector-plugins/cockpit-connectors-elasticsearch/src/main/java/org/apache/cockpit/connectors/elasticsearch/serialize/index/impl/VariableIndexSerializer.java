package org.apache.cockpit.connectors.elasticsearch.serialize.index.impl;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.elasticsearch.serialize.index.IndexSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** index include variable */
public class VariableIndexSerializer implements IndexSerializer {

    private final String index;
    private final Map<String, Integer> fieldIndexMap;

    private final String nullDefault = "null";

    public VariableIndexSerializer(
            SeaTunnelRowType seaTunnelRowType, String index, List<String> fieldNames) {
        this.index = index;
        String[] rowFieldNames = seaTunnelRowType.getFieldNames();
        fieldIndexMap = new HashMap<>(rowFieldNames.length);
        for (int i = 0; i < rowFieldNames.length; i++) {
            if (fieldNames.contains(rowFieldNames[i])) {
                fieldIndexMap.put(rowFieldNames[i], i);
            }
        }
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        String indexName = this.index;
        for (Map.Entry<String, Integer> fieldIndexEntry : fieldIndexMap.entrySet()) {
            String fieldName = fieldIndexEntry.getKey();
            int fieldIndex = fieldIndexEntry.getValue();
            String value = getValue(fieldIndex, row);
            indexName = indexName.replace(String.format("${%s}", fieldName), value);
        }
        return indexName.toLowerCase();
    }

    private String getValue(int fieldIndex, SeaTunnelRow row) {
        Object valueObj = row.getField(fieldIndex);
        if (valueObj == null) {
            return nullDefault;
        } else {
            return valueObj.toString();
        }
    }
}
