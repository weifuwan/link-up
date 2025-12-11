package org.apache.cockpit.connectors.api.type;

import lombok.Getter;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class MultipleRowType
        implements SeaTunnelDataType<SeaTunnelRow>, Iterable<Map.Entry<String, SeaTunnelRowType>> {
    private final Map<String, SeaTunnelRowType> rowTypeMap;
    @Getter private String[] tableIds;

    public MultipleRowType(String[] tableIds, SeaTunnelRowType[] rowTypes) {
        Map<String, SeaTunnelRowType> rowTypeMap = new LinkedHashMap<>();
        for (int i = 0; i < tableIds.length; i++) {
            rowTypeMap.put(tableIds[i], rowTypes[i]);
        }
        this.tableIds = tableIds;
        this.rowTypeMap = rowTypeMap;
    }

    public MultipleRowType(Map<String, SeaTunnelRowType> rowTypeMap) {
        this.tableIds = rowTypeMap.keySet().toArray(new String[0]);
        this.rowTypeMap = rowTypeMap;
    }

    public SeaTunnelRowType getRowType(String tableId) {
        return rowTypeMap.get(tableId);
    }

    @Override
    public Class<SeaTunnelRow> getTypeClass() {
        return SeaTunnelRow.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.MULTIPLE_ROW;
    }

    @Override
    public Iterator<Map.Entry<String, SeaTunnelRowType>> iterator() {
        return rowTypeMap.entrySet().iterator();
    }
}
