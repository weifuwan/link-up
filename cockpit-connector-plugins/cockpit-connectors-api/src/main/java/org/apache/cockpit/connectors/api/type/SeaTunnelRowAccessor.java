package org.apache.cockpit.connectors.api.type;

import lombok.AllArgsConstructor;

import java.util.Map;

@AllArgsConstructor
public class SeaTunnelRowAccessor {
    private final SeaTunnelRow row;

    public int getArity() {
        return row.getArity();
    }

    public String getTableId() {
        return row.getTableId();
    }

    public RowKind getRowKind() {
        return row.getRowKind();
    }

    public Object getField(int pos) {
        return row.getField(pos);
    }

    public Object[] getFields() {
        return row.getFields();
    }

    public Map<String, Object> getOptions() {
        return row.getOptions();
    }
}
