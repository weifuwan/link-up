package org.apache.cockpit.connectors.starrocks.serialize;



import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.type.SqlType;
import org.apache.cockpit.connectors.api.util.JsonUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksJsonSerializer extends StarRocksBaseSerializer
        implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;
    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean enableUpsertDelete;

    public StarRocksJsonSerializer(SeaTunnelRowType seaTunnelRowType, boolean enableUpsertDelete) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.enableUpsertDelete = enableUpsertDelete;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        Map<String, Object> rowMap = new LinkedHashMap<>(row.getFields().length);

        for (int i = 0; i < row.getFields().length; i++) {
            SqlType sqlType = seaTunnelRowType.getFieldType(i).getSqlType();
            Object value;
            if (sqlType == SqlType.ARRAY
                    || sqlType == SqlType.MAP
                    || sqlType == SqlType.ROW
                    || sqlType == SqlType.MULTIPLE_ROW) {
                // If the field type is complex type, we should keep the origin value.
                // It will be transformed to json string in the next step
                // JsonUtils.toJsonString(rowMap).
                value = row.getField(i);
            } else {
                value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            }
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        if (enableUpsertDelete) {
            rowMap.put(
                    StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.parse(row.getRowKind()).ordinal());
        }
        return JsonUtils.toJsonString(rowMap);
    }
}
