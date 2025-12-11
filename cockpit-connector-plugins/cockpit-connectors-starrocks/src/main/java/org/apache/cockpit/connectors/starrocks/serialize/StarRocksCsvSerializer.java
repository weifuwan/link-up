package org.apache.cockpit.connectors.starrocks.serialize;


import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;

public class StarRocksCsvSerializer extends StarRocksBaseSerializer
        implements StarRocksISerializer {
    private static final long serialVersionUID = 1L;

    private final String columnSeparator;
    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean enableUpsertDelete;

    public StarRocksCsvSerializer(
            String sp, SeaTunnelRowType seaTunnelRowType, boolean enableUpsertDelete) {
        this.columnSeparator = StarRocksDelimiterParser.parse(sp, "\t");
        this.seaTunnelRowType = seaTunnelRowType;
        this.enableUpsertDelete = enableUpsertDelete;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getFields().length; i++) {
            Object value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getFields().length - 1) {
                sb.append(columnSeparator);
            }
        }
        if (enableUpsertDelete) {
            sb.append(columnSeparator).append(StarRocksSinkOP.parse(row.getRowKind()).ordinal());
        }
        return sb.toString();
    }
}
