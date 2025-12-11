
package org.apache.cockpit.connectors.api.type;



import org.apache.cockpit.connectors.api.catalog.TablePath;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.cockpit.connectors.api.type.CommonOptions.*;


public class MetadataUtil {

    public static final List<String> METADATA_FIELDS;

    static {
        METADATA_FIELDS = new ArrayList<>();
        Stream.of(CommonOptions.values())
                .filter(CommonOptions::isSupportMetadataTrans)
                .map(CommonOptions::getName)
                .forEach(METADATA_FIELDS::add);
    }

    public static void setDelay(SeaTunnelRow row, Long delay) {
        row.getOptions().put(DELAY.getName(), delay);
    }

    public static void setPartition(SeaTunnelRow row, String[] partition) {
        row.getOptions().put(PARTITION.getName(), partition);
    }

    public static void setEventTime(SeaTunnelRow row, Long delay) {
        row.getOptions().put(EVENT_TIME.getName(), delay);
    }

    public static Long getDelay(SeaTunnelRowAccessor row) {
        return (Long) row.getOptions().get(DELAY.getName());
    }

    public static String getDatabase(SeaTunnelRowAccessor row) {
        if (row.getTableId() == null) {
            return null;
        }
        return TablePath.of(row.getTableId()).getDatabaseName();
    }

    public static String getTable(SeaTunnelRowAccessor row) {
        if (row.getTableId() == null) {
            return null;
        }
        return TablePath.of(row.getTableId()).getTableName();
    }

    public static String getRowKind(SeaTunnelRowAccessor row) {
        return row.getRowKind().shortString();
    }

    public static String getPartitionStr(SeaTunnelRowAccessor row) {
        Object partition = row.getOptions().get(PARTITION.getName());
        return Objects.nonNull(partition) ? String.join(",", (String[]) partition) : null;
    }

    public static String[] getPartition(SeaTunnelRowAccessor row) {
        return (String[]) row.getOptions().get(PARTITION.getName());
    }

    public static Long getEventTime(SeaTunnelRowAccessor row) {
        return (Long) row.getOptions().get(EVENT_TIME.getName());
    }

    public static boolean isMetadataField(String fieldName) {
        return METADATA_FIELDS.contains(fieldName);
    }
}
