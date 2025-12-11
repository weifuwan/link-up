package org.apache.cockpit.connectors.api.type;

import lombok.Getter;
import org.apache.cockpit.connectors.api.catalog.Column;

/**
 * Common option keys of SeaTunnel {@link Column#getOptions()} / {@link SeaTunnelRow#getOptions()}.
 * Used to store some extra information of the column value.
 */
@Getter
public enum CommonOptions {
    /**
     * The key of {@link Column#getOptions()} to specify the column value is a json format string.
     */
    JSON("Json", false),
    /** The key of {@link Column#getOptions()} to specify the column value is a metadata field. */
    METADATA("Metadata", false),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the partition value of the row value.
     */
    PARTITION("Partition", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the DATABASE value of the row value.
     */
    DATABASE("Database", true),
    /** The key of {@link SeaTunnelRow#getOptions()} to store the TABLE value of the row value. */
    TABLE("Table", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the ROW_KIND value of the row value.
     */
    ROW_KIND("RowKind", true),
    /**
     * The key of {@link SeaTunnelRow#getOptions()} to store the EVENT_TIME value of the row value.
     */
    EVENT_TIME("EventTime", true),
    /** The key of {@link SeaTunnelRow#getOptions()} to store the DELAY value of the row value. */
    DELAY("Delay", true);

    private final String name;
    private final boolean supportMetadataTrans;

    CommonOptions(String name, boolean supportMetadataTrans) {
        this.name = name;
        this.supportMetadataTrans = supportMetadataTrans;
    }

    public static CommonOptions fromName(String name) {
        for (CommonOptions option : CommonOptions.values()) {
            if (option.getName().equals(name)) {
                return option;
            }
        }
        throw new IllegalArgumentException("Unknown option name: " + name);
    }
}
