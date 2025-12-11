package org.apache.cockpit.connectors.api.catalog;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

/** Representation of a metadata column. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetadataColumn extends Column {
    private static final long serialVersionUID = 1L;

    private final String metadataKey;

    protected MetadataColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            String metadataKey,
            boolean nullable,
            Object defaultValue,
            String comment) {
        super(name, dataType, columnLength, nullable, defaultValue, comment);
        this.metadataKey = metadataKey;
    }

    public static MetadataColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            String metadataKey,
            boolean nullable,
            Object defaultValue,
            String comment) {
        return new MetadataColumn(
                name, dataType, columnLength, metadataKey, nullable, defaultValue, comment);
    }

    @Override
    public boolean isPhysical() {
        return false;
    }

    @Override
    public Column copy(SeaTunnelDataType<?> newType) {
        return MetadataColumn.of(
                name, newType, columnLength, metadataKey, nullable, defaultValue, comment);
    }

    @Override
    public Column copy() {
        return MetadataColumn.of(
                name, dataType, columnLength, metadataKey, nullable, defaultValue, comment);
    }

    @Override
    public Column rename(String newColumnName) {
        return MetadataColumn.of(
                newColumnName,
                dataType,
                columnLength,
                metadataKey,
                nullable,
                defaultValue,
                comment);
    }

    @Override
    public Column reSourceType(String sourceType) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
