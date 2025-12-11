package org.apache.cockpit.connectors.api.catalog;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.util.HashMap;
import java.util.Map;

/** Representation of a physical column. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class PhysicalColumn extends Column {

    private static final long serialVersionUID = 1L;

    protected PhysicalColumn(
            String name, SeaTunnelDataType<?> dataType, Long columnLength, Integer scale) {
        super(name, dataType, columnLength, scale);
    }

    protected PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        super(name, dataType, columnLength, nullable, defaultValue, comment);
    }

    public PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment) {
        super(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                null,
                new HashMap<>());
    }

    public PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            Map<String, Object> options) {
        super(
                name,
                dataType,
                columnLength,
                null,
                nullable,
                defaultValue,
                comment,
                sourceType,
                options);
    }

    @Builder
    public PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            Map<String, Object> options) {
        super(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                options);
    }

    @Deprecated
    protected PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        super(name, dataType, columnLength, nullable, defaultValue, comment);
    }

    @Deprecated
    protected PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            boolean isUnsigned,
            boolean isZeroFill,
            Long bitLen,
            Long longColumnLength,
            Map<String, Object> options) {
        super(
                name,
                dataType,
                columnLength,
                nullable,
                defaultValue,
                comment,
                sourceType,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength,
                options);
    }

    @Deprecated
    public PhysicalColumn(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            String sinkType,
            Map<String, Object> options,
            boolean isUnsigned,
            boolean isZeroFill,
            Long bitLen,
            Long longColumnLength) {
        super(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                sinkType,
                options,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength);
    }

    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        return new PhysicalColumn(name, dataType, columnLength, nullable, defaultValue, comment);
    }

    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment) {
        return new PhysicalColumn(
                name, dataType, columnLength, scale, nullable, defaultValue, comment);
    }

    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            Map<String, Object> options) {
        return new PhysicalColumn(
                name, dataType, columnLength, nullable, defaultValue, comment, sourceType, options);
    }

    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            Map<String, Object> options) {
        return new PhysicalColumn(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                options);
    }

    @Deprecated
    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        return new PhysicalColumn(name, dataType, columnLength, nullable, defaultValue, comment);
    }

    @Deprecated
    public static PhysicalColumn of(
            String name,
            SeaTunnelDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            boolean isUnsigned,
            boolean isZeroFill,
            Long bitLen,
            Map<String, Object> options,
            Long longColumnLength) {
        return new PhysicalColumn(
                name,
                dataType,
                columnLength,
                nullable,
                defaultValue,
                comment,
                sourceType,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength,
                options);
    }

    @Override
    public boolean isPhysical() {
        return true;
    }

    @Override
    public Column copy(SeaTunnelDataType<?> newType) {
        return new PhysicalColumn(
                name,
                newType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                sinkType,
                options,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength);
    }

    @Override
    public Column copy() {
        return new PhysicalColumn(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                sinkType,
                options,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength);
    }

    @Override
    public Column rename(String newColumnName) {
        return new PhysicalColumn(
                newColumnName,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                sourceType,
                sinkType,
                options,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength);
    }

    @Override
    public Column reSourceType(String newSourceType) {
        return new PhysicalColumn(
                name,
                dataType,
                columnLength,
                scale,
                nullable,
                defaultValue,
                comment,
                newSourceType,
                sinkType,
                options,
                isUnsigned,
                isZeroFill,
                bitLen,
                longColumnLength);
    }
}
