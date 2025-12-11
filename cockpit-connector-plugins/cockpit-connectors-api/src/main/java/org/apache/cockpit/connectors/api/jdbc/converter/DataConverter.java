package org.apache.cockpit.connectors.api.jdbc.converter;


import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Data converter to transfer to/from external system data type.
 *
 * @param <T>
 */
public interface DataConverter<T> extends Serializable {

    String identifier();

    /**
     * Convert an external system's data type to {@link SeaTunnelDataType#getTypeClass()}.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    Object convert(SeaTunnelDataType typeDefine, Object value);

    default Object convert(Column columnDefine, Object value) {
        return convert(columnDefine.getDataType(), value);
    }

    default Object convert(T typeDefine, Column columnDefine, Object value) {
        return convert(columnDefine, value);
    }

    default Object[] convert(T[] typeDefine, Column[] columnDefine, Object[] value) {
        for (int i = 0; i < value.length; i++) {
            value[i] =
                    convert(typeDefine != null ? typeDefine[i] : null, columnDefine[i], value[i]);
        }
        return value;
    }

    default Object[] convert(Column[] columnDefine, Function<Column[], Object[]> valueApply) {
        Object[] fields = valueApply.apply(columnDefine);
        if (fields.length != columnDefine.length) {
            throw new IllegalStateException("columnDefine size not match");
        }

        for (int i = 0; i < fields.length; i++) {
            fields[i] = convert(columnDefine[i], fields[i]);
        }
        return fields;
    }

    default Object[] convert(
            T[] typeDefine, Column[] columnDefine, BiFunction<T[], Column[], Object[]> valueApply) {
        boolean hasTypeDefine = typeDefine != null;
        if (hasTypeDefine && typeDefine.length != columnDefine.length) {
            throw new IllegalStateException("typeDefine size not match");
        }

        Object[] fields = valueApply.apply(typeDefine, columnDefine);
        if (fields.length != columnDefine.length) {
            throw new IllegalStateException("columnDefine size not match");
        }

        for (int i = 0; i < fields.length; i++) {
            fields[i] = convert(hasTypeDefine ? typeDefine[i] : null, columnDefine[i], fields[i]);
        }
        return fields;
    }

    default Object reconvert(T typeDefine, Column columnDefine, Object value) {
        return reconvert(typeDefine, value);
    }

    /**
     * Convert object to an external system's data type.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    default Object reconvert(T typeDefine, Object value) {
        throw new UnsupportedOperationException("reconvert not support");
    }

    default Object reconvert(Column columnDefine, Object value) {
        return reconvert(columnDefine.getDataType(), value);
    }

    /**
     * Convert {@link SeaTunnelDataType#getTypeClass()} to an external system's data type.
     *
     * @param typeDefine
     * @param value
     * @return
     */
    default Object reconvert(SeaTunnelDataType typeDefine, Object value) {
        throw new UnsupportedOperationException("reconvert not support");
    }
}
