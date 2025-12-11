package org.apache.cockpit.connectors.api.jdbc.converter;


import org.apache.cockpit.connectors.api.catalog.Column;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Type converter to transfer to/from external system types.
 *
 * @param <T>
 */
public interface TypeConverter<T> extends Serializable {

    String identifier();

    /**
     * Convert an external system's type definition to {@link Column}.
     *
     * @param typeDefine type define
     * @return column
     */
    Column convert(T typeDefine);

    default List<Column> convert(List<T> typeDefines) {
        return typeDefines.stream().map(this::convert).collect(Collectors.toList());
    }

    /**
     * Convert {@link Column} to an external system's type definition.
     *
     * @param column
     * @return
     */
    T reconvert(Column column);

    default List<T> reconvert(List<Column> columns) {
        return columns.stream().map(this::reconvert).collect(Collectors.toList());
    }
}
