package org.apache.cockpit.connectors.api.jdbc.converter;



import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface BasicTypeConverter<T extends BasicTypeDefine> extends TypeConverter<T> {

    /**
     * Convert {@link CatalogTable} columns definition to external system's type definition.
     *
     * @param table
     * @param identifiers
     * @return
     */
    default List<T> reconvert(CatalogTable table, String... identifiers) {
        List<T> typeDefines = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            T t = reconvert(column);
            if (table.getCatalogName().equals(identifier())) {
                t.setColumnType(column.getSourceType());
            }
            if (identifiers != null) {
                Arrays.asList(identifiers)
                        .forEach(
                                id -> {
                                    if (id.equals(t.getName())) {
                                        t.setColumnType(column.getSourceType());
                                    }
                                });
            }
            typeDefines.add(t);
        }
        return typeDefines;
    }
}
