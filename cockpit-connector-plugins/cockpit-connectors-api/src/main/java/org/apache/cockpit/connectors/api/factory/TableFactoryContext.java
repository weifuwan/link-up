package org.apache.cockpit.connectors.api.factory;

import lombok.Getter;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelException;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Getter
public abstract class TableFactoryContext {

    private final ReadonlyConfig options;
    private final ClassLoader classLoader;

    public TableFactoryContext(ReadonlyConfig options, ClassLoader classLoader) {
        this.options = options;
        this.classLoader = classLoader;
    }

    protected static void checkCatalogTableIllegal(List<CatalogTable> catalogTables) {
        for (CatalogTable catalogTable : catalogTables) {
            List<String> alreadyChecked = new ArrayList<>();
            for (String fieldName : catalogTable.getTableSchema().getFieldNames()) {
                if (StringUtils.isBlank(fieldName)) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Table %s field name cannot be empty",
                                    catalogTable.getTablePath().getFullName()));
                }
                if (alreadyChecked.contains(fieldName)) {
                    throw new SeaTunnelException(
                            String.format(
                                    "Table %s field %s duplicate",
                                    catalogTable.getTablePath().getFullName(), fieldName));
                }
                alreadyChecked.add(fieldName);
            }
        }
    }
}
