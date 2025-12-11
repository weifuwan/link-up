package org.apache.cockpit.connectors.api.catalog;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
public final class TableIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String catalogName;

    private final String databaseName;

    private final String schemaName;

    @NonNull private final String tableName;

    public TableIdentifier(
            String catalogName, String databaseName, String schemaName, @NonNull String tableName) {
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName cannot be empty");
        }
    }

    public static TableIdentifier of(String catalogName, String databaseName, String tableName) {
        return new TableIdentifier(catalogName, databaseName, null, tableName);
    }

    public static TableIdentifier of(String catalogName, TablePath tablePath) {
        return new TableIdentifier(
                catalogName,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    public static TableIdentifier of(
            String catalogName, String databaseName, String schemaName, String tableName) {
        return new TableIdentifier(catalogName, databaseName, schemaName, tableName);
    }

    public TablePath toTablePath() {
        return TablePath.of(databaseName, schemaName, tableName);
    }

    public TableIdentifier copy() {
        return TableIdentifier.of(catalogName, databaseName, schemaName, tableName);
    }

    @Override
    public String toString() {
        if (schemaName == null) {
            return String.join(".", catalogName, databaseName, tableName);
        }
        return String.join(".", catalogName, databaseName, schemaName, tableName);
    }
}
