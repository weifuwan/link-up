package org.apache.cockpit.connectors.api.catalog;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class PrimaryKey implements Serializable {
    private static final long serialVersionUID = 1L;

    // This field is not used now
    private final String primaryKey;

    private final List<String> columnNames;

    private Boolean enableAutoId;

    public PrimaryKey(String primaryKey, List<String> columnNames) {
        this.primaryKey = primaryKey;
        this.columnNames = columnNames;
        this.enableAutoId = null;
    }

    public static boolean isPrimaryKeyField(PrimaryKey primaryKey, String fieldName) {
        if (primaryKey == null || primaryKey.getColumnNames() == null) {
            return false;
        }
        return primaryKey.getColumnNames().contains(fieldName);
    }

    public static PrimaryKey of(String primaryKey, List<String> columnNames, Boolean autoId) {
        return new PrimaryKey(primaryKey, columnNames, autoId);
    }

    public static PrimaryKey of(String primaryKey, List<String> columnNames) {
        return new PrimaryKey(primaryKey, columnNames);
    }

    public PrimaryKey copy() {
        return PrimaryKey.of(primaryKey, new ArrayList<>(columnNames));
    }
}
