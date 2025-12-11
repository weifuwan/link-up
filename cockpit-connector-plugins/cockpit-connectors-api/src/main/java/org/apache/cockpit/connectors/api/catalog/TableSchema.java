package org.apache.cockpit.connectors.api.catalog;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Represent a physical table schema. */
@Data
public final class TableSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Column> columns;

    @Getter(AccessLevel.PRIVATE)
    private final List<String> columnNames;

    private final PrimaryKey primaryKey;

    private final List<ConstraintKey> constraintKeys;

    public TableSchema(
            List<Column> columns, PrimaryKey primaryKey, List<ConstraintKey> constraintKeys) {
        this.columns = columns;
        this.columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        this.primaryKey = primaryKey;
        this.constraintKeys = constraintKeys;
    }

    public static Builder builder() {
        return new Builder();
    }

    public SeaTunnelRowType toPhysicalRowDataType() {
        SeaTunnelDataType<?>[] fieldTypes =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getDataType)
                        .toArray(SeaTunnelDataType[]::new);
        String[] fields =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(Column::getName)
                        .toArray(String[]::new);
        return new SeaTunnelRowType(fields, fieldTypes);
    }

    public String[] getFieldNames() {
        return columnNames.toArray(new String[0]);
    }

    public int indexOf(String columnName) {
        return columnNames.indexOf(columnName);
    }

    public Column getColumn(String columnName) {
        return columns.get(indexOf(columnName));
    }

    public boolean contains(String columnName) {
        return columnNames.contains(columnName);
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public static final class Builder {
        private final List<Column> columns = new ArrayList<>();

        private PrimaryKey primaryKey;

        private final List<ConstraintKey> constraintKeys = new ArrayList<>();

        public Builder columns(List<Column> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public Builder column(Column column) {
            this.columns.add(column);
            return this;
        }

        public Builder primaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder constraintKey(ConstraintKey constraintKey) {
            this.constraintKeys.add(constraintKey);
            return this;
        }

        public Builder constraintKey(List<ConstraintKey> constraintKeys) {
            this.constraintKeys.addAll(constraintKeys);
            return this;
        }

        public TableSchema build() {
            return new TableSchema(columns, primaryKey, constraintKeys);
        }
    }

    public TableSchema copy() {
        List<Column> copyColumns = columns.stream().map(Column::copy).collect(Collectors.toList());
        List<ConstraintKey> copyConstraintKeys =
                constraintKeys.stream().map(ConstraintKey::copy).collect(Collectors.toList());
        return TableSchema.builder()
                .constraintKey(copyConstraintKeys)
                .columns(copyColumns)
                .primaryKey(primaryKey == null ? null : primaryKey.copy())
                .build();
    }
}
