package org.apache.cockpit.connectors.api.catalog;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;


@Data
public class ConstraintKey implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ConstraintType constraintType;

    private final String constraintName;

    private final List<ConstraintKeyColumn> columnNames;

    private ConstraintKey(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        checkNotNull(constraintType, "constraintType must not be null");

        this.constraintType = constraintType;
        this.constraintName = constraintName;
        this.columnNames = columnNames;
    }

    public static ConstraintKey of(
            ConstraintType constraintType,
            String constraintName,
            List<ConstraintKeyColumn> columnNames) {
        return new ConstraintKey(constraintType, constraintName, columnNames);
    }

    @Data
    @AllArgsConstructor
    public static class ConstraintKeyColumn implements Serializable {
        private final String columnName;
        private final ColumnSortType sortType;

        public static ConstraintKeyColumn of(String columnName, ColumnSortType sortType) {
            return new ConstraintKeyColumn(columnName, sortType);
        }

        public ConstraintKeyColumn copy() {
            return ConstraintKeyColumn.of(columnName, sortType);
        }
    }

    public enum ConstraintType {
        INDEX_KEY,
        UNIQUE_KEY,
        FOREIGN_KEY,
        VECTOR_INDEX_KEY
    }

    public enum ColumnSortType {
        ASC,
        DESC
    }

    public ConstraintKey copy() {
        List<ConstraintKeyColumn> collect =
                columnNames.stream().map(ConstraintKeyColumn::copy).collect(Collectors.toList());
        return ConstraintKey.of(constraintType, constraintName, collect);
    }
}
