package org.apache.cockpit.connectors.api.catalog;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractJdbcCreateTableSqlBuilder {

    protected boolean primaryContainsAllConstrainKey(
            PrimaryKey primaryKey, ConstraintKey constraintKey) {
        List<String> columnNames = primaryKey.getColumnNames();
        List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumnNames =
                constraintKey.getColumnNames();
        return new HashSet<>(
                        columnNames.stream().map(Object::toString).collect(Collectors.toList()))
                .containsAll(
                        constraintKeyColumnNames.stream()
                                .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                                .collect(Collectors.toList()));
    }
}
