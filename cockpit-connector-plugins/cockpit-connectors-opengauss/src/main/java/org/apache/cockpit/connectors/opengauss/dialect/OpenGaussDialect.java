package org.apache.cockpit.connectors.opengauss.dialect;

import org.apache.cockpit.connectors.api.catalog.ConstraintKey;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.psql.dialect.PostgresDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpenGaussDialect extends PostgresDialect {

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        String[] fieldNames = tableSchema.getFieldNames();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();

        List<String> allUniqueKeyFields = new ArrayList<>();

        if (uniqueKeyFields != null) {
            allUniqueKeyFields.addAll(Arrays.asList(uniqueKeyFields));
        }

        for (ConstraintKey constraintKey : constraintKeys) {
            if (constraintKey.getConstraintType() == ConstraintKey.ConstraintType.UNIQUE_KEY) {
                List<String> constraintUniqueFields = constraintKey.getColumnNames().stream()
                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                        .collect(Collectors.toList());
                allUniqueKeyFields.addAll(constraintUniqueFields);
            }
        }

        List<String> distinctUniqueKeys = allUniqueKeyFields.stream()
                .distinct()
                .collect(Collectors.toList());

        String updateClause = Arrays.stream(fieldNames)
                .filter(fieldName -> !distinctUniqueKeys.contains(fieldName))
                .map(fieldName -> quoteIdentifier(fieldName) + "=EXCLUDED." + quoteIdentifier(fieldName))
                .collect(Collectors.joining(", "));

        if (updateClause.isEmpty()) {
            return Optional.empty();
        }

        String upsertSQL = String.format(
                "%s ON DUPLICATE KEY UPDATE %s",
                getInsertIntoStatement(database, tableName, fieldNames),
                updateClause);
        return Optional.of(upsertSQL);
    }
}