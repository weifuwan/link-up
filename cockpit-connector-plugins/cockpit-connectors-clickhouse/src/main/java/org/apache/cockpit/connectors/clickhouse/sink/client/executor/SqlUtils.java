package org.apache.cockpit.connectors.clickhouse.sink.client.executor;

import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class SqlUtils {
    public static String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(SqlUtils::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName)
                        .collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    public static String getDeleteStatement(
            String tableName,
            String[] conditionFields,
            boolean enableExperimentalLightweightDelete) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        String deleteStatement =
                format("DELETE FROM %s WHERE %s", quoteIdentifier(tableName), conditionClause);
        if (enableExperimentalLightweightDelete) {
            deleteStatement += " settings allow_experimental_lightweight_delete = true";
        }
        return deleteStatement;
    }

    public static String getAlterTableUpdateStatement(
            String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(conditionFields).contains(fieldName))
                        .map(
                                fieldName ->
                                        String.format(
                                                "%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "ALTER TABLE %s UPDATE %s WHERE %s settings mutations_sync = 1",
                tableName, setClause, conditionClause);
    }

    public static String getAlterTableDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "ALTER TABLE %s DELETE WHERE %s settings mutations_sync = 1",
                tableName, conditionClause);
    }

    public static String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(field -> format("%s = :%s", quoteIdentifier(field), field))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "SELECT 1 FROM %s WHERE %s", quoteIdentifier(tableName), fieldExpressions);
    }
}
