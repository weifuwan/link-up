package org.apache.cockpit.connectors.api.sink;

import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.api.util.PlaceholderUtils.replacePlaceholders;


public class TablePlaceholderProcessor {

    public static final String NAME_DELIMITER = ".";

    public static final String FIELD_DELIMITER = ",";

    private static String replaceTableIdentifier(
            String placeholder, TableIdentifier identifier, String defaultValue) {
        placeholder =
                replacePlaceholders(
                        placeholder,
                        TablePlaceholder.REPLACE_DATABASE_NAME_KEY.getPlaceholder(),
                        identifier.getDatabaseName(),
                        defaultValue);
        placeholder =
                replacePlaceholders(
                        placeholder,
                        TablePlaceholder.REPLACE_SCHEMA_NAME_KEY.getPlaceholder(),
                        identifier.getSchemaName(),
                        defaultValue);
        placeholder =
                replacePlaceholders(
                        placeholder,
                        TablePlaceholder.REPLACE_TABLE_NAME_KEY.getPlaceholder(),
                        identifier.getTableName(),
                        defaultValue);

        List<String> fullPath = new ArrayList<>();
        if (identifier.getDatabaseName() != null) {
            fullPath.add(identifier.getDatabaseName());
        }
        if (identifier.getSchemaName() != null) {
            fullPath.add(identifier.getSchemaName());
        }
        if (!fullPath.isEmpty()) {
            placeholder =
                    replacePlaceholders(
                            placeholder,
                            TablePlaceholder.REPLACE_SCHEMA_FULL_NAME_KEY.getPlaceholder(),
                            String.join(NAME_DELIMITER, fullPath),
                            defaultValue);
        }

        if (identifier.getTableName() != null) {
            fullPath.add(identifier.getTableName());
        }
        if (!fullPath.isEmpty()) {
            placeholder =
                    replacePlaceholders(
                            placeholder,
                            TablePlaceholder.REPLACE_TABLE_FULL_NAME_KEY.getPlaceholder(),
                            String.join(NAME_DELIMITER, fullPath),
                            defaultValue);
        }
        return placeholder;
    }

    public static String replaceTableIdentifier(String placeholder, TableIdentifier identifier) {
        return replaceTableIdentifier(placeholder, identifier, "");
    }

    public static String replaceTablePrimaryKey(String placeholder, PrimaryKey primaryKey) {
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            String pkFieldsString = String.join(FIELD_DELIMITER, primaryKey.getColumnNames());
            return replacePlaceholders(
                    placeholder,
                    TablePlaceholder.REPLACE_PRIMARY_KEY.getPlaceholder(),
                    pkFieldsString);
        }
        return placeholder;
    }

    public static String replaceTableUniqueKey(
            String placeholder, List<ConstraintKey> constraintKeys) {
        Optional<String> ukFieldsString =
                constraintKeys.stream()
                        .filter(
                                e ->
                                        e.getConstraintType()
                                                .equals(ConstraintKey.ConstraintType.UNIQUE_KEY))
                        .findFirst()
                        .map(
                                e ->
                                        e.getColumnNames().stream()
                                                .map(f -> f.getColumnName())
                                                .collect(Collectors.joining(FIELD_DELIMITER)));
        if (ukFieldsString.isPresent()) {
            return replacePlaceholders(
                    placeholder,
                    TablePlaceholder.REPLACE_UNIQUE_KEY.getPlaceholder(),
                    ukFieldsString.get());
        }
        return placeholder;
    }

    public static String replaceTableFieldNames(String placeholder, TableSchema schema) {
        return replacePlaceholders(
                placeholder,
                TablePlaceholder.REPLACE_FIELD_NAMES_KEY.getPlaceholder(),
                String.join(FIELD_DELIMITER, schema.getFieldNames()));
    }

    public static ReadonlyConfig replaceTablePlaceholder(
            ReadonlyConfig config, CatalogTable table) {
        return replaceTablePlaceholder(config, table, Collections.emptyList());
    }

    public static ReadonlyConfig replaceTablePlaceholder(
            ReadonlyConfig config, CatalogTable table, Collection<String> excludeKeys) {
        Map<String, Object> copyOnWriteData = ObjectUtils.clone(config.getSourceMap());
        for (String key : copyOnWriteData.keySet()) {
            if (excludeKeys.contains(key)) {
                continue;
            }
            Object value = copyOnWriteData.get(key);
            if (value != null) {
                if (value instanceof String) {
                    String strValue = (String) value;
                    strValue = replaceTableIdentifier(strValue, table.getTableId());
                    strValue =
                            replaceTablePrimaryKey(
                                    strValue, table.getTableSchema().getPrimaryKey());
                    strValue =
                            replaceTableUniqueKey(
                                    strValue, table.getTableSchema().getConstraintKeys());
                    strValue = replaceTableFieldNames(strValue, table.getTableSchema());
                    copyOnWriteData.put(key, strValue);
                } else if (value instanceof List) {
                    List listValue = (List) value;
                    if (listValue.size() == 1 && listValue.get(0) instanceof String) {
                        String strValue = (String) listValue.get(0);
                        if (strValue.equals(
                                "${"
                                        + TablePlaceholder.REPLACE_PRIMARY_KEY.getPlaceholder()
                                        + "}")) {
                            strValue =
                                    replaceTablePrimaryKey(
                                            strValue, table.getTableSchema().getPrimaryKey());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        } else if (strValue.equals(
                                "${"
                                        + TablePlaceholder.REPLACE_UNIQUE_KEY.getPlaceholder()
                                        + "}")) {
                            strValue =
                                    replaceTableUniqueKey(
                                            strValue, table.getTableSchema().getConstraintKeys());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        } else if (strValue.equals(
                                "${"
                                        + TablePlaceholder.REPLACE_FIELD_NAMES_KEY.getPlaceholder()
                                        + "}")) {
                            strValue = replaceTableFieldNames(strValue, table.getTableSchema());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        }
                        copyOnWriteData.put(key, listValue);
                    }
                }
            }
        }
        return ReadonlyConfig.fromMap(copyOnWriteData);
    }
}
