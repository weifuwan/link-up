package org.apache.cockpit.connectors.db2.dialect;


import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectTypeMapper;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class DB2Dialect implements JdbcDialect {

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DB2JdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DB2TypeMapper();
    }


    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        // Generate field list for USING and INSERT clauses
        String fieldList = String.join(", ", tableSchema.getFieldNames());

        // Generate placeholder list for VALUES clause
        String placeholderList =
                Arrays.stream(tableSchema.getFieldNames()).map(field -> "?").collect(Collectors.joining(", "));

        // Generate ON clause
        String onClause =
                Arrays.stream(uniqueKeyFields)
                        .map(field -> "target." + field + " = source." + field)
                        .collect(Collectors.joining(" AND "));

        // Generate WHEN MATCHED clause
        String whenMatchedClause =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(field -> "target." + field + " <> source." + field)
                        .collect(Collectors.joining(" OR "));

        // Generate UPDATE SET clause
        String updateSetClause =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(field -> "target." + field + " = source." + field)
                        .collect(Collectors.joining(", "));

        // Generate WHEN NOT MATCHED clause
        String insertClause =
                "INSERT ("
                        + fieldList
                        + ") VALUES ("
                        + Arrays.stream(tableSchema.getFieldNames())
                        .map(field -> "source." + field)
                        .collect(Collectors.joining(", "))
                        + ")";

        // Combine all parts to form the final SQL statement
        String mergeStatement =
                String.format(
                        "MERGE INTO %s.%s AS target USING (VALUES (%s)) AS source (%s) ON %s "
                                + "WHEN MATCHED AND (%s) THEN UPDATE SET %s "
                                + "WHEN NOT MATCHED THEN %s;",
                        database,
                        tableName,
                        placeholderList,
                        fieldList,
                        onClause,
                        whenMatchedClause,
                        updateSetClause,
                        insertClause);

        return Optional.of(mergeStatement);
    }

    @Override
    public String dualTable() {
        return " FROM SYSIBM.SYSDUMMY1 ";
    }
}
