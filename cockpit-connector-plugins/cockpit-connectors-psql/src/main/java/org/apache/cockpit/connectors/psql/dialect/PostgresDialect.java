package org.apache.cockpit.connectors.psql.dialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.JdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.cockpit.connectors.api.jdbc.dialect.SQLUtils;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.psql.dialect.PostgresTypeConverter.*;

@Slf4j
public class PostgresDialect implements JdbcDialect {

    private static final long serialVersionUID = -5834746193472465218L;
    public static final int DEFAULT_POSTGRES_FETCH_SIZE = 128;

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public PostgresDialect() {
    }

    public PostgresDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new PostgresTypeMapper();
    }

    @Override
    public String hashModForField(String nativeType, String fieldName, int mod) {
        String quoteFieldName = quoteIdentifier(fieldName);
        if (StringUtils.isNotBlank(nativeType)) {
            quoteFieldName = convertType(quoteFieldName, nativeType);
        }
        return "(ABS(HASHTEXT(" + quoteFieldName + ")) % " + mod + ")";
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return hashModForField(null, fieldName, mod);
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        Map<String, Column> columns =
                table.getCatalogTable().getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, c -> c));
        Column column = columns.get(columnName);

        String quotedColumn = quoteIdentifier(columnName);
        quotedColumn = convertType(quotedColumn, column.getSourceType());
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM (%s) AS T1 WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                    + ") AS T2",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                    + ") AS T",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        }
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                } else {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
            }
        }
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, TableSchema tableSchema, String[] uniqueKeyFields) {
        String[] fieldNames = tableSchema.getFieldNames();
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        final Set<String> uniqueKeyFieldsSet = new HashSet<>(Arrays.asList(uniqueKeyFields));
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !uniqueKeyFieldsSet.contains(fieldName))
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=EXCLUDED."
                                                + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String conflictAction =
                updateClause.isEmpty()
                        ? "DO NOTHING"
                        : String.format("DO UPDATE SET %s", updateClause);
        String upsertSQL =
                String.format(
                        "%s ON CONFLICT (%s) %s",
                        getInsertIntoStatement(database, tableName, fieldNames),
                        uniqueColumns,
                        conflictAction);
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // use cursor mode, reference:
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        connection.setAutoCommit(false);
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_POSTGRES_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        // resolve pg database name upper or lower not recognised
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }

        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tablePath.getFullNameWithQuoted("\"");
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {

        // 1. If no query is configured, use TABLE STATUS.
        // 2. If a query is configured but does not contain a WHERE clause and tablePath is
        // configured, use TABLE STATUS.
        // 3. If a query is configured with a WHERE clause, or a query statement is configured but
        // tablePath is TablePath.DEFAULT, use COUNT(*).

        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                        && table.getTablePath() != null
                        && !TablePath.DEFAULT
                        .getFullName()
                        .equals(table.getTablePath().getFullName()));
        if (useTableStats) {
            String rowCountQuery =
                    String.format(
                            "SELECT reltuples FROM pg_class r WHERE relkind = 'r' AND relname = '%s';",
                            table.getTablePath().getTableName());
            try (Statement stmt = connection.createStatement()) {
                log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(1);
                }
            }
        }
        return SQLUtils.countForSubquery(connection, table.getQuery());
    }

    @Override
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return PostgresTypeConverter.INSTANCE;
    }


    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        String pgDataType = columnDefine.getDataType().toLowerCase();
        switch (pgDataType) {
            case PG_CHAR:
            case PG_VARCHAR:
            case PG_TEXT:
            case PG_CHARACTER:
            case PG_XML:
                return true;
            default:
                return false;
        }
    }


    public String convertType(String columnName, String columnType) {
        if (PostgresTypeConverter.PG_UUID.equals(columnType)) {
            return columnName + "::text";
        }
        return columnName;
    }
}
