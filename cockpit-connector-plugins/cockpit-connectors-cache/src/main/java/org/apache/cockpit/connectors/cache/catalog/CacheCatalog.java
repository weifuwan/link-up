package org.apache.cockpit.connectors.cache.catalog;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.catalog.exception.*;
import org.apache.cockpit.connectors.api.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;
import org.apache.cockpit.connectors.cache.dialect.CacheTypeConverter;
import org.apache.cockpit.connectors.cache.dialect.CacheTypeMapper;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class CacheCatalog extends AbstractJdbcCatalog {

    private static final String LIST_TABLES_SQL_TEMPLATE =
            "SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE TABLE_SCHEMA='%s' and TABLE_TYPE != 'SYSTEM TABLE' and TABLE_TYPE != 'SYSTEM VIEW'";

    public CacheCatalog(
            String catalogName,
            String username,
            String password,
            JdbcUrlUtil.UrlInfo urlInfo,
            String driverClass,
            String driverLocation) {
        super(catalogName, username, password, urlInfo, null, driverClass, driverLocation);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new CacheCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    public String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE \"%s\"", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE \"%s\"", databaseName);
    }

    @Override
    protected String getListTableSql(String tableSchemaName) {
        return String.format(LIST_TABLES_SQL_TEMPLATE, tableSchemaName);
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        String schemaName = rs.getString(1);
        String tableName = rs.getString(2);
        // It's the system schema when schema name start with %
        if (schemaName.startsWith("%")) {
            return null;
        }
        return schemaName + "." + tableName;
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        Long columnLength = resultSet.getLong("COLUMN_SIZE");
        Long columnPrecision = columnLength;
        Integer columnScale = resultSet.getObject("DECIMAL_DIGITS", Integer.class);
        String columnComment = resultSet.getString("REMARKS");
        Object defaultValue = resultSet.getObject("COLUMN_DEF");
        boolean isNullable = (resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return CacheTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        throw new SeaTunnelException("Not supported for list databases for cache");
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return querySQLResultExists(
                    this.getUrlFromDatabaseName(tablePath.getDatabaseName()),
                    getTableWithConditionSql(tablePath));
        } catch (SQLException e) {
            throw new SeaTunnelException("Failed to querySQLResult", e);
        }
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getSchemaName()) + " and TABLE_NAME = '%s'",
                tablePath.getTableName());
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    public List<String> listTables(String schemaName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return queryString(defaultUrl, getListTableSql(schemaName), this::getTableName);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new CacheTypeMapper());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl;
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        } else {
            dbUrl = getUrlFromDatabaseName(defaultDatabase);
        }
        try {
            Connection conn = getConnection(dbUrl);
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet resultSet =
                         metaData.getColumns(
                                 null, tablePath.getSchemaName(), tablePath.getTableName(), null)) {
                Optional<PrimaryKey> primaryKey = getPrimaryKey(metaData, tablePath);
                List<ConstraintKey> constraintKeys = getConstraintKeys(metaData, tablePath);
                TableSchema.Builder builder = TableSchema.builder();
                buildColumnsWithErrorCheck(tablePath, resultSet, builder);
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        catalogName);
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");
        createDatabaseInternal(tablePath.getDatabaseName());
    }

    @Override
    public void createTable(
            TablePath tablePath, CatalogTable table, boolean ignoreIfExists, boolean createIndex)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        if (defaultSchema.isPresent()) {
            tablePath =
                    new TablePath(
                            tablePath.getDatabaseName(),
                            defaultSchema.get(),
                            tablePath.getTableName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        createTableInternal(tablePath, table, createIndex);
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        truncateTableInternal(tablePath);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(tablePath.getDatabaseName(), "Database name cannot be null");
        dropDatabaseInternal(tablePath.getDatabaseName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT TOP 1 * FROM \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @VisibleForTesting
    public void setConnection(String url, Connection connection) {
        this.connectionMap.put(url, connection);
    }
}
