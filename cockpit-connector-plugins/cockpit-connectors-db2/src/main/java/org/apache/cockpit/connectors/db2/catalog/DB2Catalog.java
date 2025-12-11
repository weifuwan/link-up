package org.apache.cockpit.connectors.db2.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.ConstraintKey;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.CatalogException;
import org.apache.cockpit.connectors.api.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;
import org.apache.cockpit.connectors.db2.dialect.DB2TypeConverter;
import org.apache.cockpit.connectors.db2.dialect.DB2TypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Henry
 * @date 2025/12/4
 */
@Slf4j
public class DB2Catalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n" +
                    "    colname AS COLUMN_NAME,\n" +
                    "    typename AS TYPE_NAME,\n" +
                    "    CASE\n" +
                    "        WHEN typename IN ('VARCHAR', 'CHAR', 'GRAPHIC', 'VARGRAPHIC') THEN typename || '(' || length || ')'\n" +
                    "        WHEN typename IN ('DECIMAL', 'NUMERIC') AND scale > 0 THEN typename || '(' || length || ',' || scale || ')'\n" +
                    "        WHEN typename IN ('DECIMAL', 'NUMERIC') AND scale = 0 THEN typename || '(' || length || ')'\n" +
                    "        ELSE typename\n" +
                    "    END AS FULL_TYPE_NAME,\n" +
                    "    length AS COLUMN_LENGTH,\n" +
                    "    length AS COLUMN_PRECISION,\n" +
                    "    scale AS COLUMN_SCALE,\n" +
                    "    COALESCE(remarks, '') AS COLUMN_COMMENT,\n" +
                    "    default AS DEFAULT_VALUE,\n" +
                    "    CASE nulls WHEN 'Y' THEN 'YES' ELSE 'NO' END AS IS_NULLABLE\n" +
                    "FROM syscat.columns\n" +
                    "WHERE tabschema = '%s'\n" +
                    "    AND tabname = '%s'\n" +
                    "ORDER BY colno";

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass,
            String driverJarName) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass, driverJarName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return getListTableSql(tablePath.getDatabaseName())
                + "  AND  tabschema = '"
                + tablePath.getSchemaName()
                + "' AND tabname = '"
                + tablePath.getTableName()
                + "'";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>(Collections.singletonList("default"));
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new DB2CreateTableSqlBuilder(table, createIndex).build(tablePath).get(0);
    }

    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new DB2CreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT tabschema, tabname FROM syscat.tables\n" +
                "WHERE type = 'T'\n" +
                "    AND tabschema NOT IN ('SYSIBM', 'SYSCAT', 'SYSSTAT', 'SYSIBMADM', 'SYSTOOLS')";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        Long columnPrecision = resultSet.getObject("COLUMN_PRECISION", Long.class);
        Integer columnScale = resultSet.getObject("COLUMN_SCALE", Integer.class);
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("YES");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(fullTypeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return new DB2TypeConverter()
                .convert(typeDefine);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(
                defaultConnection,
                sqlQuery,
                new DB2TypeMapper());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\" IMMEDIATE",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select * from \"%s\".\"%s\" FETCH FIRST 1 ROWS ONLY",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try {
            return getConstraintKeys(
                    metaData,
                    tablePath.getDatabaseName(),
                    tablePath.getSchemaName(),
                    tablePath.getTableName());
        } catch (SQLException e) {
            log.info("Obtain constraint failure", e);
            return new ArrayList<>();
        }
    }
}
