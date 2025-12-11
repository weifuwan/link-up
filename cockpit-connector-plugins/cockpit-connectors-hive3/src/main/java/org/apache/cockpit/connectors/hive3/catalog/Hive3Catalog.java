package org.apache.cockpit.connectors.hive3.catalog;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.catalog.exception.*;
import org.apache.cockpit.connectors.api.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;
import org.apache.cockpit.connectors.hive3.dialect.HiveType;
import org.apache.cockpit.connectors.hive3.dialect.HiveTypeConverter;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class Hive3Catalog extends AbstractJdbcCatalog {

    protected final Map<String, Connection> connectionMap;

    private final String driverClass;
    private final String driverLocation;

    private Connection conn;

    private final HiveTypeConverter hiveTypeConverter = new HiveTypeConverter();

    protected Hive3Catalog(String catalogName,
                           String username,
                           String pwd,
                           JdbcUrlUtil.UrlInfo urlInfo,
                           String driverClass,
                           String driverLocation,
                           String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, driverClass, driverLocation, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
        this.driverClass = driverClass;
        this.driverLocation = driverLocation;
    }

    @Override
    public void open() throws CatalogException {
        String jdbcUrl = baseUrl;
        try {
            if (driverClass != null && driverLocation != null) {
                // 加载驱动
                Class.forName(driverClass);
            }
            conn = DriverManager.getConnection(jdbcUrl, username, pwd);
            connectionMap.put("default", conn);
        } catch (SQLException | ClassNotFoundException e) {
            throw new CatalogException(String.format("Failed to connect url %s", jdbcUrl), e);
        }
        log.info("Catalog {} established connection to {} success", catalogName, jdbcUrl);
    }

    @Override
    public void close() throws CatalogException {
        try {
            for (Connection connection : connectionMap.values()) {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
            connectionMap.clear();
            log.info("Catalog {} closed all connections", catalogName);
        } catch (SQLException e) {
            throw new CatalogException("Failed to close connections", e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            List<String> databases = listDatabases();
            return databases.contains(databaseName.toLowerCase());
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to check if database %s exists", databaseName), e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        String sql = "SHOW DATABASES";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                databases.add(rs.getString(1).toLowerCase());
            }
            return databases;

        } catch (SQLException e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        List<String> tables = new ArrayList<>();
        String sql = String.format("SHOW TABLES IN `%s`", databaseName);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                tables.add(rs.getString(1).toLowerCase());
            }
            return tables;

        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @SneakyThrows
    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();

        try {
            List<String> tables = listTables(databaseName);
            return tables.contains(tableName.toLowerCase());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<String> comment = getTableComment(metaData, tablePath);
            Optional<PrimaryKey> primaryKey = Optional.empty();
            List<ConstraintKey> constraintKeys = new ArrayList<>();
            TableSchema.Builder tableSchemaBuilder =
                    buildColumnsReturnTablaSchemaBuilder(tablePath, conn);
            // add primary key
            primaryKey.ifPresent(tableSchemaBuilder::primaryKey);
            // add constraint key
            constraintKeys.forEach(tableSchemaBuilder::constraintKey);

            TableIdentifier tableIdentifier = getTableIdentifier(tablePath);
            return CatalogTable.of(
                    tableIdentifier,
                    tableSchemaBuilder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    comment.orElse(""),
                    catalogName);

        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    protected Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", getUrlFromDatabaseName(tablePath.getDatabaseName()));
        options.put("table-name", getOptionTableName(tablePath));
        return options;
    }

    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getFullName();
    }


    protected String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
    }

    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
    }


    protected TableSchema.Builder buildColumnsReturnTablaSchemaBuilder(
            TablePath tablePath, Connection conn) throws SQLException {
        TableSchema.Builder columnsBuilder = TableSchema.builder();
        try (PreparedStatement ps = conn.prepareStatement(getSelectColumnsSql(tablePath));
             ResultSet resultSet = ps.executeQuery()) {
            buildColumnsWithErrorCheck(tablePath, resultSet, columnsBuilder);
        }
        return columnsBuilder;
    }

    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format("DESCRIBE `%s`.`%s`",
                tablePath.getDatabaseName(),
                tablePath.getTableName());
    }

    protected void buildColumnsWithErrorCheck(
            TablePath tablePath, ResultSet resultSet, TableSchema.Builder builder)
            throws SQLException {
        Map<String, String> unsupported = new LinkedHashMap<>();
        while (resultSet.next()) {
            try {
                builder.column(buildColumn(resultSet));
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    catalogName, tablePath.getFullName(), unsupported);
        }
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        // Hive DESCRIBE返回的格式: 字段名 | 字段类型 | 注释
        String columnName = resultSet.getString(1); // 第一列：字段名
        String fullType = resultSet.getString(2);   // 第二列：完整字段类型，如decimal(20,0)
        String comment = resultSet.getString(3);    // 第三列：注释

        // 解析字段类型
        String columnType = fullType;
        String dataType = extractDataType(fullType);

        Object defaultValue = null;

        boolean isNullable = true;

        TypeParameters params = parseTypeParameters(fullType, dataType);

        boolean unsigned = false;

        HiveType hiveType = HiveType.getByName(dataType.toUpperCase());

        BasicTypeDefine<HiveType> typeDefine =
                BasicTypeDefine.<HiveType>builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType.toUpperCase())
                        .nativeType(hiveType)
                        .unsigned(unsigned)
                        .length(Math.max(params.charOctetLength, params.numberPrecision))
                        .precision(params.numberPrecision)
                        .scale(params.numberScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment != null ? comment : "")
                        .build();
        return hiveTypeConverter.convert(typeDefine);
    }

    private String extractDataType(String fullType) {
        if (fullType == null || fullType.isEmpty()) {
            return "string";
        }

        fullType = fullType.trim().toLowerCase();

        int parenIndex = fullType.indexOf('(');
        if (parenIndex > 0) {
            return fullType.substring(0, parenIndex);
        }

        if (fullType.endsWith("[]")) {
            return fullType.substring(0, fullType.length() - 2);
        }

        return fullType;
    }

    private static class TypeParameters {
        long numberPrecision = 0;
        int numberScale = 0;
        long charOctetLength = 0;
    }

    private TypeParameters parseTypeParameters(String fullType, String dataType) {
        TypeParameters params = new TypeParameters();

        if (fullType == null) {
            return params;
        }

        fullType = fullType.toLowerCase();
        int openParen = fullType.indexOf('(');
        int closeParen = fullType.indexOf(')');

        if (openParen > 0 && closeParen > openParen) {
            String paramStr = fullType.substring(openParen + 1, closeParen);
            String[] parts = paramStr.split(",");

            if (parts.length >= 1) {
                try {
                    long firstParam = Long.parseLong(parts[0].trim());

                    switch (dataType.toLowerCase()) {
                        case "decimal":
                        case "numeric":
                        case "dec":
                            params.numberPrecision = firstParam;
                            if (parts.length >= 2) {
                                params.numberScale = Integer.parseInt(parts[1].trim());
                            }
                            break;
                        case "varchar":
                        case "char":
                            params.charOctetLength = firstParam;
                            break;
                        case "string":
                            params.charOctetLength = firstParam;
                            break;
                        default:
                            params.numberPrecision = firstParam;
                    }
                } catch (NumberFormatException e) {
                    log.debug("Failed to parse type parameters for type: {}", fullType);
                }
            }
        }

        if (params.charOctetLength == 0 && params.numberPrecision == 0) {
            switch (dataType.toLowerCase()) {
                case "string":
                    params.charOctetLength = 65535;
                    break;
                case "varchar":
                    params.charOctetLength = 255;
                    break;
                case "char":
                    params.charOctetLength = 255;
                    break;
            }
        }

        return params;
    }

    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getConstraintKeys(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getConstraintKeys(metaData, TablePath.of(database, schema, table));
    }

    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getPrimaryKey(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    protected Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getPrimaryKey(metaData, TablePath.of(database, schema, table));
    }


    protected Optional<String> getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        return getTableComment(
                metaData,
                tablePath.getDatabaseName(),
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    protected Optional<String> getTableComment(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        return CatalogUtils.getTableComment(metaData, TablePath.of(database, schema, table));
    }

    @SneakyThrows
    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        String databaseName = tablePath.getDatabaseName();

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                log.info("Table {} already exists, skip creation", tablePath);
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        HiveCreateTableSqlBuilder sqlBuilder = HiveCreateTableSqlBuilder.builder(
                tablePath,
                table,
                hiveTypeConverter,
                true);

        String createTableSql = sqlBuilder.build(databaseName);

        log.info("Creating table with SQL: {}", createTableSql);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
            log.info("Created table: {}", tablePath);

            List<String> indexSqls = sqlBuilder.buildIndexSqls(databaseName);
            if (indexSqls != null && !indexSqls.isEmpty()) {
                for (String indexSql : indexSqls) {
                    try {
                        stmt.execute(indexSql);
                        log.debug("Created index with SQL: {}", indexSql);
                    } catch (SQLException e) {
                        log.warn("Failed to create index, but table was created successfully: {}", e.getMessage());
                    }
                }
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create table %s. SQL: %s", tablePath, createTableSql), e);
        }
    }

    @SneakyThrows
    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {

        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                log.info("Table {} does not exist, skip dropping", tablePath);
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        String sql = String.format("DROP TABLE `%s`.`%s`",
                tablePath.getDatabaseName(), tablePath.getTableName());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Dropped table: {}", tablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop table %s", tablePath), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {

        String databaseName = tablePath.getDatabaseName();

        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                log.info("Database {} already exists, skip creation", databaseName);
                return;
            }
            throw new DatabaseAlreadyExistException(catalogName, databaseName);
        }

        String sql = String.format("CREATE DATABASE `%s`", databaseName);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Created database: {}", databaseName);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create database %s", databaseName), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {

        String databaseName = tablePath.getDatabaseName();

        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                log.info("Database {} does not exist, skip dropping", databaseName);
                return;
            }
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        String sql = String.format("DROP DATABASE `%s`", databaseName);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Dropped database: {}", databaseName);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop database %s", databaseName), e);
        }
    }

    @SneakyThrows
    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {

        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                log.info("Table {} does not exist, skip truncating", tablePath);
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        String sql = String.format("TRUNCATE TABLE `%s`.`%s`",
                tablePath.getDatabaseName(), tablePath.getTableName());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            log.info("Truncated table: {}", tablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to truncate table %s", tablePath), e);
        }
    }

    @SneakyThrows
    @Override
    public boolean isExistsData(TablePath tablePath) {
        if (!tableExists(tablePath)) {
            return false;
        }

        String sql = String.format("SELECT 1 FROM `%s`.`%s` LIMIT 1",
                tablePath.getDatabaseName(), tablePath.getTableName());

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            return rs.next(); // 如果有结果，说明有数据

        } catch (SQLException e) {
            log.warn("Failed to check if data exists in table {}", tablePath, e);
            return false;
        }
    }
}