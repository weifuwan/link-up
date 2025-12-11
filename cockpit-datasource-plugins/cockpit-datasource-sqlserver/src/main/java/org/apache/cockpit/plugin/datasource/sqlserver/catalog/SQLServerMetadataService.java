package org.apache.cockpit.plugin.datasource.sqlserver.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.sqlserver.param.SQLServerConnectionParam;
import org.apache.cockpit.plugin.datasource.sqlserver.param.SQLServerDataSourceProcessor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SQLServerMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {

        if (!(connectionParam instanceof SQLServerConnectionParam)) {
            throw new IllegalArgumentException("连接参数必须是SQLServerConnectionParam类型");
        }

        SQLServerConnectionParam sqlServerConnectionParam = (SQLServerConnectionParam) connectionParam;
        String database = sqlServerConnectionParam.getDatabase();
        String schema = sqlServerConnectionParam.getSchema();

        String sql = getListTableSql(database, schema);

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(sql)) {

            // 设置查询参数
            setQueryParameters(ps, database, schema);

            try (ResultSet rs = ps.executeQuery()) {
                List<String> result = new ArrayList<>();
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    result.add(tableName);
                }
                log.debug("成功获取 {} 个表", result.size());
                return result;
            }

        } catch (Exception e) {
            log.error("获取表列表失败 - 数据库: {}, 模式: {}: {}", database, schema, e.getMessage(), e);
            throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
        }
    }

    /**
     * 构建查询SQL
     *
     * @param catalogName 数据库名（catalog）
     * @param schema      模式名
     * @return SQL语句
     */
    private static String getListTableSql(String catalogName, String schema) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT TABLE_NAME ");
        sql.append("FROM INFORMATION_SCHEMA.TABLES ");
        sql.append("WHERE TABLE_TYPE = 'BASE TABLE' ");

        // 添加过滤条件
        if (isNotEmpty(catalogName)) {
            sql.append("AND TABLE_CATALOG = ? ");
        }

        if (isNotEmpty(schema)) {
            sql.append("AND TABLE_SCHEMA = ? ");
        }

        sql.append("ORDER BY TABLE_SCHEMA, TABLE_NAME");

        return sql.toString();
    }

    /**
     * 设置查询参数
     *
     * @param ps          PreparedStatement
     * @param catalogName 数据库名
     * @param schema      模式名
     * @throws Exception 异常
     */
    private static void setQueryParameters(PreparedStatement ps, String catalogName, String schema) throws Exception {
        int paramIndex = 1;

        if (isNotEmpty(catalogName)) {
            ps.setString(paramIndex++, catalogName);
        }

        if (isNotEmpty(schema)) {
            ps.setString(paramIndex++, schema);
        }
    }

    /**
     * 检查字符串是否非空
     *
     * @param str 字符串
     * @return 是否非空
     */
    private static boolean isNotEmpty(String str) {
        return str != null && !str.trim().isEmpty();
    }


    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam,
                                                          String tableName,
                                                          DataSourceProcessor processor) {

        if (!(connectionParam instanceof SQLServerConnectionParam)) {
            throw new IllegalArgumentException("连接参数必须是SQLServerConnectionParam类型");
        }

        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("表名不能为空");
        }

        SQLServerConnectionParam sqlServerConnectionParam = (SQLServerConnectionParam) connectionParam;
        String schema = sqlServerConnectionParam.getSchema();

        String sql = getListColumnsSqlUsingSysObjects(schema);

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(sql)) {

            setSysColumnsQueryParameters(ps, schema, tableName);

            try (ResultSet rs = ps.executeQuery()) {
                List<DataSourceTableColumn> result = new ArrayList<>();
                while (rs.next()) {
                    DataSourceTableColumn column = mapSysResultSetToColumn(rs);
                    result.add(column);
                }

                log.debug("成功获取表 {} 的 {} 个列（系统视图）", tableName, result.size());
                return result;
            }

        } catch (Exception e) {
            log.error("获取表列信息失败（系统视图）- 表: {}, 模式: {}: {}",
                    tableName, schema, e.getMessage(), e);
            throw new RuntimeException("获取表列信息失败: " + e.getMessage(), e);
        }
    }

    /**
     * 使用系统视图的SQL
     */
    private static String getListColumnsSqlUsingSysObjects(String schema) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        sql.append("    c.name AS COLUMN_NAME, ");
        sql.append("    t.name AS DATA_TYPE, ");
        sql.append("    c.max_length AS CHARACTER_MAXIMUM_LENGTH, ");
        sql.append("    c.precision AS NUMERIC_PRECISION, ");
        sql.append("    c.scale AS NUMERIC_SCALE, ");
        sql.append("    c.is_nullable AS IS_NULLABLE, ");
        sql.append("    OBJECT_DEFINITION(c.default_object_id) AS COLUMN_DEFAULT, ");
        sql.append("    c.column_id AS ORDINAL_POSITION, ");
        sql.append("    c.is_identity AS IS_IDENTITY, ");
        sql.append("    CASE ");
        sql.append("        WHEN pk.column_id IS NOT NULL THEN 1 ");
        sql.append("        ELSE 0 ");
        sql.append("    END AS IS_PRIMARY_KEY ");
        sql.append("FROM sys.columns c ");
        sql.append("INNER JOIN sys.types t ON c.user_type_id = t.user_type_id ");
        sql.append("INNER JOIN sys.objects o ON c.object_id = o.object_id ");
        sql.append("LEFT JOIN ( ");
        sql.append("    SELECT ic.object_id, ic.column_id ");
        sql.append("    FROM sys.index_columns ic ");
        sql.append("    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id ");
        sql.append("    WHERE i.is_primary_key = 1 ");
        sql.append(") pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id ");
        sql.append("WHERE o.name = ? ");
        sql.append("AND o.type = 'U' "); // 只查询用户表

        if (isNotEmpty(schema)) {
            sql.append("AND SCHEMA_NAME(o.schema_id) = ? ");
        }

        sql.append("ORDER BY c.column_id");

        return sql.toString();
    }

    /**
     * 设置系统视图查询参数
     */
    private static void setSysColumnsQueryParameters(PreparedStatement ps, String schema, String tableName) throws SQLException {
        int paramIndex = 1;

        ps.setString(paramIndex++, tableName);

        if (isNotEmpty(schema)) {
            ps.setString(paramIndex++, schema);
        }
    }

    /**
     * 映射系统视图结果集
     */
    private static DataSourceTableColumn mapSysResultSetToColumn(ResultSet rs) throws SQLException {
        DataSourceTableColumn column = new DataSourceTableColumn();

        // 基本信息
        column.setColumnName(rs.getString("COLUMN_NAME"));
        column.setColumnType(rs.getString("DATA_TYPE"));
        column.setSourceType(rs.getString("DATA_TYPE"));

        return column;
    }

    public static QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           SQLServerDataSourceProcessor processor) {
        String tablePath = requestBody.get("table_path").toString();
        TaskExecutionTypeEnum taskExecuteType = TaskExecutionTypeEnum.valueOf(requestBody.get("taskExecuteType").toString());
        String query = requestBody.get("query").toString();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = processor.getConnection(connectionParam);
            String finalQuery;

            switch (taskExecuteType) {
                case SINGLE_TABLE:
                    finalQuery = buildTop10Query(tablePath);
                    preparedStatement = connection.prepareStatement(finalQuery);
                    break;

                case SINGLE_TABLE_CUSTOM:
                    finalQuery = addLimitClause(query, 10);
                    preparedStatement = connection.prepareStatement(finalQuery);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported execution type: " + taskExecuteType);
            }

            resultSet = preparedStatement.executeQuery();

            return processResultSet(resultSet);

        } catch (SQLException e) {
            throw new RuntimeException("Database query failed", e);
        } catch (Exception e) {
            throw new RuntimeException("Data processing failed", e);
        } finally {
            closeResources(connection, preparedStatement, resultSet);
        }
    }

    /**
     * Build SQL to query top 20 records
     */
    private static String buildTop10Query(String tablePath) {
        // Clean table name to prevent SQL injection
        String cleanTableName = tablePath.replaceAll("[^a-zA-Z0-9_]", "");
        return "SELECT TOP 10 * FROM " + cleanTableName;
    }

    /**
     * Add LIMIT clause to query
     */
    private static String addLimitClause(String query, int limit) {
        String upperQuery = query.toUpperCase().trim();

        // If query already contains LIMIT, use the original LIMIT
        if (upperQuery.contains("LIMIT")) {
            return query;
        }

        // If query ends with semicolon, remove it
        if (query.trim().endsWith(";")) {
            query = query.trim().substring(0, query.trim().length() - 1);
        }

        return query + " LIMIT " + limit;
    }

    /**
     * Process result set and build QueryResult
     */
    private static QueryResult processResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<FrontedTableColumn> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            FrontedTableColumn column = new FrontedTableColumn();
            column.setTitle(columnName);
            column.setDataIndex(columnName);
            column.setKey(columnName);
            column.setEllipsis(true);
            columns.add(column);
        }

        List<Map<String, Object>> data = new ArrayList<>();
        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getString(i);
                row.put(columnName, value);
            }
            data.add(row);
        }

        return new QueryResult(columns, data);
    }

    /**
     * Close database resources
     */
    private static void closeResources(Connection connection, PreparedStatement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println("Error occurred while closing database resources: " + e.getMessage());
        }
    }

}