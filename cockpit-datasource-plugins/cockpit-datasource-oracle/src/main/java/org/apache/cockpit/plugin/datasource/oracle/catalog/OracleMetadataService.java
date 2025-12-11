package org.apache.cockpit.plugin.datasource.oracle.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleConnectionParam;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleDataSourceProcessor;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class OracleMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        OracleConnectionParam connectionParam1 = (OracleConnectionParam) connectionParam;
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(getListTableSql(connectionParam1.getUsername().toUpperCase()));
             ResultSet rs = ps.executeQuery()) {

            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String value = rs.getString(1);
                result.add(value);
            }
            return result;

        } catch (Exception e) {
            log.error("获取表列表失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName, DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        OracleConnectionParam oracleConnectionParam = (OracleConnectionParam) connectionParam;
        String catalog = oracleConnectionParam.getDatabase();
        String schema = oracleConnectionParam.getUsername().toUpperCase();

        try (Connection connection = processor.getConnection(connectionParam);
             ResultSet rs = connection.getMetaData().getColumns(catalog, schema, tableName, null)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String nativeType = rs.getString("TYPE_NAME");
                DataSourceTableColumn typeDefine = DataSourceTableColumn.builder()
                        .columnName(columnName)
                        .sourceType(nativeType)
                        .build();

                if (nativeType.startsWith("TIMESTAMP")) {
                    typeDefine.setColumnType("TIMESTAMP");
                } else {
                    switch (nativeType) {
                        case "INT":
                        case "BIGINT":
                        case "DECIMAL":
                        case "INT UNSIGNED":
                        case "BIGINT UNSIGNED":
                        case "FLOAT":
                        case "TINYINT":
                        case "TINYINT UNSIGNED":
                        case "DOUBLE":
                            typeDefine.setColumnType("NUMBER");
                            break;
                        case "DATE":
                        case "DATETIME":
                            typeDefine.setColumnType("TIMESTAMP");
                            break;
                        default:
                            typeDefine.setColumnType("STRING");
                            break;
                    }
                }

                columns.add(typeDefine);
            }
        } catch (Exception e) {
            log.error("获取表 {} 的字段列表失败: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("获取字段失败: " + e.getMessage(), e);
        }
        return columns;
    }

    private static String getListTableSql(String OWNER) {
        return "SELECT TABLE_NAME FROM ALL_TABLES"
                + "  WHERE TABLE_NAME NOT LIKE 'MDRT_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDRS_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDXT_%'"
                + "  AND (TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL) AND OWNER = '" + OWNER + "'";
    }

    public static QueryResult getTop10Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           OracleDataSourceProcessor processor) {

        TaskExecutionTypeEnum taskExecuteType = TaskExecutionTypeEnum.valueOf(requestBody.get("taskExecuteType").toString());

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = processor.getConnection(connectionParam);
            String finalQuery;

            switch (taskExecuteType) {
                case SINGLE_TABLE:
                    String tablePath = requestBody.get("table_path").toString();
                    if (StringUtils.isBlank(tablePath)) {
                        throw new RuntimeException("table is null");
                    }
                    OracleConnectionParam oracleConnectionParam = (OracleConnectionParam) connectionParam;
                    finalQuery = buildTop10Query(tablePath, oracleConnectionParam.getUsername().toUpperCase());
                    preparedStatement = connection.prepareStatement(finalQuery);
                    break;

                case SINGLE_TABLE_CUSTOM:
                    String query = requestBody.get("query").toString();
                    if (StringUtils.isBlank(query)) {
                        throw new RuntimeException("table is null");
                    }
                    finalQuery = addRownumClause(query, 10);
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
     * Build SQL to query top 10 records for Oracle
     */
    private static String buildTop10Query(String tablePath, String schemaName) {
        // Clean table name to prevent SQL injection
        String cleanTableName = tablePath.replaceAll("[^a-zA-Z0-9_]", "");
        return "SELECT * FROM " + schemaName + "." + cleanTableName + " WHERE ROWNUM <= 10";
    }

    /**
     * Add ROWNUM clause to query for Oracle
     */
    private static String addRownumClause(String query, int limit) {
        String upperQuery = query.toUpperCase().trim();

        // If query already contains ROWNUM condition, return original query
        if (upperQuery.contains("ROWNUM")) {
            return query;
        }

        // If query ends with semicolon, remove it
        if (query.trim().endsWith(";")) {
            query = query.trim().substring(0, query.trim().length() - 1);
        }

        // For Oracle 12c and above, you can also use FETCH FIRST, but ROWNUM is more compatible
        // Using subquery approach to handle ORDER BY scenarios
        if (upperQuery.contains("ORDER BY")) {
            // If there's ORDER BY, we need to use subquery to ensure correct ordering
            return "SELECT * FROM (" + query + ") WHERE ROWNUM <= " + limit;
        } else {
            // For simple queries without ORDER BY
            return query + " WHERE ROWNUM <= " + limit;
        }
    }

    /**
     * Alternative method using FETCH FIRST (for Oracle 12c and above)
     */
    private static String addFetchFirstClause(String query, int limit) {
        String upperQuery = query.toUpperCase().trim();

        // If query already contains FETCH FIRST or ROWNUM, return original
        if (upperQuery.contains("FETCH FIRST") || upperQuery.contains("ROWNUM")) {
            return query;
        }

        // Remove trailing semicolon
        if (query.trim().endsWith(";")) {
            query = query.trim().substring(0, query.trim().length() - 1);
        }

        return query + " FETCH FIRST " + limit + " ROWS ONLY";
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
            log.info("Error occurred while closing database resources: " + e.getMessage());
        }
    }
}
