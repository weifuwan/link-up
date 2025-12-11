package org.apache.cockpit.plugin.datasource.doris.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.doris.param.DorisConnectionParam;
import org.apache.cockpit.plugin.datasource.doris.param.DorisDataSourceProcessor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DorisMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(getListTableSql());
             ResultSet rs = ps.executeQuery()) {

            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String value = rs.getString(1);
                if (value != null) {
                    result.add(value);
                }
            }
            return result;

        } catch (Exception e) {
            log.error("获取表列表失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName, DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        DorisConnectionParam dorisConnectionParam = (DorisConnectionParam) connectionParam;
        String catalog = dorisConnectionParam.getDatabase();

        try (Connection connection = processor.getConnection(connectionParam);
             ResultSet rs = connection.getMetaData().getColumns(catalog, null, tableName, null)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String nativeType = rs.getString("TYPE_NAME");
                DataSourceTableColumn typeDefine = DataSourceTableColumn.builder()
                        .columnName(columnName)
                        .sourceType(nativeType)
                        .build();

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
                    case "TIMESTAMP":
                    case "DATE":
                    case "DATETIME":
                        typeDefine.setColumnType("TIMESTAMP");
                        break;
                    default:
                        typeDefine.setColumnType("STRING");
                        break;
                }
                columns.add(typeDefine);
            }
        } catch (Exception e) {
            log.error("获取表 {} 的字段列表失败: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("获取字段失败: " + e.getMessage(), e);
        }
        return columns;
    }

    private static String getListTableSql() {
        return "SHOW TABLES;";
    }

    public static QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           DorisDataSourceProcessor processor) {
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
                    finalQuery = buildTop20Query(tablePath);
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
    private static String buildTop20Query(String tablePath) {
        // Clean table name to prevent SQL injection
        String cleanTableName = tablePath.replaceAll("[^a-zA-Z0-9_]", "");
        return "SELECT * FROM " + cleanTableName + " LIMIT 10";
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
