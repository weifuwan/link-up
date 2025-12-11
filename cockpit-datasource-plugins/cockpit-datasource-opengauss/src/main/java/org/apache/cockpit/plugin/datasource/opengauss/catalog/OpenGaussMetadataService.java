package org.apache.cockpit.plugin.datasource.opengauss.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.opengauss.param.OpenGaussConnectionParam;
import org.apache.cockpit.plugin.datasource.opengauss.param.OpenGaussDataSourceProcessor;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class OpenGaussMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        OpenGaussConnectionParam openGaussConnectionParam = (OpenGaussConnectionParam) connectionParam;
        if (StringUtils.isBlank(openGaussConnectionParam.getSchema())) {
            throw new RuntimeException("schema is null");
        }
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(getListTableSql(openGaussConnectionParam.getDatabase(),
                     openGaussConnectionParam.getSchema()));
             ResultSet rs = ps.executeQuery()) {

            List<String> result = new ArrayList<>();
            while (rs.next()) {
                String value = getTableName(rs);
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
        OpenGaussConnectionParam openGaussConnectionParam = (OpenGaussConnectionParam) connectionParam;
        String catalog = openGaussConnectionParam.getDatabase();
        String schema = openGaussConnectionParam.getUsername();

        try (Connection connection = processor.getConnection(connectionParam);
             ResultSet rs = connection.getMetaData().getColumns(catalog, schema, tableName, null)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String nativeType = rs.getString("TYPE_NAME");
                DataSourceTableColumn typeDefine = DataSourceTableColumn.builder()
                        .columnName(columnName)
                        .build();

                switch (nativeType.toUpperCase()) {
                    case "INT4":
                    case "INT8":
                    case "NUMERIC":
                    case "DECIMAL":
                    case "FLOAT4":
                    case "FLOAT8":
                    case "SMALLINT":
                    case "INTEGER":
                    case "BIGINT":
                    case "REAL":
                    case "DOUBLE PRECISION":
                        typeDefine.setColumnType("NUMBER");
                        break;
                    case "TIMESTAMP":
                    case "TIMESTAMPTZ":
                    case "DATE":
                    case "TIME":
                    case "TIMETZ":
                        typeDefine.setColumnType("TIMESTAMP");
                        break;
                    case "BOOLEAN":
                    case "BOOL":
                        typeDefine.setColumnType("BOOLEAN");
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

    private static String getTableName(ResultSet rs) throws SQLException {
        String tableName = rs.getString(1);
        if (StringUtils.isNotBlank(tableName)) {
            return  tableName;
        }
        return null;
    }

    private static String getListTableSql(String tableCatalog, String tableSchema) {
        return String.format(
                "SELECT table_name FROM information_schema.tables" +
                        " where table_catalog = '%s' and table_schema= '%s'",
                tableCatalog,
                tableSchema);
    }

    public static QueryResult getTop10Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           OpenGaussDataSourceProcessor processor) {
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
                    OpenGaussConnectionParam pgConnectionParam = (OpenGaussConnectionParam) connectionParam;
                    if (StringUtils.isBlank(pgConnectionParam.getSchema())) {
                        throw new RuntimeException("schema is null");
                    }
                    finalQuery = buildTop10Query(tablePath, pgConnectionParam.getSchema());
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
     * Build SQL to query top 10 records for PostgreSQL
     */
    private static String buildTop10Query(String tablePath, String schemaName) {
        // Clean table name to prevent SQL injection
        String cleanTableName = tablePath.replaceAll("[^a-zA-Z0-9_]", "");

        if (schemaName != null && !schemaName.trim().isEmpty()) {
            // 使用指定的 schema
            String cleanSchemaName = schemaName.replaceAll("[^a-zA-Z0-9_]", "");
            return "SELECT * FROM \"" + cleanSchemaName + "\".\"" + cleanTableName + "\" LIMIT 10";
        } else {
            // 不使用 schema 或使用默认 schema
            return "SELECT * FROM \"" + cleanTableName + "\" LIMIT 10";
        }
    }

    /**
     * Add LIMIT clause to query for PostgreSQL
     */
    private static String addLimitClause(String query, int limit) {
        String upperQuery = query.toUpperCase().trim();

        // If query already contains LIMIT, return original query
        if (upperQuery.contains("LIMIT")) {
            return query;
        }

        // If query ends with semicolon, remove it
        if (query.trim().endsWith(";")) {
            query = query.trim().substring(0, query.trim().length() - 1);
        }

        // PostgreSQL 使用标准的 LIMIT 语法
        return query + " LIMIT " + limit;
    }

    /**
     * Alternative method using FETCH FIRST (ANSI SQL standard, also supported by PostgreSQL)
     */
    private static String addFetchFirstClause(String query, int limit) {
        String upperQuery = query.toUpperCase().trim();

        // If query already contains FETCH FIRST or LIMIT, return original
        if (upperQuery.contains("FETCH FIRST") || upperQuery.contains("LIMIT")) {
            return query;
        }

        // Remove trailing semicolon
        if (query.trim().endsWith(";")) {
            query = query.trim().substring(0, query.trim().length() - 1);
        }

        return query + " FETCH FIRST " + limit + " ROWS ONLY";
    }

    /**
     * Process result set and build QueryResult for PostgreSQL
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