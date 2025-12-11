package org.apache.cockpit.plugin.datasource.hive.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.hive.param.HiveConnectionParam;
import org.apache.cockpit.plugin.datasource.hive.param.HiveDataSourceProcessor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HiveMetadataService {

    public static final String SHOW_TABLE_SQL = "SHOW TABLES";

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(SHOW_TABLE_SQL);
             ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                String tableName = rs.getString(1);
                if (tableName != null) {
                    tables.add(tableName);
                }
            }
            return tables;

        } catch (Exception e) {
            log.error("Failed to get hive table list: {}", e.getMessage(), e);
            throw new RuntimeException("Hive database operation failed: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam,
                                                          String tableName,
                                                          DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        HiveConnectionParam hiveConnectionParam = (HiveConnectionParam) connectionParam;
        String catalog = hiveConnectionParam.getDatabase();

        try (Connection connection = processor.getConnection(connectionParam);
             ResultSet rs = connection.getMetaData().getColumns(catalog, null, tableName, null)) {

            while (rs.next()) {
                DataSourceTableColumn column = createColumnFromResultSet(rs);
                columns.add(column);
            }

        } catch (Exception e) {
            log.error("Failed to get hive column list for table {}: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to get hive columns: " + e.getMessage(), e);
        }
        return columns;
    }

    private static DataSourceTableColumn createColumnFromResultSet(ResultSet rs) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        String nativeType = rs.getString("TYPE_NAME");

        return DataSourceTableColumn.builder()
                .columnName(columnName)
                .columnType(mapColumnType(nativeType))
                .sourceType(nativeType)
                .build();
    }

    private static String mapColumnType(String nativeType) {
        if (nativeType == null) {
            return "STRING";
        }

        String upperType = nativeType.toUpperCase();
        switch (upperType) {
            case "INT":
            case "BIGINT":
            case "DECIMAL":
            case "DOUBLE":
            case "FLOAT":
            case "SMALLINT":
            case "TINYINT":
                return "NUMBER";
            case "TIMESTAMP":
            case "DATE":
                return "TIMESTAMP";
            case "BOOLEAN":
                return "BOOLEAN";
            default:
                return "STRING";
        }
    }

    public static QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           HiveDataSourceProcessor processor) {
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
            throw new RuntimeException("Hive database query failed", e);
        } catch (Exception e) {
            throw new RuntimeException("Hive data processing failed", e);
        } finally {
            closeResources(connection, preparedStatement, resultSet);
        }
    }

    private static String buildTop20Query(String tablePath) {
        // Clean table name to prevent SQL injection
        String cleanTableName = tablePath.replaceAll("[^a-zA-Z0-9_.]", "");
        return "SELECT * FROM " + cleanTableName + " LIMIT 10";
    }

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
            log.error("Error occurred while closing hive database resources: " + e.getMessage());
        }
    }
}
