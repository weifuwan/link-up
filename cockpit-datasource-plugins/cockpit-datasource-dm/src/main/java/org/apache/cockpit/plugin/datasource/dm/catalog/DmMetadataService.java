package org.apache.cockpit.plugin.datasource.dm.catalog;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.enums.integration.TaskExecutionTypeEnum;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.dm.param.DmConnectionParam;
import org.apache.cockpit.plugin.datasource.dm.param.DmDataSourceProcessor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DmMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam, DataSourceProcessor processor) {
        DmConnectionParam dmConnectionParam = (DmConnectionParam) connectionParam;
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(getListTableSql(dmConnectionParam.getUsername().toUpperCase()));
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
        DmConnectionParam dmConnectionParam = (DmConnectionParam) connectionParam;
        String catalog = dmConnectionParam.getDatabase();
        String schema = dmConnectionParam.getUsername().toUpperCase();

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

    private static String getListTableSql(String owner) {
        // 这里做了修改，修改了原来的 TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL
        return "SELECT TABLE_NAME FROM ALL_TABLES" +
                " WHERE OWNER = '" + owner + "'" +
                " AND TABLE_NAME NOT LIKE 'MDRT_%'" +
                " AND TABLE_NAME NOT LIKE 'MDRS_%'" +
                " AND TABLE_NAME NOT LIKE 'MDXT_%'" +
                " AND TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%'" +
                " AND TABLE_NAME != '##HISTOGRAMS_TABLE'";
    }

    public static QueryResult getTop10Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           DmDataSourceProcessor processor) {

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
                    DmConnectionParam dmConnectionParam = (DmConnectionParam) connectionParam;
                    finalQuery = buildTop10Query(tablePath, dmConnectionParam.getUsername().toUpperCase());
                    preparedStatement = connection.prepareStatement(finalQuery);
                    break;

                case SINGLE_TABLE_CUSTOM:
                    String query = requestBody.get("query").toString();
                    if (StringUtils.isBlank(query)) {
                        throw new RuntimeException("table is null");
                    }
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
     * 构建达梦数据库的TOP 10查询语句
     */
    private static String buildTop10Query(String tablePath, String schema) {
        // 如果表名已包含模式名，则直接使用；否则添加模式名前缀
        String tableName;
        if (tablePath.contains(".")) {
            tableName = tablePath;
        } else {
            tableName = schema + "." + tablePath;
        }

        // 达梦使用LIMIT语法获取前N条数据
        return "SELECT * FROM " + tableName + " LIMIT 10";
    }

    /**
     * 为自定义查询添加LIMIT子句
     */
    private static String addLimitClause(String query, int limit) {
        // 移除原查询中的分号和尾部空格
        String trimmedQuery = query.trim();
        if (trimmedQuery.endsWith(";")) {
            trimmedQuery = trimmedQuery.substring(0, trimmedQuery.length() - 1);
        }

        // 检查是否已有LIMIT子句
        String upperQuery = trimmedQuery.toUpperCase();
        if (upperQuery.contains(" LIMIT ")) {
            // 如果已有LIMIT子句，替换为新的LIMIT
            int limitIndex = upperQuery.lastIndexOf(" LIMIT ");
            String beforeLimit = trimmedQuery.substring(0, limitIndex);
            return beforeLimit + " LIMIT " + limit;
        }

        // 检查是否已有ORDER BY子句
        if (upperQuery.contains(" ORDER BY ")) {
            // 在ORDER BY之后添加LIMIT
            int orderByIndex = upperQuery.lastIndexOf(" ORDER BY ");
            String beforeOrderBy = trimmedQuery.substring(0, orderByIndex);
            String orderByClause = trimmedQuery.substring(orderByIndex);
            return beforeOrderBy + orderByClause + " LIMIT " + limit;
        }

        // 直接在查询末尾添加LIMIT
        return trimmedQuery + " LIMIT " + limit;
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
