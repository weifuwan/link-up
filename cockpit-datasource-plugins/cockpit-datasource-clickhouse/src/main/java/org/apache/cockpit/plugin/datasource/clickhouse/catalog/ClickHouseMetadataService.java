package org.apache.cockpit.plugin.datasource.clickhouse.catalog;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.clickhouse.param.ClickHouseConnectionParam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ClickHouseMetadataService {

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
            log.error("获取表列表失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam,
                                                          String tableName,
                                                          DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        ClickHouseConnectionParam clickHouseConnectionParam = (ClickHouseConnectionParam) connectionParam;
        String database = clickHouseConnectionParam.getDatabase();

        String sql = "SELECT name, type, default_expression, comment " +
                "FROM system.columns " +
                "WHERE database = ? AND table = ? " +
                "ORDER BY position";

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(sql)) {

            ps.setString(1, database);
            ps.setString(2, tableName);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    DataSourceTableColumn column = createColumnFromResultSet(rs);
                    columns.add(column);
                }
            }

        } catch (Exception e) {
            log.error("获取表 {} 的字段列表失败: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("获取字段失败: " + e.getMessage(), e);
        }
        return columns;
    }

    private static DataSourceTableColumn createColumnFromResultSet(ResultSet rs) throws SQLException {
        String columnName = rs.getString("name");
        String nativeType = rs.getString("type");
        String defaultValue = rs.getString("default_expression");
        String comment = rs.getString("comment");

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

        // 数值类型
        if (upperType.contains("INT") || upperType.contains("UINT") ||
                upperType.contains("FLOAT") || upperType.contains("DOUBLE") ||
                upperType.contains("DECIMAL")) {
            return "NUMBER";
        }

        // 布尔类型
        if (upperType.contains("BOOL")) {
            return "BOOLEAN";
        }

        // 日期时间类型
        if (upperType.contains("DATE") || upperType.contains("TIME") ||
                upperType.contains("DATETIME")) {
            return "TIMESTAMP";
        }

        // 数组类型
        if (upperType.startsWith("ARRAY")) {
            return "ARRAY";
        }

        // Map类型
        if (upperType.startsWith("MAP")) {
            return "MAP";
        }

        // Tuple类型
        if (upperType.startsWith("TUPLE")) {
            return "TUPLE";
        }

        // 嵌套类型
        if (upperType.startsWith("NESTED")) {
            return "NESTED";
        }

        // 默认字符串类型
        return "STRING";
    }
}