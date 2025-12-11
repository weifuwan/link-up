package org.apache.cockpit.plugin.datasource.cache.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CacheMetadataService {

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(getListTableSql());
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

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName, DataSourceProcessor dataSourceProcessor) {
        // Cache数据库暂不支持字段列表查询
        return new ArrayList<>();
    }

    private static String getListTableSql() {
        return "SHOW TABLES;";
    }

    private static String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }
}
