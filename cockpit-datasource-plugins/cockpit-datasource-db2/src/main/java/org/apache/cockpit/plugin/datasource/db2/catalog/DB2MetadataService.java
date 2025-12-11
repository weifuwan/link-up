package org.apache.cockpit.plugin.datasource.db2.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.db2.param.DB2ConnectionParam;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DB2MetadataService {

    public static final String SHOW_TABLE_SQL = "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TYPE = 'T'";

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        DB2ConnectionParam db2ConnectionParam = (DB2ConnectionParam) connectionParam;
        String schema = db2ConnectionParam.getUsername().toUpperCase(); // DB2 通常使用用户名作为schema

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(SHOW_TABLE_SQL)) {

            ps.setString(1, schema);

            try (ResultSet rs = ps.executeQuery()) {
                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    String tableName = rs.getString("TABNAME");
                    if (tableName != null) {
                        tables.add(tableName);
                    }
                }
                return tables;
            }

        } catch (Exception e) {
            log.error("获取表列表失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据库操作失败: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam,
                                                          String tableName,
                                                          DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        DB2ConnectionParam db2ConnectionParam = (DB2ConnectionParam) connectionParam;
        String schema = db2ConnectionParam.getUsername().toUpperCase();

        try (Connection connection = processor.getConnection(connectionParam);
             ResultSet rs = connection.getMetaData().getColumns(null, schema, tableName, null)) {

            while (rs.next()) {
                DataSourceTableColumn column = createColumnFromResultSet(rs);
                columns.add(column);
            }

        } catch (Exception e) {
            log.error("获取表 {} 的字段列表失败: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("获取字段失败: " + e.getMessage(), e);
        }
        return columns;
    }

    private static DataSourceTableColumn createColumnFromResultSet(ResultSet rs) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        String nativeType = rs.getString("TYPE_NAME");

        return DataSourceTableColumn.builder()
                .columnName(columnName)
                .columnType(mapColumnType(nativeType))
                .build();
    }

    private static String mapColumnType(String nativeType) {
        if (nativeType == null) {
            return "STRING";
        }

        switch (nativeType.toUpperCase()) {
            case "INTEGER":
            case "BIGINT":
            case "DECIMAL":
            case "NUMERIC":
            case "SMALLINT":
            case "REAL":
            case "DOUBLE":
            case "FLOAT":
                return "NUMBER";
            case "TIMESTAMP":
            case "DATE":
            case "TIME":
                return "TIMESTAMP";
            case "CHAR":
            case "VARCHAR":
            case "LONG VARCHAR":
            case "CLOB":
                return "STRING";
            case "BLOB":
            case "BINARY":
            case "VARBINARY":
                return "BINARY";
            default:
                return "STRING";
        }
    }
}