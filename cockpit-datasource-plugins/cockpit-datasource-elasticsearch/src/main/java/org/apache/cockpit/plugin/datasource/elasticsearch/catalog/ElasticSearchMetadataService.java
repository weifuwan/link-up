package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.FrontedTableColumn;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchDataSourceProcessor;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticSearchMetadataService {

    public static final String SHOW_INDICES_SQL = "SHOW TABLES";
    public static final String DESCRIBE_INDEX_SQL = "DESCRIBE ?";

    public static List<String> listTables(ConnectionParam connectionParam,
                                          DataSourceProcessor processor) {
        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(SHOW_INDICES_SQL);
             ResultSet rs = ps.executeQuery()) {

            List<String> indices = new ArrayList<>();
            while (rs.next()) {
                String indexName = rs.getString("name");
                if (indexName != null && !indexName.startsWith(".")) { // Skip system indices
                    indices.add(indexName);
                }
            }
            return indices;

        } catch (Exception e) {
            log.error("Failed to get index list: {}", e.getMessage(), e);
            throw new RuntimeException("Database operation failed: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam,
                                                          String indexName,
                                                          DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(DESCRIBE_INDEX_SQL)) {

            ps.setString(1, indexName);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                DataSourceTableColumn column = createColumnFromResultSet(rs);
                columns.add(column);
            }

        } catch (Exception e) {
            log.error("Failed to get column list for index {}: {}", indexName, e.getMessage(), e);
            throw new RuntimeException("Failed to get columns: " + e.getMessage(), e);
        }
        return columns;
    }

    private static DataSourceTableColumn createColumnFromResultSet(ResultSet rs) throws SQLException {
        String columnName = rs.getString("column");
        String nativeType = rs.getString("type");

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
            case "LONG":
            case "INTEGER":
            case "SHORT":
            case "BYTE":
            case "DOUBLE":
            case "FLOAT":
            case "HALF_FLOAT":
            case "SCALED_FLOAT":
                return "NUMBER";
            case "DATE":
            case "DATE_NANOS":
                return "TIMESTAMP";
            case "BOOLEAN":
                return "BOOLEAN";
            default:
                return "STRING";
        }
    }

    public static QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody,
                                           ElasticSearchDataSourceProcessor processor) {
        String indexName = requestBody.get("index").toString();
        String query = "SELECT * FROM \"" + indexName + "\" LIMIT 20";

        try (Connection connection = processor.getConnection(connectionParam);
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {

            return processResultSet(rs);

        } catch (SQLException e) {
            throw new RuntimeException("Database query failed", e);
        } catch (Exception e) {
            throw new RuntimeException("Data processing failed", e);
        }
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
                Object value = resultSet.getObject(i);
                row.put(columnName, value);
            }
            data.add(row);
        }

        return new QueryResult(columns, data);
    }
}