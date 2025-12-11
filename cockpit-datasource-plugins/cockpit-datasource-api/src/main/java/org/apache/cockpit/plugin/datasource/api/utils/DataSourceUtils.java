
package org.apache.cockpit.plugin.datasource.api.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.plugin.DataSourceProcessorProvider;

import java.sql.Connection;
import java.util.Map;

@Slf4j
public class DataSourceUtils {

    public DataSourceUtils() {
    }

    /**
     * check datasource param
     *
     * @param baseDataSourceParamDTO datasource param
     */
    public static void checkDatasourceParam(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        getDatasourceProcessor(baseDataSourceParamDTO.getType()).checkDatasourceParam(baseDataSourceParamDTO);
    }

    public static ConnectionParam buildConnectionParams(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        return getDatasourceProcessor(baseDataSourceParamDTO.getType()).createConnectionParams(baseDataSourceParamDTO);
    }

    public static ConnectionParam buildConnectionParams(DbType dbType, String connectionJson) {
        return getDatasourceProcessor(dbType).createConnectionParams(connectionJson);
    }

    public static String getJdbcUrl(DbType dbType, ConnectionParam baseConnectionParam) {
        return getDatasourceProcessor(dbType).getJdbcUrl(baseConnectionParam);
    }

    public static Connection getConnection(DbType dbType, ConnectionParam connectionParam) {
        try {
            return getDatasourceProcessor(dbType).getConnection(connectionParam);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDatasourceDriver(DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceDriver();
    }

    public static BaseDataSourceParamDTO buildDatasourceParamDTO(DbType dbType, String connectionParams) {
        return getDatasourceProcessor(dbType).createDatasourceParamDTO(connectionParams);
    }

    public static DataSourceProcessor getDatasourceProcessor(DbType dbType) {
        Map<String, DataSourceProcessor> dataSourceProcessorMap =
                DataSourceProcessorProvider.getDataSourceProcessorMap();
        if (!dataSourceProcessorMap.containsKey(dbType.name())) {
            throw new IllegalArgumentException("illegal datasource type");
        }
        return dataSourceProcessorMap.get(dbType.name());
    }

    /**
     * get datasource UniqueId
     */
    public static String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType) {
        return getDatasourceProcessor(dbType).getDatasourceUniqueId(connectionParam, dbType);
    }

    /**
     * build connection url
     */
    public static BaseDataSourceParamDTO buildDatasourceParam(String param) {
        JSONObject jsonNodes = JSON.parseObject(param);

        return getDatasourceProcessor(DbType.ofName(jsonNodes.getString("type").toUpperCase()))
                .castDatasourceParamDTO(param);
    }

    public static String getJdbcUrl(String param, ConnectionParam connectionParam) {
        JSONObject jsonNodes = JSON.parseObject(param);

        return getDatasourceProcessor(DbType.ofName(jsonNodes.getString("type").toUpperCase())).getJdbcUrl(connectionParam);
    }
}
