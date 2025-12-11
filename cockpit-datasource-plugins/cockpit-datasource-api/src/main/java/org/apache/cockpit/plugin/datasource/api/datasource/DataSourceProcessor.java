
package org.apache.cockpit.plugin.datasource.api.datasource;


import com.alibaba.fastjson.JSONObject;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DataSourceProcessor {

    /**
     * cast JSON to relate DTO
     *
     * @param paramJson paramJson
     * @return {@link BaseDataSourceParamDTO}
     */
    BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson);

    /**
     * check datasource param is valid.
     *
     * @throws IllegalArgumentException if invalid
     */
    void checkDatasourceParam(BaseDataSourceParamDTO datasourceParam);

    /**
     * get Datasource Client UniqueId
     *
     * @return UniqueId
     */
    String getDatasourceUniqueId(ConnectionParam connectionParam, DbType dbType);

    /**
     * create BaseDataSourceParamDTO by connectionJson
     *
     * @param connectionJson connectionJson
     * @return {@link BaseDataSourceParamDTO}
     */
    BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson);

    /**
     * create datasource connection parameter which will be stored at DataSource
     * <p>
     * see {@code org.apache.dolphinscheduler.dao.entity.DataSource.connectionParams}
     */
    ConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam);

    /**
     * deserialize json to datasource connection param
     *
     * @param connectionJson {@code org.apache.dolphinscheduler.dao.entity.DataSource.connectionParams}
     * @return {@link BaseConnectionParam}
     */
    ConnectionParam createConnectionParams(String connectionJson);

    /**
     * get datasource Driver
     */
    String getDatasourceDriver();

    /**
     * get validation Query
     */
    String getValidationQuery();

    /**
     * get jdbcUrl by connection param, the jdbcUrl is different with ConnectionParam.jdbcUrl, this method will inject
     * other to jdbcUrl
     *
     * @param connectionParam connection param
     */
    String getJdbcUrl(ConnectionParam connectionParam);

    /**
     * get connection by connectionParam
     *
     * @param connectionParam connectionParam
     * @return {@link Connection}
     */
    // todo: Change to return a ConnectionWrapper
    Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException, IOException;

    /**
     * test connection
     *
     * @param connectionParam connectionParam
     * @return true if connection is valid
     */
    boolean checkDataSourceConnectivity(ConnectionParam connectionParam);

    /**
     * @return {@link DbType}
     */
    DbType getDbType();

    /**
     * get datasource processor
     */
    DataSourceProcessor create();

    List<String> splitAndRemoveComment(String sql);

    JSONObject buildSourceJson(String connectionParam, String sourceParam);

    List<String> listTables(ConnectionParam connectionParam);

    List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName);

    JSONObject buildSinkJson(String connectionParams, String globalParam);

    QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody);


}
