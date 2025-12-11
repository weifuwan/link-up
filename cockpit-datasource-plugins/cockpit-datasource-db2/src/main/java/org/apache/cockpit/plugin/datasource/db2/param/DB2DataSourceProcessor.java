
package org.apache.cockpit.plugin.datasource.db2.param;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.entity.integration.QueryResult;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.db2.catalog.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class DB2DataSourceProcessor extends AbstractDataSourceProcessor {

    private final DB2ConnectionManager connectionManager;
    private final DB2QueryBuilder queryBuilder;

    public DB2DataSourceProcessor() {
        this.connectionManager = new DB2ConnectionManager();
        this.queryBuilder = new DB2QueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, DB2DataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return DB2ParamConverter.createParamDTOFromConnection(connectionJson);
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return DB2ParamConverter.createConnectionParamFromDTO(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, DB2ConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_DB2_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.DB2_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        return connectionManager.buildJdbcUrl(connectionParam);
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        return connectionManager.getConnection(connectionParam);
    }

    @Override
    public DbType getDbType() {
        return DbType.DB2;
    }

    @Override
    public DataSourceProcessor create() {
        return new DB2DataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return DB2QueryParser.splitAndRemoveComment(sql);
    }

    @Override
    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        return queryBuilder.buildSourceJson(connectionParam, sourceParam);
    }

    @Override
    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        return queryBuilder.buildSinkJson(connectionParam, globalParam);
    }

    @Override
    public List<String> listTables(ConnectionParam connectionParam) {
        return DB2MetadataService.listTables(connectionParam, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return null;
    }


    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        return DB2MetadataService.listColumns(connectionParam, tableName, this);
    }
}