package org.apache.cockpit.plugin.datasource.dm.param;

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
import org.apache.cockpit.plugin.datasource.dm.catalog.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class DmDataSourceProcessor extends AbstractDataSourceProcessor {

    private final DmConnectionManager connectionManager;
    private final DmQueryBuilder queryBuilder;

    public DmDataSourceProcessor() {
        this.connectionManager = new DmConnectionManager();
        this.queryBuilder = new DmQueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, DmDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return DmParamConverter.createDatasourceParamDTO(connectionJson);
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return DmParamConverter.createConnectionParams(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, DmConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_DAMENG_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.DAMENG_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        return connectionManager.getJdbcUrl(connectionParam);
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        return connectionManager.getConnection(connectionParam);
    }

    @Override
    public DbType getDbType() {
        return DbType.DAMENG;
    }

    @Override
    public DataSourceProcessor create() {
        return new DmDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return DmQueryParser.splitAndRemoveComment(sql);
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
        return DmMetadataService.listTables(connectionParam, this);
    }

    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        return DmMetadataService.listColumns(connectionParam, tableName, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return DmMetadataService.getTop10Data(connectionParam, requestBody, this);
    }
}
