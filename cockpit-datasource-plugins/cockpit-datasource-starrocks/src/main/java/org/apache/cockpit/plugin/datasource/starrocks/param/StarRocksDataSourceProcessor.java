package org.apache.cockpit.plugin.datasource.starrocks.param;

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
import org.apache.cockpit.plugin.datasource.starrocks.catalog.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class StarRocksDataSourceProcessor extends AbstractDataSourceProcessor {

    private final StarRocksConnectionManager connectionManager;
    private final StarRocksQueryBuilder queryBuilder;

    public StarRocksDataSourceProcessor() {
        this.connectionManager = new StarRocksConnectionManager();
        this.queryBuilder = new StarRocksQueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, StarRocksDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return StarRocksParamConverter.createParamDTOFromConnection(connectionJson);
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return StarRocksParamConverter.createConnectionParamFromDTO(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, StarRocksConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER; // StarRocks使用MySQL协议
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.MYSQL_VALIDATION_QUERY;
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
        return DbType.STARROCKS;
    }

    @Override
    public DataSourceProcessor create() {
        return new StarRocksDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return StarRocksQueryParser.splitAndRemoveComment(sql);
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
        return StarRocksMetadataService.listTables(connectionParam, this);
    }

    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        return StarRocksMetadataService.listColumns(connectionParam, tableName, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return StarRocksMetadataService.getTop20Data(connectionParam, requestBody, this);
    }
}
