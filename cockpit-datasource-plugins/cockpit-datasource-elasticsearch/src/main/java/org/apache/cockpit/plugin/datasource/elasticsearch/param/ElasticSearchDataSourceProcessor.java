package org.apache.cockpit.plugin.datasource.elasticsearch.param;

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
import org.apache.cockpit.plugin.datasource.elasticsearch.catalog.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class ElasticSearchDataSourceProcessor extends AbstractDataSourceProcessor {

    private final ElasticSearchConnectionManager connectionManager;
    private final ElasticSearchQueryBuilder queryBuilder;

    public ElasticSearchDataSourceProcessor() {
        this.connectionManager = new ElasticSearchConnectionManager();
        this.queryBuilder = new ElasticSearchQueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, ElasticSearchDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return ElasticSearchParamConverter.createParamDTOFromConnection(connectionJson);
    }

    @Override
    public boolean checkDataSourceConnectivity(ConnectionParam connectionParam) {
        ElasticsearchConnectionFactory factory = new ElasticsearchConnectionFactory();
        ElasticSearchConnectionParam elasticSearchConnectionParam = (ElasticSearchConnectionParam) connectionParam;
        return factory.checkDataSourceConnectivity(elasticSearchConnectionParam);
    }


    public void checkDatasourceParam(BaseDataSourceParamDTO baseDataSourceParamDTO) {
        if (!baseDataSourceParamDTO.getType().equals(DbType.REDSHIFT)) {
            // due to redshift use not regular hosts
            checkHost(baseDataSourceParamDTO.getHost());
        }
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return ElasticSearchParamConverter.createConnectionParamFromDTO(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, ElasticSearchConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.ELASTICSEARCH_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.ELASTICSEARCH_VALIDATION_QUERY;
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
        return DbType.ELASTICSEARCH;
    }

    @Override
    public DataSourceProcessor create() {
        return new ElasticSearchDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return ElasticSearchQueryParser.splitAndRemoveComment(sql);
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
        return ElasticSearchMetadataService.listTables(connectionParam, this);
    }

    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        return ElasticSearchMetadataService.listColumns(connectionParam, tableName, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return ElasticSearchMetadataService.getTop20Data(connectionParam, requestBody, this);
    }
}
