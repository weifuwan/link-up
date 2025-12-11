package org.apache.cockpit.plugin.datasource.hive.param;

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
import org.apache.cockpit.plugin.datasource.hive.catalog.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class HiveDataSourceProcessor extends AbstractDataSourceProcessor {

    private final HiveConnectionManager connectionManager;
    private final HiveQueryBuilder queryBuilder;

    public HiveDataSourceProcessor() {
        this.connectionManager = new HiveConnectionManager();
        this.queryBuilder = new HiveQueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        try {
            return JSONUtils.parseObject(paramJson, HiveDataSourceParamDTO.class);
        } catch (Exception e) {
            log.error("Failed to cast datasource param DTO from JSON: {}", paramJson, e);
            throw new RuntimeException("Failed to parse Hive datasource parameters", e);
        }
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return HiveParamConverter.createParamDTOFromConnection(connectionJson);
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return HiveParamConverter.createConnectionParamFromDTO(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        try {
            ConnectionParam param = JSONUtils.parseObject(connectionJson, HiveConnectionParam.class);
            assert param != null;
            param.setDbType(DbType.HIVE);
            return param;
        } catch (Exception e) {
            log.error("Failed to create connection params from JSON: {}", connectionJson, e);
            throw new RuntimeException("Failed to parse Hive connection parameters", e);
        }
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.ORG_APACHE_HIVE_JDBC_HIVE_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.HIVE_VALIDATION_QUERY;
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
        return DbType.HIVE3;
    }

    @Override
    public DataSourceProcessor create() {
        return new HiveDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String sql) {
        return HiveQueryParser.splitAndRemoveComment(sql);
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
        return HiveMetadataService.listTables(connectionParam, this);
    }

    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        return HiveMetadataService.listColumns(connectionParam, tableName, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return HiveMetadataService.getTop20Data(connectionParam, requestBody, this);
    }

}