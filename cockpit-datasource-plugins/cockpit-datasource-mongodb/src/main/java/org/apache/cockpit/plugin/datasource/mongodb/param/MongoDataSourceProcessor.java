package org.apache.cockpit.plugin.datasource.mongodb.param;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.mongodb.catalog.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@AutoService(DataSourceProcessor.class)
@Slf4j
public class MongoDataSourceProcessor extends AbstractDataSourceProcessor {

    private final MongoConnectionManager connectionManager;
    private final MongoQueryBuilder queryBuilder;

    public MongoDataSourceProcessor() {
        this.connectionManager = new MongoConnectionManager();
        this.queryBuilder = new MongoQueryBuilder();
    }

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, MongoDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        return MongoParamConverter.createParamDTOFromConnection(connectionJson);
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        return MongoParamConverter.createConnectionParamFromDTO(dataSourceParam);
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, MongoConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return "com.mongodb.client.MongoClient";
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.MONGODB_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        MongoConnectionParam mongoParam = (MongoConnectionParam) connectionParam;
        return mongoParam.getConnectionString();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        throw new UnsupportedOperationException("MongoDB不使用JDBC连接");
    }

    @Override
    public boolean checkDataSourceConnectivity(ConnectionParam connectionParam) {
        MongoConnectionParam param = (MongoConnectionParam) connectionParam;
        MongoClient mongoClient = null;
        try {
            MongoClientSettings settings = buildMongoClientSettings(param);
            mongoClient = MongoClients.create(settings);

            // 执行简单操作验证连接
            mongoClient.listDatabaseNames().first();
            return true;
        } catch (Exception e) {
            log.error("Check MongoDB connectivity error", e);
            return false;
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private MongoClientSettings buildMongoClientSettings(MongoConnectionParam param) {
        // 直接从参数中获取认证信息，避免URL解析问题
        MongoCredential credential = MongoCredential.createCredential(
                param.getUsername(),
                param.getDatabase(),
                PasswordUtils.decodePassword(param.getPassword()).toCharArray()
        );

        ServerAddress serverAddress = new ServerAddress(param.getHost(), param.getPort());

        return MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Collections.singletonList(serverAddress)))
                .credential(credential)
                .applyToSocketSettings(builder ->
                        builder.connectTimeout(2000, TimeUnit.MILLISECONDS)
                                .readTimeout(2000, TimeUnit.MILLISECONDS))
                .build();
    }


    @Override
    public DbType getDbType() {
        return DbType.MONGODB;
    }

    @Override
    public DataSourceProcessor create() {
        return new MongoDataSourceProcessor();
    }

    @Override
    public List<String> splitAndRemoveComment(String query) {
        return MongoQueryParser.splitAndRemoveComment(query);
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
        // MongoDB中表对应集合
        return MongoMetadataService.listCollections(connectionParam, this);
    }

    @Override
    public List<DataSourceTableColumn> listColumns(ConnectionParam connectionParam, String tableName) {
        // MongoDB中字段对应文档字段
        return MongoMetadataService.listFields(connectionParam, tableName, this);
    }

    @Override
    public QueryResult getTop20Data(ConnectionParam connectionParam, Map<String, Object> requestBody) {
        return null;
    }
}