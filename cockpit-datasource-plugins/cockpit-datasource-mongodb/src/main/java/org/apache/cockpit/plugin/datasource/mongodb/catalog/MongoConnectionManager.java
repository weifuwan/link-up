package org.apache.cockpit.plugin.datasource.mongodb.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.mongodb.param.MongoConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class MongoConnectionManager {

    private static final String[] SENSITIVE_PARAMS = {
            "sslInvalidHostNameAllowed", "sslInvalidCertificateAllowed"
    };

    public String buildConnectionString(ConnectionParam connectionParam) {
        MongoConnectionParam mongoConnectionParam = (MongoConnectionParam) connectionParam;

        StringBuilder connectionString = new StringBuilder("mongodb://");

        // 添加认证信息
        if (mongoConnectionParam.getUsername() != null && !mongoConnectionParam.getUsername().isEmpty()) {
            connectionString.append(mongoConnectionParam.getUsername());
            if (mongoConnectionParam.getPassword() != null && !mongoConnectionParam.getPassword().isEmpty()) {
                String password = PasswordUtils.decodePassword(mongoConnectionParam.getPassword());
                connectionString.append(":").append(password);
            }
            connectionString.append("@");
        }

        // 添加主机和端口
        connectionString.append(mongoConnectionParam.getAddress());

        // 添加数据库
        if (mongoConnectionParam.getDatabase() != null && !mongoConnectionParam.getDatabase().isEmpty()) {
            connectionString.append("/").append(mongoConnectionParam.getDatabase());
        }

        // 添加其他参数
        Map<String, String> otherParams = mongoConnectionParam.getOtherAsMap();
        if (MapUtils.isNotEmpty(otherParams)) {
            connectionString.append("?").append(buildQueryString(otherParams));
        }

        return connectionString.toString();
    }

    public Properties createConnectionProperties(MongoConnectionParam connectionParam) {
        Properties properties = new Properties();

        // 设置基础属性
        properties.put("connectionString", buildConnectionString(connectionParam));
        properties.put("database", connectionParam.getDatabase());

        // 添加其他参数
        addOtherProperties(properties, connectionParam.getOtherAsMap());

        // 设置安全参数
        setSecurityProperties(properties);

        return properties;
    }

    private void addOtherProperties(Properties properties, Map<String, String> otherParams) {
        if (MapUtils.isNotEmpty(otherParams)) {
            otherParams.forEach((key, value) -> {
                if (isSafePropertyKey(key)) {
                    properties.put(key, value);
                } else {
                    log.info("Key `{}` is not legitimate for security reason", key);
                }
            });
        }
    }

    private void setSecurityProperties(Properties properties) {
        // MongoDB安全设置
        properties.put("sslEnabled", "false");
        properties.put("sslInvalidHostNameAllowed", "false");
        properties.put("sslInvalidCertificateAllowed", "false");
        properties.put("maxConnectionIdleTime", "30000");
    }

    private boolean isSafePropertyKey(String key) {
        for (String sensitiveParam : SENSITIVE_PARAMS) {
            if (key.contains(sensitiveParam)) {
                return false;
            }
        }
        return true;
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder queryBuilder = new StringBuilder();
        params.forEach((key, value) -> {
            if (queryBuilder.length() > 0) {
                queryBuilder.append("&");
            }
            queryBuilder.append(key).append("=").append(value);
        });
        return queryBuilder.toString();
    }
}
