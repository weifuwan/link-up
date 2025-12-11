package org.apache.cockpit.plugin.datasource.cache.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.cache.param.CacheConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class CacheConnectionManager {

    private static final String ALLOW_LOAD_LOCAL_IN_FILE_NAME = "allowLoadLocalInfile";
    private static final String AUTO_DESERIALIZE = "autoDeserialize";
    private static final String ALLOW_LOCAL_IN_FILE_NAME = "allowLocalInfile";
    private static final String ALLOW_URL_IN_LOCAL_IN_FILE_NAME = "allowUrlInLocalInfile";

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        CacheConnectionParam cacheConnectionParam = (CacheConnectionParam) connectionParam;

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(cacheConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_CACHE_JDBC_DRIVER);

        String username = sanitizeUsername(cacheConnectionParam.getUsername());
        String password = sanitizePassword(cacheConnectionParam.getPassword());

        Properties connectionProperties = getConnectionProperties(cacheConnectionParam, username, password);
        return IDriverManager.getConnection(getJdbcUrl(connectionParam), connectionProperties, driverConfig);
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        CacheConnectionParam cacheConnectionParam = (CacheConnectionParam) connectionParam;
        if (MapUtils.isNotEmpty(cacheConnectionParam.getOtherAsMap())) {
            return String.format("%s?%s", cacheConnectionParam.getJdbcUrl(),
                    transformOther(cacheConnectionParam.getOtherAsMap()));
        }
        return cacheConnectionParam.getJdbcUrl();
    }

    private String sanitizeUsername(String username) {
        if (username.contains(AUTO_DESERIALIZE)) {
            log.warn("sensitive param : {} in username field is filtered", AUTO_DESERIALIZE);
            return username.replace(AUTO_DESERIALIZE, "");
        }
        return username;
    }

    private String sanitizePassword(String encodedPassword) {
        String password = PasswordUtils.decodePassword(encodedPassword);
        if (password.contains(AUTO_DESERIALIZE)) {
            log.warn("sensitive param : {} in password field is filtered", AUTO_DESERIALIZE);
            return password.replace(AUTO_DESERIALIZE, "");
        }
        return password;
    }

    private Properties getConnectionProperties(CacheConnectionParam cacheConnectionParam, String username,
                                               String password) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", username);
        connectionProperties.put("password", password);

        Map<String, String> paramMap = cacheConnectionParam.getOtherAsMap();
        if (MapUtils.isNotEmpty(paramMap)) {
            paramMap.forEach((k, v) -> {
                if (!checkKeyIsLegitimate(k)) {
                    log.info("Key `{}` is not legitimate for security reason", k);
                    return;
                }
                connectionProperties.put(k, v);
            });
        }

        // 设置安全参数
        connectionProperties.put(AUTO_DESERIALIZE, "false");
        connectionProperties.put(ALLOW_LOAD_LOCAL_IN_FILE_NAME, "false");
        connectionProperties.put(ALLOW_LOCAL_IN_FILE_NAME, "false");
        connectionProperties.put(ALLOW_URL_IN_LOCAL_IN_FILE_NAME, "false");

        return connectionProperties;
    }

    private boolean checkKeyIsLegitimate(String key) {
        return !key.contains(ALLOW_LOAD_LOCAL_IN_FILE_NAME)
                && !key.contains(AUTO_DESERIALIZE)
                && !key.contains(ALLOW_LOCAL_IN_FILE_NAME)
                && !key.contains(ALLOW_URL_IN_LOCAL_IN_FILE_NAME);
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isNotEmpty(otherMap)) {
            List<String> list = new ArrayList<>(otherMap.size());
            otherMap.forEach((key, value) -> list.add(String.format("%s=%s", key, value)));
            return String.join("&", list);
        }
        return null;
    }
}