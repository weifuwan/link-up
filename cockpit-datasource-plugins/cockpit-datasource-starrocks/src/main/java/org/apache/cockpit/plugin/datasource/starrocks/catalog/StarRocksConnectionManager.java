package org.apache.cockpit.plugin.datasource.starrocks.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.starrocks.param.StarRocksConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class StarRocksConnectionManager {

    private static final String[] SENSITIVE_PARAMS = {
            "allowLoadLocalInfile", "autoDeserialize", "allowLocalInfile", "allowUrlInLocalInfile"
    };

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        StarRocksConnectionParam starRocksConnectionParam = (StarRocksConnectionParam) connectionParam;

        DriverConfig driverConfig = createDriverConfig(starRocksConnectionParam);
        Properties connectionProperties = createConnectionProperties(starRocksConnectionParam);

        return IDriverManager.getConnection(
                buildJdbcUrl(connectionParam),
                connectionProperties,
                driverConfig
        );
    }

    public String buildJdbcUrl(ConnectionParam connectionParam) {
        StarRocksConnectionParam starRocksConnectionParam = (StarRocksConnectionParam) connectionParam;
        Map<String, String> otherParams = starRocksConnectionParam.getOtherAsMap();

        if (MapUtils.isNotEmpty(otherParams)) {
            return String.format("%s?%s", starRocksConnectionParam.getJdbcUrl(), buildQueryString(otherParams));
        }
        return starRocksConnectionParam.getJdbcUrl();
    }

    private DriverConfig createDriverConfig(StarRocksConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER); // StarRocks使用MySQL协议
        return driverConfig;
    }

    private Properties createConnectionProperties(StarRocksConnectionParam connectionParam) {
        Properties properties = new Properties();
        properties.put("user", sanitizeUsername(connectionParam.getUsername()));
        properties.put("password", sanitizePassword(connectionParam.getPassword()));

        // 添加其他参数
        addOtherProperties(properties, connectionParam.getOtherAsMap());

        // 设置安全参数
        setSecurityProperties(properties);

        return properties;
    }

    private String sanitizeUsername(String username) {
        return sanitizeSensitiveParam(username, "autoDeserialize");
    }

    private String sanitizePassword(String encodedPassword) {
        String password = PasswordUtils.decodePassword(encodedPassword);
        return sanitizeSensitiveParam(password, "autoDeserialize");
    }

    private String sanitizeSensitiveParam(String value, String sensitiveParam) {
        if (value.contains(sensitiveParam)) {
            log.warn("sensitive param : {} in field is filtered", sensitiveParam);
            return value.replace(sensitiveParam, "");
        }
        return value;
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
        properties.put("autoDeserialize", "false");
        properties.put("allowLoadLocalInfile", "false");
        properties.put("allowLocalInfile", "false");
        properties.put("allowUrlInLocalInfile", "false");
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
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}
