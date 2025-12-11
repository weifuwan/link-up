package org.apache.cockpit.plugin.datasource.clickhouse.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.clickhouse.param.ClickHouseConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ClickHouseConnectionManager {

    private static final String[] SENSITIVE_PARAMS = {
            "allowLoadLocalInfile", "autoDeserialize", "allowLocalInfile", "allowUrlInLocalInfile"
    };

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        ClickHouseConnectionParam clickHouseConnectionParam = (ClickHouseConnectionParam) connectionParam;

        DriverConfig driverConfig = createDriverConfig(clickHouseConnectionParam);
        Properties connectionProperties = createConnectionProperties(clickHouseConnectionParam);

        return IDriverManager.getConnection(
                buildJdbcUrl(connectionParam),
                connectionProperties,
                driverConfig
        );
    }

    public String buildJdbcUrl(ConnectionParam connectionParam) {
        ClickHouseConnectionParam clickHouseConnectionParam = (ClickHouseConnectionParam) connectionParam;
        Map<String, String> otherParams = clickHouseConnectionParam.getOtherAsMap();

        String baseUrl = clickHouseConnectionParam.getJdbcUrl();
        if (MapUtils.isNotEmpty(otherParams)) {
            return String.format("%s?%s", baseUrl, buildQueryString(otherParams));
        }
        return baseUrl;
    }

    private DriverConfig createDriverConfig(ClickHouseConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_CLICKHOUSE_JDBC_DRIVER);
        return driverConfig;
    }

    private Properties createConnectionProperties(ClickHouseConnectionParam connectionParam) {
        Properties properties = new Properties();

        // ClickHouse认证信息
        if (connectionParam.getUsername() != null) {
            properties.put("user", sanitizeUsername(connectionParam.getUsername()));
        } else {
            properties.put("user", "default"); // ClickHouse默认用户
        }

        if (connectionParam.getPassword() != null) {
            properties.put("password", sanitizePassword(connectionParam.getPassword()));
        }

        // 添加其他参数
        addOtherProperties(properties, connectionParam.getOtherAsMap());

        // 设置ClickHouse特定的性能参数
        setClickHouseProperties(properties);

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

    private void setClickHouseProperties(Properties properties) {
        // ClickHouse性能优化参数
        properties.put("connect_timeout", "30000");
        properties.put("socket_timeout", "300000");
        properties.put("keep_alive_timeout", "30");
        properties.put("tcp_keep_alive", "true");
        properties.put("use_server_time_zone", "false");
        properties.put("use_time_zone", "UTC");

        // 安全参数
        properties.put("autoDeserialize", "false");
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
