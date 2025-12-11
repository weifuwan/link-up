package org.apache.cockpit.plugin.datasource.db2.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.db2.param.DB2ConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class DB2ConnectionManager {

    private static final String[] SENSITIVE_PARAMS = {
            "deferPrepares", "currentSchema", "currentFunctionPath"
    };

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        DB2ConnectionParam db2ConnectionParam = (DB2ConnectionParam) connectionParam;

        DriverConfig driverConfig = createDriverConfig(db2ConnectionParam);
        Properties connectionProperties = createConnectionProperties(db2ConnectionParam);

        return IDriverManager.getConnection(
                buildJdbcUrl(connectionParam),
                connectionProperties,
                driverConfig
        );
    }

    public String buildJdbcUrl(ConnectionParam connectionParam) {
        DB2ConnectionParam db2ConnectionParam = (DB2ConnectionParam) connectionParam;
        Map<String, String> otherParams = db2ConnectionParam.getOtherAsMap();

        if (MapUtils.isNotEmpty(otherParams)) {
            return String.format("%s:%s", db2ConnectionParam.getJdbcUrl(), buildQueryString(otherParams));
        }
        return db2ConnectionParam.getJdbcUrl();
    }

    private DriverConfig createDriverConfig(DB2ConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_DB2_JDBC_DRIVER);
        return driverConfig;
    }

    private Properties createConnectionProperties(DB2ConnectionParam connectionParam) {
        Properties properties = new Properties();
        properties.put("user", sanitizeUsername(connectionParam.getUsername()));
        properties.put("password", sanitizePassword(connectionParam.getPassword()));

        // 添加其他参数
        addOtherProperties(properties, connectionParam.getOtherAsMap());

        // 设置DB2特定参数
        setDB2Properties(properties);

        return properties;
    }

    private String sanitizeUsername(String username) {
        return sanitizeSensitiveParam(username, "currentSchema");
    }

    private String sanitizePassword(String encodedPassword) {
        String password = PasswordUtils.decodePassword(encodedPassword);
        return sanitizeSensitiveParam(password, "currentSchema");
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

    private void setDB2Properties(Properties properties) {
        properties.put("retrieveMessagesFromServerOnGetMessage", "true");
        properties.put("deferPrepares", "false");
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