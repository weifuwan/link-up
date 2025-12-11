package org.apache.cockpit.plugin.datasource.sqlserver.catalog;

import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.sqlserver.param.SQLServerConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class SQLServerConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        SQLServerConnectionParam sqlServerConnectionParam = (SQLServerConnectionParam) connectionParam;

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(sqlServerConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_SQLSERVER_JDBC_DRIVER);

        return IDriverManager.getConnection(
                getJdbcUrl(connectionParam),
                sqlServerConnectionParam.getUsername(),
                PasswordUtils.decodePassword(sqlServerConnectionParam.getPassword()),
                driverConfig
        );
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        SQLServerConnectionParam sqlServerConnectionParam = (SQLServerConnectionParam) connectionParam;

        if (MapUtils.isNotEmpty(sqlServerConnectionParam.getOtherAsMap())) {
            return String.format("%s;%s", sqlServerConnectionParam.getJdbcUrl(),
                    transformOther(sqlServerConnectionParam.getOtherAsMap()));
        }
        return sqlServerConnectionParam.getJdbcUrl();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        otherMap.forEach((key, value) -> stringBuilder.append(String.format("%s=%s;", key, value)));
        return stringBuilder.toString();
    }
}