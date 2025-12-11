package org.apache.cockpit.plugin.datasource.oracle.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class OracleConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        OracleConnectionParam oracleConnectionParam = (OracleConnectionParam) connectionParam;

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(oracleConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_ORACLE_JDBC_DRIVER);

        return IDriverManager.getConnection(
                getJdbcUrl(connectionParam),
                oracleConnectionParam.getUsername(),
                PasswordUtils.decodePassword(oracleConnectionParam.getPassword()),
                driverConfig
        );
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        OracleConnectionParam oracleConnectionParam = (OracleConnectionParam) connectionParam;
        if (MapUtils.isNotEmpty(oracleConnectionParam.getOtherAsMap())) {
            return String.format("%s?%s", oracleConnectionParam.getJdbcUrl(),
                    transformOther(oracleConnectionParam.getOtherAsMap()));
        }
        return oracleConnectionParam.getJdbcUrl();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        List<String> otherList = new ArrayList<>();
        otherMap.forEach((key, value) -> otherList.add(String.format("%s=%s", key, value)));
        return String.join("&", otherList);
    }
}
