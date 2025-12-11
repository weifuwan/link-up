package org.apache.cockpit.plugin.datasource.doris.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.doris.param.DorisConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DorisConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        DorisConnectionParam dorisConnectionParam = (DorisConnectionParam) connectionParam;

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(dorisConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_MYSQL_CJ_JDBC_DRIVER);

        String username = dorisConnectionParam.getUsername();
        String password = PasswordUtils.decodePassword(dorisConnectionParam.getPassword());

        return IDriverManager.getConnection(getJdbcUrl(connectionParam), username, password, driverConfig);
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        DorisConnectionParam mysqlConnectionParam = (DorisConnectionParam) connectionParam;
        String jdbcUrl = mysqlConnectionParam.getJdbcUrl();
        if (MapUtils.isNotEmpty(mysqlConnectionParam.getOtherAsMap())) {
            return String.format("%s?%s", jdbcUrl, transformOther(mysqlConnectionParam.getOtherAsMap()));
        }
        return String.format("%s", jdbcUrl);
    }

    private String transformOther(Map<String, String> paramMap) {
        if (MapUtils.isEmpty(paramMap)) {
            return null;
        }
        Map<String, String> otherMap = new HashMap<>();
        paramMap.forEach((k, v) -> otherMap.put(k, v));
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        List<String> otherList = new ArrayList<>();
        otherMap.forEach((key, value) -> otherList.add(String.format("%s=%s", key, value)));
        return String.join("&", otherList);
    }
}