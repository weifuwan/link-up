package org.apache.cockpit.plugin.datasource.postgresql.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.postgresql.param.PostgreSQLConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PostgreSQLConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        PostgreSQLConnectionParam postgreSqlConnectionParam = (PostgreSQLConnectionParam) connectionParam;

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(postgreSqlConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.ORG_POSTGRESQL_DRIVER);

        return IDriverManager.getConnection(
                getJdbcUrl(connectionParam),
                postgreSqlConnectionParam.getUsername(),
                PasswordUtils.decodePassword(postgreSqlConnectionParam.getPassword()),
                driverConfig
        );
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        PostgreSQLConnectionParam postgreSqlConnectionParam = (PostgreSQLConnectionParam) connectionParam;
        if (MapUtils.isNotEmpty(postgreSqlConnectionParam.getOtherAsMap())) {
            return String.format("%s?%s", postgreSqlConnectionParam.getJdbcUrl(),
                    transformOther(postgreSqlConnectionParam.getOtherAsMap()));
        }
        return postgreSqlConnectionParam.getJdbcUrl();
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
