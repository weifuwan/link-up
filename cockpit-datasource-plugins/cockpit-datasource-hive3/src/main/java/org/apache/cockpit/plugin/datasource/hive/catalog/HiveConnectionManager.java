package org.apache.cockpit.plugin.datasource.hive.catalog;


import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.hive.param.HiveConnectionParam;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Slf4j
public class HiveConnectionManager {

    public String buildJdbcUrl(ConnectionParam connectionParam) {
        HiveConnectionParam hiveConnectionParam = (HiveConnectionParam) connectionParam;
        return hiveConnectionParam.getJdbcUrl();
    }

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        HiveConnectionParam hiveConnectionParam = (HiveConnectionParam) connectionParam;

        DriverConfig driverConfig = createDriverConfig(hiveConnectionParam);

        // 1. 加载驱动
        Class.forName(driverConfig.getJdbcDriverClass());

        // 2. 建立连接
        return DriverManager.getConnection(driverConfig.getUrl(), hiveConnectionParam.getUsername(), hiveConnectionParam.getPassword());

    }


    private DriverConfig createDriverConfig(HiveConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setUrl(connectionParam.getJdbcUrl());
        driverConfig.setJdbcDriverClass(connectionParam.getDriverClassName());
        return driverConfig;
    }
}
