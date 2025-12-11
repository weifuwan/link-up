package org.apache.cockpit.plugin.datasource.opengauss.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DriverType;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.opengauss.param.OpenGaussConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class OpenGaussConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        OpenGaussConnectionParam openGaussConnectionParam = (OpenGaussConnectionParam) connectionParam;

        // 根据驱动类型设置对应的驱动类名
        if (openGaussConnectionParam.getDriverType() == DriverType.OPEN_GAUSS) {
            openGaussConnectionParam.setDriverClassName(DataSourceConstants.ORG_OPEN_GAUSS_DRIVER);
        } else {
            openGaussConnectionParam.setDriverClassName(DataSourceConstants.ORG_POSTGRESQL_DRIVER);
        }

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(openGaussConnectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(openGaussConnectionParam.getDriverClassName());

        return IDriverManager.getConnection(
                getJdbcUrl(connectionParam),
                openGaussConnectionParam.getUsername(),
                PasswordUtils.decodePassword(openGaussConnectionParam.getPassword()),
                driverConfig
        );
    }

    public String getJdbcUrl(ConnectionParam connectionParam) {
        OpenGaussConnectionParam openGaussConnectionParam = (OpenGaussConnectionParam) connectionParam;
        if (MapUtils.isNotEmpty(openGaussConnectionParam.getOtherAsMap())) {
            return String.format("%s?%s", openGaussConnectionParam.getJdbcUrl(),
                    transformOther(openGaussConnectionParam.getOtherAsMap()));
        }
        return openGaussConnectionParam.getJdbcUrl();
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