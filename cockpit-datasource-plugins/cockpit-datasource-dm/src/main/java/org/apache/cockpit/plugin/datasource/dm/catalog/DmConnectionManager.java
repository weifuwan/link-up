package org.apache.cockpit.plugin.datasource.dm.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.dm.param.DmConnectionParam;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DmConnectionManager {

    public String getJdbcUrl(ConnectionParam connectionParam) {
        DmConnectionParam dmConnectionParam = (DmConnectionParam) connectionParam;
        Map<String, String> otherParams = dmConnectionParam.getOtherAsMap();
        if (MapUtils.isNotEmpty(otherParams)) {
            return String.format("%s?%s", dmConnectionParam.getJdbcUrl(),
                    transformOther(otherParams));
        }
        return dmConnectionParam.getJdbcUrl();
    }

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        DmConnectionParam dmConnectionParam = (DmConnectionParam) connectionParam;
        String user = dmConnectionParam.getUsername();
        String password = PasswordUtils.decodePassword(dmConnectionParam.getPassword());

        DriverConfig driverConfig = createDriverConfig(dmConnectionParam);
        return IDriverManager.getConnection(getJdbcUrl(connectionParam), user, password, driverConfig);
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isEmpty(otherMap)) {
            return null;
        }
        List<String> otherList = new ArrayList<>();
        otherMap.forEach((key, value) -> otherList.add(String.format("%s=%s", key, value)));
        return String.join("&", otherList);
    }



    private DriverConfig createDriverConfig(DmConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.COM_DAMENG_JDBC_DRIVER);
        return driverConfig;
    }
}
