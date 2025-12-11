package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;
import org.apache.cockpit.plugin.datasource.api.constants.DataSourceConstants;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchConnectionParam;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ElasticSearchConnectionManager {

    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        ElasticSearchConnectionParam esConnectionParam = (ElasticSearchConnectionParam) connectionParam;

        DriverConfig driverConfig = createDriverConfig(esConnectionParam);
        Properties connectionProperties = createConnectionProperties(esConnectionParam);

        return IDriverManager.getConnection(
                buildJdbcUrl(connectionParam),
                connectionProperties,
                driverConfig
        );
    }

    public String buildJdbcUrl(ConnectionParam connectionParam) {
        ElasticSearchConnectionParam esConnectionParam = (ElasticSearchConnectionParam) connectionParam;
        Map<String, String> otherParams = esConnectionParam.getOtherAsMap();

        if (MapUtils.isNotEmpty(otherParams)) {
            return String.format("%s?%s", esConnectionParam.getJdbcUrl(), buildQueryString(otherParams));
        }
        return esConnectionParam.getJdbcUrl();
    }

    private DriverConfig createDriverConfig(ElasticSearchConnectionParam connectionParam) {
        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriver(connectionParam.getDriverLocation());
        driverConfig.setJdbcDriverClass(DataSourceConstants.ELASTICSEARCH_DRIVER);
        return driverConfig;
    }

    private Properties createConnectionProperties(ElasticSearchConnectionParam connectionParam) {
        Properties properties = new Properties();
        if (StringUtils.isNotBlank(connectionParam.getUsername())) {
            properties.put("user", connectionParam.getUsername());
        }

        if (StringUtils.isNotBlank(connectionParam.getPassword())) {
            properties.put("password", PasswordUtils.decodePassword(connectionParam.getPassword()));
        }

        return properties;
    }

    private String buildQueryString(Map<String, String> params) {
        List<String> queryParams = new ArrayList<>();
        params.forEach((key, value) -> queryParams.add(String.format("%s=%s", key, value)));
        return String.join("&", queryParams);
    }
}
