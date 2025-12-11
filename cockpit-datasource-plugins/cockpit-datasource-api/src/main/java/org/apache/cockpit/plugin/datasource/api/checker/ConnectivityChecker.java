package org.apache.cockpit.plugin.datasource.api.checker;

import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;

public interface ConnectivityChecker {
    boolean supports(DbType dbType);

    boolean checkConnectivity(ConnectionParam connectionParam, DataSourceProcessor dataSourceProcessor);
}
