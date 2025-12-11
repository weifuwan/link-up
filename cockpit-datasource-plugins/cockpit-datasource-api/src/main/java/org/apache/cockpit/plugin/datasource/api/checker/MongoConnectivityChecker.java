package org.apache.cockpit.plugin.datasource.api.checker;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;

@Slf4j
public class MongoConnectivityChecker implements ConnectivityChecker {
    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.MONGODB;
    }

    @Override
    public boolean checkConnectivity(ConnectionParam connectionParam, DataSourceProcessor dataSourceProcessor) {
        return dataSourceProcessor.checkDataSourceConnectivity(connectionParam);
    }

}