package org.apache.cockpit.plugin.datasource.api.checker;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;

import java.sql.Connection;


@Slf4j
public class JdbcConnectivityChecker implements ConnectivityChecker {
    @Override
    public boolean supports(DbType dbType) {
        return dbType == DbType.MONGODB || dbType == DbType.ELASTICSEARCH;
    }

    @Override
    public boolean checkConnectivity(ConnectionParam connectionParam, DataSourceProcessor dataSourceProcessor) {
        try (Connection connection = dataSourceProcessor.getConnection(connectionParam)) {
            return true;
        } catch (Exception e) {
            log.error("Check JDBC connectivity error", e);
            return false;
        }
    }
}

