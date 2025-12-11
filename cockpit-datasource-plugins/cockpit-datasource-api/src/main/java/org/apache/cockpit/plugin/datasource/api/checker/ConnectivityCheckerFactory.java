package org.apache.cockpit.plugin.datasource.api.checker;

import org.apache.cockpit.common.spi.enums.DbType;

public class ConnectivityCheckerFactory {
    public static ConnectivityChecker getChecker(DbType dbType) {
        switch (dbType) {
            case MONGODB:
                return new MongoConnectivityChecker();
            case ELASTICSEARCH:
                return null;
            default:
                return new JdbcConnectivityChecker();
        }
    }
}
