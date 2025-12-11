package org.apache.cockpit.plugin.datasource.mysql;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

public class MySQLPooledDataSourceClient extends BasePooledDataSourceClient {

    public MySQLPooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

}
