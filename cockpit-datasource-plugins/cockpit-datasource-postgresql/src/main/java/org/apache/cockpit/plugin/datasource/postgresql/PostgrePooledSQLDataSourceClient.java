package org.apache.cockpit.plugin.datasource.postgresql;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

public class PostgrePooledSQLDataSourceClient extends BasePooledDataSourceClient {

    public PostgrePooledSQLDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

}
