package org.apache.cockpit.plugin.datasource.opengauss;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

public class OpenGaussPooledSQLDataSourceClient extends BasePooledDataSourceClient {

    public OpenGaussPooledSQLDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

}
