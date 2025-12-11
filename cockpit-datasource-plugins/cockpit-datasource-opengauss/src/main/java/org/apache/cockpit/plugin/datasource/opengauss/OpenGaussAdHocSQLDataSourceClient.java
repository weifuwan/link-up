package org.apache.cockpit.plugin.datasource.opengauss;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;

public class OpenGaussAdHocSQLDataSourceClient extends BaseAdHocDataSourceClient {

    public OpenGaussAdHocSQLDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}
