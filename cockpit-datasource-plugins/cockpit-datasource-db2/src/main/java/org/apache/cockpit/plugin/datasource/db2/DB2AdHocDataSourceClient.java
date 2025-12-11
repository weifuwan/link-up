package org.apache.cockpit.plugin.datasource.db2;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;

public class DB2AdHocDataSourceClient extends BaseAdHocDataSourceClient {

    public DB2AdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}
