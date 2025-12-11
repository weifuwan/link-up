package org.apache.cockpit.plugin.datasource.starrocks;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;

public class StarRocksAdHocDataSourceClient extends BaseAdHocDataSourceClient {

    public StarRocksAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}
