package org.apache.cockpit.plugin.datasource.dm;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

public class DmPooledDataSourceClient extends BasePooledDataSourceClient {

    public DmPooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

}
