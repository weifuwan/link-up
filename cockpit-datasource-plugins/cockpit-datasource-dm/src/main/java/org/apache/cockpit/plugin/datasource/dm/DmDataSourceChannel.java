package org.apache.cockpit.plugin.datasource.dm;

import org.apache.cockpit.common.spi.datasource.AdHocDataSourceClient;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.PooledDataSourceClient;
import org.apache.cockpit.common.spi.enums.DbType;

public class DmDataSourceChannel implements DataSourceChannel {

    @Override
    public AdHocDataSourceClient createAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        return new DmAdHocDataSourceClient(baseConnectionParam, dbType);
    }

    @Override
    public PooledDataSourceClient createPooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        return new DmPooledDataSourceClient(baseConnectionParam, dbType);
    }
}
