package org.apache.cockpit.plugin.datasource.opengauss;


import org.apache.cockpit.common.spi.datasource.AdHocDataSourceClient;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.DataSourceChannel;
import org.apache.cockpit.common.spi.datasource.PooledDataSourceClient;
import org.apache.cockpit.common.spi.enums.DbType;

public class OpenGaussDataSourceChannel implements DataSourceChannel {

    @Override
    public AdHocDataSourceClient createAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        return new OpenGaussAdHocSQLDataSourceClient(baseConnectionParam, dbType);
    }

    @Override
    public PooledDataSourceClient createPooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        return new OpenGaussPooledSQLDataSourceClient(baseConnectionParam, dbType);
    }
}
