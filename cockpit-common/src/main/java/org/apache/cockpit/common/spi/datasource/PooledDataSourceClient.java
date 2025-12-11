
package org.apache.cockpit.common.spi.datasource;

import org.apache.cockpit.common.spi.enums.DbType;

import javax.sql.DataSource;

/**
 * This is a marker interface for pooled data source client, which means the connection is pooled.
 */
public interface PooledDataSourceClient extends DataSourceClient {

    DataSource createDataSourcePool(BaseConnectionParam baseConnectionParam, DbType dbType);

}
