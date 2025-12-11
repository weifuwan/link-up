
package org.apache.cockpit.plugin.datasource.api.client;


import org.apache.cockpit.common.spi.datasource.AdHocDataSourceClient;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.plugin.DataSourceProcessorProvider;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class BaseAdHocDataSourceClient implements AdHocDataSourceClient {

    private final BaseConnectionParam baseConnectionParam;
    private final DbType dbType;

    protected BaseAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        this.baseConnectionParam = baseConnectionParam;
        this.dbType = dbType;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return DataSourceProcessorProvider.getDataSourceProcessor(dbType).getConnection(baseConnectionParam);
        } catch (Exception e) {
            throw new SQLException("Create adhoc connection error", e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
