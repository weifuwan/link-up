package org.apache.cockpit.plugin.datasource.clickhouse;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;

public class ClickHouseAdHocDataSourceClient extends BaseAdHocDataSourceClient {

    public ClickHouseAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}