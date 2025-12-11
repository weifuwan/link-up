package org.apache.cockpit.plugin.datasource.elasticsearch;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

public class ElasticSearchPooledDataSourceClient extends BasePooledDataSourceClient {

    public ElasticSearchPooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

}
