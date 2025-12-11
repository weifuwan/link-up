package org.apache.cockpit.plugin.datasource.elasticsearch;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;

public class ElasticSearchAdHocDataSourceClient extends BaseAdHocDataSourceClient {

    public ElasticSearchAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}
