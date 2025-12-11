
package org.apache.cockpit.plugin.datasource.oracle;

import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BaseAdHocDataSourceClient;
public class OracleAdHocDataSourceClient extends BaseAdHocDataSourceClient {

    public OracleAdHocDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
}
