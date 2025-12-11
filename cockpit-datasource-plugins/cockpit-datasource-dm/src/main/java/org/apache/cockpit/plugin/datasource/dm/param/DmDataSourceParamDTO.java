package org.apache.cockpit.plugin.datasource.dm.param;

import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class DmDataSourceParamDTO extends BaseDataSourceParamDTO {

    @Override
    public String toString() {
        return "DamengDatasourceParamDTO{"
                + "name='" + database + '\''
                + ", host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", userName='" + username + '\''
                + ", other='" + other + '\''
                + '}';
    }

    @Override
    public DbType getType() {
        return DbType.DAMENG;
    }
}
