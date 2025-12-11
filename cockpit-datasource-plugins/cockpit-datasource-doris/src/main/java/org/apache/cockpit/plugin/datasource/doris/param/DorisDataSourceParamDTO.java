package org.apache.cockpit.plugin.datasource.doris.param;


import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class DorisDataSourceParamDTO extends BaseDataSourceParamDTO {

    @Override
    public String toString() {
        return "DorisDataSourceParamDTO{"
                + ", host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", username='" + username + '\''
                + ", password='" + password + '\''
                + ", other='" + other + '\''
                + '}';
    }
    @Override
    public DbType getType() {
        return DbType.DORIS;
    }
}
