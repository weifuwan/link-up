package org.apache.cockpit.plugin.datasource.db2.param;


import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class DB2DataSourceParamDTO extends BaseDataSourceParamDTO {

    @Override
    public String toString() {
        return "DB2DataSourceParamDTO{"
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
        return DbType.DB2;
    }
}
