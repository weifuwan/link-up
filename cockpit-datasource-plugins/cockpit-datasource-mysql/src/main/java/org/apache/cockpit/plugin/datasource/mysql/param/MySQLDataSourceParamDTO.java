package org.apache.cockpit.plugin.datasource.mysql.param;

import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class MySQLDataSourceParamDTO extends BaseDataSourceParamDTO {

    @Override
    public String toString() {
        return "MySQLDataSourceParamDTO{"
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
        return DbType.MYSQL;
    }
}
