package org.apache.cockpit.plugin.datasource.hive.param;


import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseHDFSDataSourceParamDTO;

public class HiveDataSourceParamDTO extends BaseHDFSDataSourceParamDTO {

    @Override
    public String toString() {
        return "HiveDataSourceParamDTO{"
                + "host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", principal='" + principal + '\''
                + ", username='" + username + '\''
                + ", password='" + password + '\''
                + ", other='" + other + '\''
                + ", javaSecurityKrb5Conf='" + javaSecurityKrb5Conf + '\''
                + ", loginUserKeytabUsername='" + loginUserKeytabUsername + '\''
                + ", loginUserKeytabPath='" + loginUserKeytabPath + '\''
                + '}';
    }

    @Override
    public DbType getType() {
        return DbType.HIVE3;
    }
}
