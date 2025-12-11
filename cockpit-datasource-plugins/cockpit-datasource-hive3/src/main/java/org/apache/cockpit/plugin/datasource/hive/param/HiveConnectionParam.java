package org.apache.cockpit.plugin.datasource.hive.param;


import org.apache.cockpit.plugin.datasource.api.datasource.BaseHDFSConnectionParam;

public class HiveConnectionParam extends BaseHDFSConnectionParam {

    private String host;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "HiveConnectionParam{"
                + "username='" + username + '\''
                + ", password='" + password + '\''
                + ", address='" + address + '\''
                + ", database='" + database + '\''
                + ", jdbcUrl='" + jdbcUrl + '\''
                + ", driverLocation='" + driverLocation + '\''
                + ", driverClassName='" + driverClassName + '\''
                + ", validationQuery='" + validationQuery + '\''
                + ", other='" + other + '\''
                + ", principal='" + principal + '\''
                + ", javaSecurityKrb5Conf='" + javaSecurityKrb5Conf + '\''
                + ", loginUserKeytabUsername='" + loginUserKeytabUsername + '\''
                + ", loginUserKeytabPath='" + loginUserKeytabPath + '\''
                + '}';
    }
}
