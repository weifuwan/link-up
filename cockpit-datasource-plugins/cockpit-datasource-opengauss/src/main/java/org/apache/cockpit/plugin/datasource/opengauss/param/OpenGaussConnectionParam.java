package org.apache.cockpit.plugin.datasource.opengauss.param;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DriverType;

public class OpenGaussConnectionParam extends BaseConnectionParam {

    protected DriverType driverType;

    public DriverType getDriverType() {
        return driverType;
    }

    public void setDriverType(DriverType connectType) {
        this.driverType = connectType;
    }


    private String schema;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "OpenGaussConnectionParam{"
                + "username='" + username + '\''
                + ", password='" + password + '\''
                + ", address='" + address + '\''
                + ", database='" + database + '\''
                + ", jdbcUrl='" + jdbcUrl + '\''
                + ", driverLocation='" + driverLocation + '\''
                + ", driverClassName='" + driverClassName + '\''
                + ", validationQuery='" + validationQuery + '\''
                + ", other='" + other + '\''
                + '}';
    }
}
