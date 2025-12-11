package org.apache.cockpit.plugin.datasource.opengauss.param;


import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.spi.enums.DriverType;
import org.apache.cockpit.plugin.datasource.api.datasource.BaseDataSourceParamDTO;

public class OpenGaussDataSourceParamDTO extends BaseDataSourceParamDTO {

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
        return "OpenGaussDataSourceParamDTO{"
                + "host='" + host + '\''
                + ", port=" + port
                + ", database='" + database + '\''
                + ", username='" + username + '\''
                + ", password='" + password + '\''
                + ", other='" + other + '\''
                + '}';
    }

    @Override
    public DbType getType() {
        return DbType.OPENGAUSS;
    }
}
