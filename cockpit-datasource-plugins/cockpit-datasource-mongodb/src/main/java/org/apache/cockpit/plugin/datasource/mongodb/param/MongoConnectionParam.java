package org.apache.cockpit.plugin.datasource.mongodb.param;


import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;

public class MongoConnectionParam extends BaseConnectionParam {

    private String connectionString;
    private String version;

    public String getConnectionString() {
        return connectionString;
    }

    private String authDatabase;

    public String getAuthDatabase() {
        return "admin";
    }

    public void setAuthDatabase(String authDatabase) {
        this.authDatabase = authDatabase;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    private String host;
    private Integer port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "MongoConnectionParam{"
                + "username='" + username + '\''
                + ", password='" + password + '\''
                + ", address='" + address + '\''
                + ", database='" + database + '\''
                + ", connectionString='" + connectionString + '\''
                + ", version='" + version + '\''
                + ", driverLocation='" + driverLocation + '\''
                + ", driverClassName='" + driverClassName + '\''
                + ", validationQuery='" + validationQuery + '\''
                + ", other='" + other + '\''
                + '}';
    }
}
