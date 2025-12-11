
package org.apache.cockpit.datasource.connection.config;



import java.io.Serializable;


public class DriverConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * url jdbc:mysql://192.168.1.113:3306/cockpit?allowPublicKeyRetrieval=true&useSSL=false
     */
    private String url;

    /**
     * jdbcDriver eg. mysql-connector-java-8.0.29.jar
     */
    private String jdbcDriver;

    /**
     * jdbcDriverClass eg.com.mysql.cj.jdbc.Driver
     */
    private String jdbcDriverClass;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }

    public void setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
    }
}