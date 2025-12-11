package org.apache.cockpit.datasource.connection;

import org.apache.cockpit.datasource.connection.config.DriverConfig;
import org.apache.cockpit.datasource.connection.driver.IDriverManager;

import java.sql.Connection;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) throws SQLException {

        DriverConfig driverConfig = new DriverConfig();
        driverConfig.setJdbcDriverClass("com.intersys.jdbc.CacheDriver");
        driverConfig.setJdbcDriver("cachejdbc.jar");

        Connection root = IDriverManager.getConnection("jdbc:Cache://192.168.1.114:1972/%sys",
                "root",
                "123456",
                driverConfig);

        System.out.println("root = " + root);
    }
}
