package org.apache.test;

import java.sql.*;
import java.util.Properties;

public class Test {
    public static void main(String[] args) {


        try {
            // 明确加载驱动
            Class.forName("org.postgresql.Driver");

            String url = "jdbc:postgresql://192.168.1.115:8090/dmp";
            Properties props = new Properties();
            props.setProperty("user", "wfw");
            props.setProperty("password", "wfw123@@");
//            props.setProperty("binaryTransferDisable", "box,path,polygon,circle,line,lseg,box3d");
//            props.setProperty("compatibleMode", "postgresql");

            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("连接成功！");

            // 执行查询测试
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT version()");
            if (rs.next()) {
                System.out.println("数据库版本: " + rs.getString(1));
            }

            conn.close();
        } catch (ClassNotFoundException e) {
            System.err.println("找不到 openGauss 驱动");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("数据库连接失败");
            e.printStackTrace();
        }

    }
}
