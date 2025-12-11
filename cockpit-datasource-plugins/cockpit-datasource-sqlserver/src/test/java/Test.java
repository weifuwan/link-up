import java.sql.*;

public class Test {
    public static void main(String[] args) {
        String connectionUrl = "jdbc:sqlserver://192.168.1.114:1433;databaseName=CompanyDB;user=company_user;password=Company123!;";

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            System.out.println("数据库连接成功！");

            // 简单的测试查询
            String sql = "SELECT 1 as test";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                if (resultSet.next()) {
                    System.out.println("测试查询结果: " + resultSet.getInt("test"));
                }
            }

        } catch (SQLException e) {
            System.out.println("数据库连接失败！");
            e.printStackTrace();
        }
    }
}
