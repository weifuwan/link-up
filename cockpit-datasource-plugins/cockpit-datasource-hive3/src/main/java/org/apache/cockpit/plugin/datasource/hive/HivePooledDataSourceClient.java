package org.apache.cockpit.plugin.datasource.hive;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.client.BasePooledDataSourceClient;

import java.sql.Connection;
import java.sql.SQLException;


@Slf4j
public class HivePooledDataSourceClient extends BasePooledDataSourceClient {

    public HivePooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

    public HikariDataSource createDataSourcePool(BaseConnectionParam baseConnectionParam, DbType dbType) {
        return super.createDataSourcePool(baseConnectionParam, dbType);
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("连接失败");
        }
    }

    @Override
    public void close() {
        super.close();
        log.info("Closed Hive datasource client.");

    }
}
