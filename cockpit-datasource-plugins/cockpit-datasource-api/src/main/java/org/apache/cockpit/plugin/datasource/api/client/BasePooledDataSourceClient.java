
package org.apache.cockpit.plugin.datasource.api.client;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.BaseConnectionParam;
import org.apache.cockpit.common.spi.datasource.PooledDataSourceClient;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public abstract class BasePooledDataSourceClient implements PooledDataSourceClient {

    protected final BaseConnectionParam baseConnectionParam;
    protected HikariDataSource dataSource;

    public BasePooledDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {

        this.baseConnectionParam = checkNotNull(baseConnectionParam, "baseConnectionParam is null");
        this.dataSource = createDataSourcePool(baseConnectionParam, checkNotNull(dbType, "dbType is null"));
    }

    // todo: support multiple version databases
    @Override
    public HikariDataSource createDataSourcePool(BaseConnectionParam baseConnectionParam, DbType dbType) {

        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setDriverClassName(baseConnectionParam.getDriverClassName());
        dataSource.setJdbcUrl(DataSourceUtils.getJdbcUrl(dbType, baseConnectionParam));
        dataSource.setUsername(baseConnectionParam.getUsername());
        dataSource.setPassword(PasswordUtils.decodePassword(baseConnectionParam.getPassword()));

        dataSource.setMinimumIdle(5);
        dataSource.setMaximumPoolSize(50);
        dataSource.setConnectionTestQuery(baseConnectionParam.getValidationQuery());

        if (MapUtils.isNotEmpty(baseConnectionParam.getOtherAsMap())) {
            baseConnectionParam.getOtherAsMap().forEach(dataSource::addDataSourceProperty);
        }

        log.info("Creating HikariDataSource for {} success.", dbType.name());
        return dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        log.info("do close dataSource {}.", baseConnectionParam.getDatabase());
        try (HikariDataSource closedDatasource = dataSource) {
            // only close the resource
        }
    }

}
