
package org.apache.cockpit.plugin.datasource.api.plugin;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.*;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.plugin.datasource.api.utils.DataSourceUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DataSourceClientProvider {

    // We use the cache here to avoid creating a new datasource client every time,
    // One DataSourceClient corresponds to one unique datasource.
    private static final Cache<String, PooledDataSourceClient> POOLED_DATASOURCE_CLIENT_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(24L,
                            TimeUnit.HOURS)
                    .removalListener((RemovalListener<String, PooledDataSourceClient>) notification -> {
                        try (PooledDataSourceClient closedClient = notification.getValue()) {
                            log.info("Datasource: {} is removed from cache due to expire", notification.getKey());
                        } catch (Exception e) {
                            log.error("Close datasource client error", e);
                        }
                    })
                    .maximumSize(100)
                    .build();
    private static final DataSourcePluginManager dataSourcePluginManager = new DataSourcePluginManager();

    static {
        dataSourcePluginManager.installPlugin();
    }

    public static DataSourceClient getPooledDataSourceClient(DbType dbType,
                                                             ConnectionParam connectionParam) throws ExecutionException {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        String datasourceUniqueId = DataSourceUtils.getDatasourceUniqueId(baseConnectionParam, dbType);
        return POOLED_DATASOURCE_CLIENT_CACHE.get(datasourceUniqueId, () -> {
            DataSourceChannel dataSourceChannel = dataSourcePluginManager.getDataSourceChannel(dbType);
            if (null == dataSourceChannel) {
                throw new RuntimeException(String.format("datasource plugin '%s' is not found", dbType.getName()));
            }
            return dataSourceChannel.createPooledDataSourceClient(baseConnectionParam, dbType);
        });
    }

    public static Connection getPooledConnection(DbType dbType,
                                                 ConnectionParam connectionParam) throws SQLException, ExecutionException {
        return getPooledDataSourceClient(dbType, connectionParam).getConnection();
    }

    public static AdHocDataSourceClient getAdHocDataSourceClient(DbType dbType, ConnectionParam connectionParam) {
        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) connectionParam;
        DataSourceChannel dataSourceChannel = dataSourcePluginManager.getDataSourceChannel(dbType);
        if (null == dataSourceChannel) {
            throw new RuntimeException(String.format("datasource plugin '%s' is not found", dbType.getName()));
        }
        return dataSourceChannel.createAdHocDataSourceClient(baseConnectionParam, dbType);
    }

    public static Connection getAdHocConnection(DbType dbType,
                                                ConnectionParam connectionParam) throws SQLException, ExecutionException {
        return getAdHocDataSourceClient(dbType, connectionParam).getConnection();
    }
}
