package org.apache.cockpit.connectors.api.jdbc.connection;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcConnectionConfig;
import org.apache.cockpit.connectors.api.jdbc.sink.ConnectionPoolManager;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class SimpleJdbcConnectionPoolProviderProxy implements JdbcConnectionProvider {

    private final transient ConnectionPoolManager poolManager;
    private final JdbcConnectionConfig jdbcConfig;
    private final int queueIndex;

    public SimpleJdbcConnectionPoolProviderProxy(
            ConnectionPoolManager poolManager, JdbcConnectionConfig jdbcConfig, int queueIndex) {
        this.jdbcConfig = jdbcConfig;
        this.poolManager = poolManager;
        this.queueIndex = queueIndex;
    }

    @Override
    public Connection getConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return poolManager.containsConnection(queueIndex)
                && poolManager
                        .getConnection(queueIndex)
                        .isValid(jdbcConfig.getConnectionCheckTimeoutSeconds());
    }

    @Override
    public Connection getOrEstablishConnection() {
        return poolManager.getConnection(queueIndex);
    }

    @Override
    public void closeConnection() {
        if (poolManager.containsConnection(queueIndex)) {
            try {
                poolManager.remove(queueIndex).close();
            } catch (SQLException e) {
                log.warn("JDBC connection close failed.", e);
            }
        }
    }

    @Override
    public Connection reestablishConnection() {
        closeConnection();
        return getOrEstablishConnection();
    }
}
