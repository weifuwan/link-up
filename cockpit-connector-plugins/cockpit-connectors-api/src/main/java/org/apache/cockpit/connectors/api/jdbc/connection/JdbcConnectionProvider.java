package org.apache.cockpit.connectors.api.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;

/** JDBC connection provider. */
public interface JdbcConnectionProvider {
    /**
     * Get existing connection.
     *
     * @return existing connection
     */
    Connection getConnection();

    /**
     * Check whether possible existing connection is valid or not through {@link
     * Connection#isValid(int)}.
     *
     * @return true if existing connection is valid
     * @throws SQLException sql exception throw from {@link Connection#isValid(int)}
     */
    boolean isConnectionValid() throws SQLException;

    /**
     * Get existing connection or establish an new one if there is none.
     *
     * @return existing connection or newly established connection
     * @throws SQLException sql exception
     * @throws ClassNotFoundException driver class not found
     */
    Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException;

    /** Close possible existing connection. */
    void closeConnection();

    /**
     * Close possible existing connection and establish an new one.
     *
     * @return newly established connection
     * @throws SQLException sql exception
     * @throws ClassNotFoundException driver class not found
     */
    Connection reestablishConnection() throws SQLException, ClassNotFoundException;
}
