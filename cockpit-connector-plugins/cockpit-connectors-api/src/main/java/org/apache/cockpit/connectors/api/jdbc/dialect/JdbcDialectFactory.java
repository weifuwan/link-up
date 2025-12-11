package org.apache.cockpit.connectors.api.jdbc.dialect;


import org.apache.cockpit.connectors.api.jdbc.config.JdbcConnectionConfig;

/**
 * A factory to create a specific {@link JdbcDialect}
 *
 * @see JdbcDialect
 */
public interface JdbcDialectFactory {

    /**
     * Retrieves the name of the dialect.
     *
     * @return the name of the dialect
     */
    String dialectFactoryName();
    /**
     * Retrieves whether the dialect thinks that it can open a connection to the given URL.
     * Typically, dialects will return <code>true</code> if they understand the sub-protocol
     * specified in the URL and <code>false</code> if they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this dialect understands the given URL; <code>false</code>
     *     otherwise.
     */
    boolean acceptsURL(String url);

    /** @return Creates a new instance of the {@link JdbcDialect}. */
    JdbcDialect create();

    /**
     * Create a {@link JdbcDialect} instance based on the driver type and compatible mode.
     *
     * @param compatibleMode The compatible mode
     * @param fieldId The field identifier enumeration value
     * @return a new instance of {@link JdbcDialect}
     */
    default JdbcDialect create(String compatibleMode, String fieldId) {
        return create();
    }

    /**
     * Create a {@link JdbcDialect} instance based on the driver type, compatible mode, and JDBC
     * connection config.
     *
     * @param compatibleMode The compatible mode
     * @param fieldId The field identifier enumeration value
     * @param jdbcConnectionConfig The JDBC connection configuration
     * @return a new instance of {@link JdbcDialect}
     */
    default JdbcDialect create(
            String compatibleMode, String fieldId, JdbcConnectionConfig jdbcConnectionConfig) {
        return create(compatibleMode, fieldId);
    }
}
