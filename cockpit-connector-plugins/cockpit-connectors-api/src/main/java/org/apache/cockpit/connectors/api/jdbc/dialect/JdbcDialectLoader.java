package org.apache.cockpit.connectors.api.jdbc.dialect;

import org.apache.cockpit.connectors.api.jdbc.config.JdbcConnectionConfig;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Utility for working with {@link JdbcDialect}. */
public final class JdbcDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectLoader.class);

    private JdbcDialectLoader() {}

    public static JdbcDialect load(String url, String dialect, String compatibleMode) {
        return load(url, compatibleMode, dialect, "", null);
    }

    public static JdbcDialect load(
            String url,
            String dialect,
            String compatibleMode,
            JdbcConnectionConfig jdbcConnectionConfig) {
        return load(url, compatibleMode, dialect, "", jdbcConnectionConfig);
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database URL.
     * @param compatibleMode The compatible mode.
     * @return The loaded dialect.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     */
    public static JdbcDialect load(
            String url, String compatibleMode, String dialect, String fieldIde) {
        return load(url, compatibleMode, dialect, fieldIde, null);
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param url A database URL.
     * @param compatibleMode The compatible mode.
     * @return The loaded dialect.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     */
    public static JdbcDialect load(
            String url,
            String compatibleMode,
            String dialect,
            String fieldIde,
            JdbcConnectionConfig jdbcConnectionConfig) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialectFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    String.format(
                            "Could not find any jdbc dialect factories that implement '%s' in the classpath.",
                            JdbcDialectFactory.class.getName()));
        }
        List<JdbcDialectFactory> matchingFactories;
        if (dialect != null) {
            matchingFactories =
                    foundFactories.stream()
                            .filter(f -> f.dialectFactoryName().equalsIgnoreCase(dialect))
                            .collect(Collectors.toList());
        } else {
            matchingFactories =
                    foundFactories.stream()
                            .filter(f -> f.acceptsURL(url))
                            .collect(Collectors.toList());
        }

        // filter out generic dialect factory
        if (matchingFactories.size() > 1) {
            matchingFactories =
                    matchingFactories.stream()
                            .filter(f -> !(f instanceof GenericDialectFactory))
                            .collect(Collectors.toList());
        }

        if (matchingFactories.size() > 1) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    String.format(
                            "Multiple jdbc dialect factories can handle url '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            url,
                            JdbcDialectFactory.class.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0).create(compatibleMode, fieldIde, jdbcConnectionConfig);
    }

    private static List<JdbcDialectFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcDialectFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc dialects factory.", e);
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    "Could not load service provider for jdbc dialects factory.",
                    e);
        }
    }
}
