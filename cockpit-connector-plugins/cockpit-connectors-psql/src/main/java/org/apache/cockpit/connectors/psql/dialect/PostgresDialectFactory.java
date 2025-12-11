package org.apache.cockpit.connectors.psql.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class PostgresDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public JdbcDialect create() {
        throw new UnsupportedOperationException(
                "Can't create JdbcDialect without compatible mode for Postgres");
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        if ("postgresLow".equalsIgnoreCase(compatibleMode)) {
            return new PostgresLowDialect(fieldIde);
        }
        return new PostgresDialect(fieldIde);
    }
}
