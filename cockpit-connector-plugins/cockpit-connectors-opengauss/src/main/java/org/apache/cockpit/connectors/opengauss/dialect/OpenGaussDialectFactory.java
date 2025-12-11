package org.apache.cockpit.connectors.opengauss.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;
import org.apache.cockpit.connectors.psql.dialect.PostgresDialectFactory;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class OpenGaussDialectFactory extends PostgresDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.OPENGAUSS;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:opengauss:");
    }

    @Override
    public JdbcDialect create() {
        return new OpenGaussDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new OpenGaussDialect();
    }
}
