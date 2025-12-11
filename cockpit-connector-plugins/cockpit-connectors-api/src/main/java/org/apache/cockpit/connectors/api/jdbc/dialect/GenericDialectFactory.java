package org.apache.cockpit.connectors.api.jdbc.dialect;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

/** Factory for {@link GenericDialect}. */
@AutoService(JdbcDialectFactory.class)
public class GenericDialectFactory implements JdbcDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.GENERIC;
    }

    // GenericDialect does not have any special requirements.
    @Override
    public boolean acceptsURL(String url) {
        return true;
    }

    @Override
    public JdbcDialect create() {
        return new GenericDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new GenericDialect(fieldIde);
    }
}
