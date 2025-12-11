package org.apache.cockpit.connectors.cache.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class CacheDialectFactory implements JdbcDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.CACHE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:Cache:");
    }

    @Override
    public JdbcDialect create() {
        return new CacheDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new CacheDialect(fieldIde);
    }
}
