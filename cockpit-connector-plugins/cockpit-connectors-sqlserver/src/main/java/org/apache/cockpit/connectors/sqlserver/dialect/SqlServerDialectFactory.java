package org.apache.cockpit.connectors.sqlserver.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class SqlServerDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:sqlserver:");
    }

    @Override
    public JdbcDialect create() {
        return new SqlServerDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new SqlServerDialect(fieldIde);
    }
}
