package org.apache.cockpit.connectors.dm.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;

/** Factory for {@link DmdbDialect}. */
@AutoService(JdbcDialectFactory.class)
public class DmdbDialectFactory implements JdbcDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:dm:");
    }

    @Override
    public JdbcDialect create() {
        return create(null, FieldIdeEnum.ORIGINAL.getValue());
    }

    @Override
    public JdbcDialect create(String compatibleMode, String fieldIde) {
        return new DmdbDialect(fieldIde);
    }
}
