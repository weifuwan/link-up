package org.apache.cockpit.connectors.oracle.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcConnectionConfig;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

import javax.annotation.Nonnull;

/** Factory for {@link OracleDialect}. */
@AutoService(JdbcDialectFactory.class)
public class OracleDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:oracle:thin:");
    }

    @Override
    public JdbcDialect create() {
        return new OracleDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return create(compatibleMode, fieldIde, null);
    }

    @Override
    public JdbcDialect create(
            @Nonnull String compatibleMode,
            String fieldIde,
            JdbcConnectionConfig jdbcConnectionConfig) {
        boolean handleBlobAsString =
                jdbcConnectionConfig != null && jdbcConnectionConfig.isHandleBlobAsString();
        return new OracleDialect(fieldIde, handleBlobAsString);
    }
}
