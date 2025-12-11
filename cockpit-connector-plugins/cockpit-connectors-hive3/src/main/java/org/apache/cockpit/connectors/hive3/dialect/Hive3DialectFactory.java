package org.apache.cockpit.connectors.hive3.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class Hive3DialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DbType.HIVE3.getCode();
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:hive2:");
    }

    @Override
    public JdbcDialect create() {
        return new Hive3Dialect( FieldIdeEnum.ORIGINAL.getValue());
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
        return new Hive3Dialect(fieldIde);
    }
}
