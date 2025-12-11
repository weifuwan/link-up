package org.apache.cockpit.connectors.mysql.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

import javax.annotation.Nonnull;

@AutoService(JdbcDialectFactory.class)
public class MySqlDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.MYSQL;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public JdbcDialect create() {
        return new MysqlDialect();
    }

    @Override
    public JdbcDialect create(@Nonnull String compatibleMode, String fieldIde) {
//        if (DatabaseIdentifier.STARROCKS.equalsIgnoreCase(compatibleMode)) {
//            return new StarRocksDialect(fieldIde);
//        }
        return new MysqlDialect(fieldIde);
    }
}
