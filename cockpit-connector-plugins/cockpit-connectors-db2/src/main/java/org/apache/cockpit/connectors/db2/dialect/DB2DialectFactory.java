
package org.apache.cockpit.connectors.db2.dialect;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectFactory;

@AutoService(JdbcDialectFactory.class)
public class DB2DialectFactory implements JdbcDialectFactory {

    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.DB_2;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:db2:");
    }

    @Override
    public JdbcDialect create() {
        return new DB2Dialect();
    }
}
