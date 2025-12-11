package org.apache.cockpit.connectors.db2.dialect;


import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;

public class DB2JdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.DB_2;
    }
}
