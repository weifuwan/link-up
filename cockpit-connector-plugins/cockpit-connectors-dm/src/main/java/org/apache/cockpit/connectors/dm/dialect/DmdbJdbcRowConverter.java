package org.apache.cockpit.connectors.dm.dialect;


import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;

public class DmdbJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.DAMENG;
    }
}
