package org.apache.cockpit.connectors.cache.dialect;


import org.apache.cockpit.connectors.api.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;

public class CacheJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.CACHE;
    }
}
