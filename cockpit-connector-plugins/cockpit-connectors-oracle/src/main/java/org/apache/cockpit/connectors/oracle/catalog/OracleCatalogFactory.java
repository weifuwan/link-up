package org.apache.cockpit.connectors.oracle.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.OptionValidationException;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.jdbc.catalog.JdbcCatalogOptions;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;

import java.util.Optional;

@AutoService(Factory.class)
public class OracleCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCatalogOptions.BASE_URL);
        JdbcUrlUtil.UrlInfo urlInfo = OracleURLParser.parse(urlWithDatabase);
        Optional<String> defaultDatabase = urlInfo.getDefaultDatabase();
        if (!defaultDatabase.isPresent()) {
            throw new OptionValidationException(JdbcCatalogOptions.BASE_URL);
        }
        return new OracleCatalog(
                catalogName,
                options.get(JdbcCatalogOptions.USERNAME),
                options.get(JdbcCatalogOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCatalogOptions.SCHEMA),
                options.get(JdbcOptions.DECIMAL_TYPE_NARROWING),
                options.get(JdbcOptions.DRIVER),
                options.getOptional(JdbcOptions.HANDLE_BLOB_AS_STRING).orElse(false),
                options.get(JdbcOptions.DRIVER_LOCATION));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCatalogOptions.BASE_RULE.build();
    }
}
