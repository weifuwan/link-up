package org.apache.cockpit.connectors.sqlserver.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.options.JdbcCommonOptions;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;

@AutoService(Factory.class)
public class SqlServerCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String url = options.get(JdbcCommonOptions.URL);
        JdbcUrlUtil.UrlInfo urlInfo = SqlServerURLParser.parse(url);
        return new SqlServerCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.SCHEMA),
                options.get(JdbcCommonOptions.DRIVER),
                options.get(JdbcOptions.DRIVER_LOCATION));
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.BASE_CATALOG_RULE.build();
    }
}
