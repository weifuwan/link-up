package org.apache.cockpit.connectors.cache.catalog;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.options.JdbcCommonOptions;
import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;
import org.apache.commons.lang3.StringUtils;

@AutoService(Factory.class)
public class CacheCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return DatabaseIdentifier.CACHE;
    }

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        String urlWithDatabase = options.get(JdbcCommonOptions.URL);
        Preconditions.checkArgument(
                StringUtils.isNoneBlank(urlWithDatabase),
                "Miss config <url>! Please check your config.");
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(urlWithDatabase);
        return new CacheCatalog(
                catalogName,
                options.get(JdbcCommonOptions.USERNAME),
                options.get(JdbcCommonOptions.PASSWORD),
                urlInfo,
                options.get(JdbcCommonOptions.DRIVER),
                options.get(JdbcOptions.DRIVER_LOCATION)
                );
    }

    @Override
    public OptionRule optionRule() {
        return JdbcCommonOptions.BASE_CATALOG_RULE.build();
    }
}
