package org.apache.cockpit.connectors.starrocks.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSinkOptions;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSourceOptions;

@AutoService(Factory.class)
public class StarRocksCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = StarRocksSinkOptions.CONNECTOR_IDENTITY;

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new StarRocksCatalog(
                catalogName,
                options.get(StarRocksSourceOptions.USERNAME),
                options.get(StarRocksSourceOptions.PASSWORD),
                options.get(StarRocksSinkOptions.BASE_URL),
                options.get(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(StarRocksSinkOptions.BASE_URL)
                .required(StarRocksSourceOptions.USERNAME)
                .required(StarRocksSourceOptions.PASSWORD)
                .build();
    }
}
