package org.apache.cockpit.connectors.clickhouse.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions;

@AutoService(Factory.class)
public class ClickhouseCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "clickhouse";

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new ClickhouseCatalog(options, catalogName);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(ClickhouseBaseOptions.HOST)
                .required(ClickhouseBaseOptions.DATABASE)
                .required(ClickhouseBaseOptions.USERNAME)
                .required(ClickhouseBaseOptions.PASSWORD)
                .build();
    }
}
