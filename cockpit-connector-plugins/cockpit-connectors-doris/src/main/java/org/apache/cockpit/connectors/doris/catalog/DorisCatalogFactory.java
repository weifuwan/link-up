package org.apache.cockpit.connectors.doris.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.doris.config.DorisBaseOptions;
import org.apache.cockpit.connectors.doris.config.DorisSinkOptions;

import static org.apache.cockpit.connectors.doris.config.DorisSinkOptions.SAVE_MODE_CREATE_TEMPLATE;

@AutoService(Factory.class)
public class DorisCatalogFactory implements CatalogFactory {


    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new DorisCatalog(
                catalogName,
                options.get(DorisBaseOptions.FENODES),
                options.get(DorisBaseOptions.QUERY_PORT),
                options.get(DorisBaseOptions.USERNAME),
                options.get(DorisBaseOptions.PASSWORD),
                options.get(SAVE_MODE_CREATE_TEMPLATE),
                options.get(DorisSinkOptions.DEFAULT_DATABASE));
    }

    @Override
    public String factoryIdentifier() {
        return DbType.DORIS.getCode();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisBaseOptions.FENODES,
                        DorisBaseOptions.QUERY_PORT,
                        DorisBaseOptions.USERNAME,
                        DorisBaseOptions.PASSWORD)
                .build();
    }
}
