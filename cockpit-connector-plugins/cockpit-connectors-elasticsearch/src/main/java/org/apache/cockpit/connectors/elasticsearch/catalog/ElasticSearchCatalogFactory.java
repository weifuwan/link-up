package org.apache.cockpit.connectors.elasticsearch.catalog;



import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.factory.CatalogFactory;
import org.apache.cockpit.connectors.api.factory.Factory;

@AutoService(Factory.class)
public class ElasticSearchCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new ElasticSearchCatalog(catalogName, "", options);
    }

    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }
}
