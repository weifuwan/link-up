package org.apache.cockpit.connectors.api.factory;


import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

public interface CatalogFactory extends Factory {

    /** Creates a {@link Catalog} using the options. */
    Catalog createCatalog(String catalogName, ReadonlyConfig options);
}
