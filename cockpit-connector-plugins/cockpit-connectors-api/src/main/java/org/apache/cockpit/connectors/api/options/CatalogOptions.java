package org.apache.cockpit.connectors.api.options;



import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public interface CatalogOptions {

    @Deprecated
    Option<Map<String, String>> CATALOG_OPTIONS =
            Options.key("catalog")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("configuration options for the catalog.");

    Option<String> CATALOG_NAME =
            Options.key("name").stringType().noDefaultValue().withDescription("catalog name");

    Option<List<String>> TABLE_NAMES =
            Options.key("table-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "List of table names of databases to capture."
                                    + "The table name needs to include the database name, for example: database_name.table_name");

    Option<String> DATABASE_PATTERN =
            Options.key("database-pattern")
                    .stringType()
                    .defaultValue(".*")
                    .withDescription("The database names RegEx of the database to capture.");

    Option<String> TABLE_PATTERN =
            Options.key("table-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table names RegEx of the database to capture."
                                    + "The table name needs to include the database name, for example: database_.*\\.table_.*");

    Option<List<Map<String, Object>>> TABLE_LIST =
            Options.key("table_list")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Multi Table Schema, acts on structed data sources. "
                                    + "such as jdbc, paimon, doris, etc");
}
