package org.apache.cockpit.connectors.api.options;



import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public interface TableSchemaOptions {
    Option<Map<String, Object>> SCHEMA =
            Options.key("schema")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema");

    Option<List<Map<String, Object>>> TABLE_CONFIGS =
            Options.key("tables_configs")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel Multi Table Schema, acts on unstructed data sources. "
                                    + "such as file, assert, mongodb, etc");
}
