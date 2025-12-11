package org.apache.cockpit.connectors.api.options;



import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public interface PrimaryKeyOptions {

    Option<Map<String, Object>> PRIMARY_KEY =
            Options.key("primaryKey")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Fields");

    Option<String> PRIMARY_KEY_NAME =
            Options.key("name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Primary Key Name");

    Option<List<String>> PRIMARY_KEY_COLUMNS =
            Options.key("columnNames")
                    .listType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Primary Key Columns");
}
