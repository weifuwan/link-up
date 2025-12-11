package org.apache.cockpit.connectors.api.options;



import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.Map;

// We should use ColumnOptions instead of FieldOptions
@Deprecated
public interface FieldOptions {

    Option<Map<String, Object>> FIELDS =
            Options.key("schema.fields")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Fields");
}
