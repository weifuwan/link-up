package org.apache.cockpit.connectors.api.options;


import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public interface ColumnOptions {

    // todo: how to define List<Map<String, Object>>
    Option<List<Map<String, Object>>> COLUMNS =
            Options.key("columns")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Columns");

    Option<String> COLUMN_NAME =
            Options.key("name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Name");

    Option<String> TYPE =
            Options.key("type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Type");

    Option<Integer> COLUMN_SCALE =
            Options.key("columnScale")
                    .intType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column scale");

    Option<Long> COLUMN_LENGTH =
            Options.key("columnLength")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("SeaTunnel Schema Column Length");

    Option<Boolean> NULLABLE =
            Options.key("nullable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("SeaTunnel Schema Column Nullable");

    Option<Object> DEFAULT_VALUE =
            Options.key("defaultValue")
                    .objectType(Object.class)
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Default Value");

    Option<String> COLUMN_COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Column Comment");
}
