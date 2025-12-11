package org.apache.cockpit.connectors.api.options;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

public interface TableIdentifierOptions {

    Option<Boolean> SCHEMA_FIRST =
            Options.key("schema_first")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Parse Schema First from table");

    Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Full Table Name");

    Option<String> TABLE_COMMENT =
            Options.key("comment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Comment");

    Option<String> DATABASE_NAME =
            Options.key("database_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Database Name");

    Option<String> SCHEMA_NAME =
            Options.key("schema_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Name");

    Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SeaTunnel Schema Table Name");
}
