package org.apache.cockpit.connectors.api.jdbc.catalog;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.Options;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;

public interface JdbcCatalogOptions {
    Option<String> BASE_URL =
            Options.key("base-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "URL has to be with database, like \"jdbc:mysql://localhost:5432/db\" or"
                                    + "\"jdbc:mysql://localhost:5432/db?useSSL=true\".");

    Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to use when connecting to the database server.");

    Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    Option<String> SCHEMA =
            Options.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "for databases that support the schema parameter, give it priority.");

    Option<String> COMPATIBLE_MODE =
            Options.key("compatibleMode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The compatible mode of database, required when the database supports multiple compatible modes. "
                                    + "For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.");

    Option<Boolean> HANDLE_BLOB_AS_STRING = JdbcOptions.HANDLE_BLOB_AS_STRING;

    OptionRule.Builder BASE_RULE =
            OptionRule.builder()
                    .required(BASE_URL)
                    .required(USERNAME, PASSWORD)
                    .optional(SCHEMA, JdbcOptions.DECIMAL_TYPE_NARROWING, HANDLE_BLOB_AS_STRING);

    Option<String> TABLE_PREFIX =
            Options.key("tablePrefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table prefix name added when the table is automatically created");

    Option<String> TABLE_SUFFIX =
            Options.key("tableSuffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table suffix name added when the table is automatically created");

    Option<Boolean> CREATE_INDEX =
            Options.key("create_index")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Create index or not when auto create table");

    Option<String> DRIVER = Options.key("driver").stringType().noDefaultValue();
}
