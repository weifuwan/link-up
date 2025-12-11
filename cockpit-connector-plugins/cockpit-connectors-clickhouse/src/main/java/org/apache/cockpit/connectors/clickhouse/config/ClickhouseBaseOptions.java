package org.apache.cockpit.connectors.clickhouse.config;

import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

public class ClickhouseBaseOptions {

    /** Clickhouse server host */
    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server host");

    /** Clickhouse database name */
    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse database name");

    /** Clickhouse table path */
    public static final Option<String> TABLE_PATH =
            Options.key("table_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path to the full path of table");

    /** Clickhouse server username */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server username");

    /** Clickhouse server password */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse server password");

    /** Clickhouse server timezone */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue(ZoneId.systemDefault().getId())
                    .withDescription(
                            "The session time zone in database server."
                                    + "If not set, then ZoneId.systemDefault() is used to determine the server time zone");

    public static final Option<Map<String, String>> CLICKHOUSE_CONFIG =
            Options.key("clickhouse.config")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("Clickhouse custom config");
}
