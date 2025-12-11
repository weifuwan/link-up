package org.apache.cockpit.connectors.influxdb.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

public class InfluxDBCommonOptions {

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server username");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server password");

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server url");

    public static final Option<Long> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .longType()
                    .defaultValue(15000L)
                    .withDescription("the influxdb client connect timeout ms");

    public static final Option<Integer> QUERY_TIMEOUT_SEC =
            Options.key("query_timeout_sec")
                    .intType()
                    .defaultValue(3)
                    .withDescription("the influxdb client query timeout ms");

    public static final Option<String> DATABASES =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server database");

    public static final Option<String> EPOCH =
            Options.key("epoch")
                    .stringType()
                    .defaultValue("n")
                    .withDescription("the influxdb server query epoch");
}
