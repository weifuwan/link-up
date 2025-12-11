package org.apache.cockpit.connectors.influxdb.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

public class InfluxDBSourceOptions extends InfluxDBCommonOptions {

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql");

    public static final Option<String> SQL_WHERE =
            Options.key("where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb server query sql where condition");

    public static final Option<String> SPLIT_COLUMN =
            Options.key("split_column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the influxdb column which is used as split key");

    public static final Option<Integer> PARTITION_NUM =
            Options.key("partition_num")
                    .intType()
                    .defaultValue(0)
                    .withDescription("the influxdb server partition num");

    public static final Option<Integer> UPPER_BOUND =
            Options.key("upper_bound")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb server upper bound");

    public static final Option<Integer> LOWER_BOUND =
            Options.key("lower_bound")
                    .intType()
                    .noDefaultValue()
                    .withDescription("the influxdb server lower bound");
}
