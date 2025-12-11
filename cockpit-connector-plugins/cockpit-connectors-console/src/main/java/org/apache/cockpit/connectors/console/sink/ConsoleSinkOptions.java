package org.apache.cockpit.connectors.console.sink;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;
import org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions;

public class ConsoleSinkOptions extends SinkConnectorCommonOptions {

    public static final Option<Boolean> LOG_PRINT_DATA =
            Options.key("log.print.data")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag to determine whether data should be printed in the logs.");

    public static final Option<Integer> LOG_PRINT_DELAY =
            Options.key("log.print.delay.ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Delay in milliseconds between printing each data item to the logs.");
}
