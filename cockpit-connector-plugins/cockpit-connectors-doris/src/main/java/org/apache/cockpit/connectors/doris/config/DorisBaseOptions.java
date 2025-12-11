package org.apache.cockpit.connectors.doris.config;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

public class DorisBaseOptions {

    public static final String IDENTIFIER = "Doris";

    // common option
    public static final Option<String> FENODES =
            Options.key("fenodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("doris fe http address.");

    public static final Option<Integer> QUERY_PORT =
            Options.key("query-port")
                    .intType()
                    .defaultValue(9030)
                    .withDescription("doris query port");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris user name.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the doris password.");

    public static final Option<String> TABLE =
            Options.key("table").stringType().noDefaultValue().withDescription("table");

    public static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue().withDescription("database");

    public static final Option<Integer> DORIS_BATCH_SIZE =
            Options.key("doris.batch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("the batch size of the doris read/write.");
}
