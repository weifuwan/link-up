
package org.apache.cockpit.connectors.api.jdbc.sink;

import org.apache.cockpit.connectors.api.config.ConnectorCommonOptions;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

public class SinkConnectorCommonOptions extends ConnectorCommonOptions {

    public static Option<Integer> MULTI_TABLE_SINK_REPLICA =
            Options.key("multi_table_sink_replica")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The replica number of multi table sink writer");
}
