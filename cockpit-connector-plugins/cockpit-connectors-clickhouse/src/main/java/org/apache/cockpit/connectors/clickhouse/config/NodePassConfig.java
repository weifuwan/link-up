package org.apache.cockpit.connectors.clickhouse.config;

import lombok.Data;
import org.apache.cockpit.connectors.api.config.OptionMark;

@Data
public class NodePassConfig {

    @OptionMark(description = "The address of Clickhouse server node")
    private String nodeAddress;

    @OptionMark(description = "Clickhouse server linux password")
    private String password;
}
