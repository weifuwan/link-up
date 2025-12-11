package org.apache.cockpit.connectors.api.constant;

/** The type of SeaTunnel plugin. */
public enum PluginType {
    SOURCE("source"),
    TRANSFORM("transform"),
    SINK("sink");

    private final String type;

    PluginType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
