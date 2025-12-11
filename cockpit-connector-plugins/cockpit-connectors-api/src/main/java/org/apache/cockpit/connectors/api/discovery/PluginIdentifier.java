package org.apache.cockpit.connectors.api.discovery;

import org.apache.commons.lang3.StringUtils;

/** Used to identify a plugin. */
public class PluginIdentifier {
    private final String engineType;
    private final String pluginType;
    private final String pluginName;

    private PluginIdentifier(String engineType, String pluginType, String pluginName) {
        this.engineType = engineType;
        this.pluginType = pluginType;
        this.pluginName = pluginName;
    }

    public static PluginIdentifier of(String engineType, String pluginType, String pluginName) {
        return new PluginIdentifier(engineType, pluginType, pluginName);
    }

    public String getEngineType() {
        return engineType;
    }

    public String getPluginType() {
        return pluginType;
    }

    public String getPluginName() {
        return pluginName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PluginIdentifier that = (PluginIdentifier) o;

        if (!StringUtils.equalsIgnoreCase(engineType, that.engineType)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(pluginType, that.pluginType)) {
            return false;
        }
        return StringUtils.equalsIgnoreCase(pluginName, that.pluginName);
    }

    @Override
    public int hashCode() {
        int result = engineType != null ? engineType.toLowerCase().hashCode() : 0;
        result = 31 * result + (pluginType != null ? pluginType.toLowerCase().hashCode() : 0);
        result = 31 * result + (pluginName != null ? pluginName.toLowerCase().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PluginIdentifier{"
                + "engineType='"
                + engineType
                + '\''
                + ", pluginType='"
                + pluginType
                + '\''
                + ", pluginName='"
                + pluginName
                + '\''
                + '}';
    }
}
