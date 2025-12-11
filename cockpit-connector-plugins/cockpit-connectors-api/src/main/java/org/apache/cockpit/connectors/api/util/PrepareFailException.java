package org.apache.cockpit.connectors.api.util;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.api.constant.PluginType;

public class PrepareFailException extends SeaTunnelRuntimeException {

    public PrepareFailException(String pluginName, PluginType type, String message) {
        super(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format(
                        "PluginName: %s, PluginType: %s, Message: %s",
                        pluginName, type.getType(), message));
    }

    public PrepareFailException(
            String pluginName, PluginType type, String message, Throwable cause) {
        super(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format(
                        "PluginName: %s, PluginType: %s, Message: %s",
                        pluginName, type.getType(), message),
                cause);
    }
}
