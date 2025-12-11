package org.apache.cockpit.connectors.api.common;

/** todo: unified with Plugin */
public interface PluginIdentifierInterface {
    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    String getPluginName();
}
