package org.apache.cockpit.connectors.api.discovery;

import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * Plugins discovery interface, used to find plugin. Each plugin type should have its own
 * implementation.
 *
 * @param <T> plugin type
 */
public interface PluginDiscovery<T> {

    /**
     * Get plugin instance by plugin identifier.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance. If not found, throw IllegalArgumentException.
     */
    T createPluginInstance(PluginIdentifier pluginIdentifier);

    /**
     * Get plugin instance by plugin identifier.
     *
     * @param pluginIdentifier plugin identifier.
     * @param pluginJars used to help plugin load
     * @return plugin instance. If not found, throw IllegalArgumentException.
     */
    T createPluginInstance(PluginIdentifier pluginIdentifier, Collection<URL> pluginJars);

    /**
     * Get plugin instance by plugin identifier.
     *
     * @param pluginIdentifier plugin identifier.
     * @param pluginJars used to help plugin load
     * @return plugin instance. If not found, return Optional.empty().
     */
    Optional<T> createOptionalPluginInstance(
            PluginIdentifier pluginIdentifier, Collection<URL> pluginJars);

    /**
     * Get all plugins(connectors and transforms)
     *
     * @return plugins with optionRules
     */
    default LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Get option rules of the plugin by the plugin identifier
     *
     * @param pluginIdentifier pluginIdentifier
     * @return left: pluginIdentifier middle: requiredOptions right: optionalOptions
     */
    default ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
