package org.apache.cockpit.connectors.api.discovery;


import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.factory.FactoryUtil;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;

public class SeaTunnelSinkPluginDiscovery extends AbstractPluginDiscovery<SeaTunnelSink> {

    private static final String MULTITABLESINK_FACTORYIDENTIFIER = "MultiTableSink";

    public SeaTunnelSinkPluginDiscovery() {
//        super();
    }

    @Override
    public ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> getOptionRules(
            String pluginIdentifier) {
        return super.getOptionRules(pluginIdentifier);
    }

    @Override
    public LinkedHashMap<PluginIdentifier, OptionRule> getPlugins() {

        LinkedHashMap<PluginIdentifier, OptionRule> plugins = new LinkedHashMap<>();
        getPluginFactories().stream()
                .filter(
                        pluginFactory ->
                                !pluginFactory
                                        .factoryIdentifier()
                                        .equals(MULTITABLESINK_FACTORYIDENTIFIER)
                                        && TableSinkFactory.class.isAssignableFrom(
                                        pluginFactory.getClass()))
                .forEach(
                        pluginFactory ->
                                getPluginsByFactoryIdentifier(
                                        plugins,
                                        PluginType.SINK,
                                        pluginFactory.factoryIdentifier(),
                                        FactoryUtil.sinkFullOptionRule(
                                                (TableSinkFactory) pluginFactory)));
        return plugins;
    }

    public SeaTunnelSinkPluginDiscovery(BiConsumer<ClassLoader, List<URL>> addURLToClassLoader) {
        super(addURLToClassLoader);
    }

    @Override
    protected Class<SeaTunnelSink> getPluginBaseClass() {
        return SeaTunnelSink.class;
    }
}
