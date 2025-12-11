package org.apache.cockpit.connectors.api.discovery;

import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;

public class SeaTunnelFactoryDiscovery extends AbstractPluginDiscovery<Factory> {

    private final Class<? extends Factory> factoryClass;

    public SeaTunnelFactoryDiscovery(Class<? extends Factory> factoryClass) {
        super();
        this.factoryClass = factoryClass;
    }

    public SeaTunnelFactoryDiscovery(
            Class<? extends Factory> factoryClass,
            BiConsumer<ClassLoader, List<URL>> addURLToClassLoader) {
        super(addURLToClassLoader);
        this.factoryClass = factoryClass;
    }

    @Override
    protected Class<Factory> getPluginBaseClass() {
        return Factory.class;
    }

    @Override
    protected Factory loadPluginInstance(
            PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<Factory> serviceLoader =
                ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (Factory factory : serviceLoader) {
            if (factoryClass.isInstance(factory)) {
                String factoryIdentifier = factory.factoryIdentifier();
                String pluginName = pluginIdentifier.getPluginName();
                if (StringUtils.equalsIgnoreCase(factoryIdentifier, pluginName)) {
                    return factory;
                }
            }
        }
        return null;
    }
}
