package org.apache.cockpit.connectors.api.factory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.config.ConfigValidator;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.api.discovery.PluginIdentifier;
import org.apache.cockpit.connectors.api.discovery.SeaTunnelSinkPluginDiscovery;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Use SPI to create {@link TableSourceFactory}, {@link TableSinkFactory} and {@link
 * CatalogFactory}.
 */
@Slf4j
public final class FactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static <T, SplitT extends SourceSplit> SeaTunnelSource<T, SplitT> createAndPrepareSource(
            TableSourceFactory factory, ReadonlyConfig options, ClassLoader classLoader) {
        TableSourceFactoryContext context = new TableSourceFactoryContext(options, classLoader);
        ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
        TableSource<T, SplitT> tableSource = factory.createSource(context);
        return tableSource.createSource();
    }

    public static <IN> SeaTunnelSink<IN> createAndPrepareSink(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            ClassLoader classLoader,
            String factoryIdentifier,
            TableSinkFactory tableSinkFactory) {
        try {

            // mongodb
            if (factoryIdentifier.equalsIgnoreCase(DbType.MONGODB.getCode())) {
                SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
                SeaTunnelSink sink =
                        sinkPluginDiscovery.createPluginInstance(
                                PluginIdentifier.of(
                                        "seatunnel",
                                        PluginType.SINK.getType(),
                                        factoryIdentifier));
                sink.prepare(config.toConfig());
                sink.setTypeInfo(catalogTable.getSeaTunnelRowType());

                return sink;
            }


            if (tableSinkFactory == null) {
                tableSinkFactory =
                        discoverFactory(classLoader, TableSinkFactory.class, factoryIdentifier);
            }

            TableSinkFactoryContext context =
                    TableSinkFactoryContext.replacePlaceholderAndCreate(
                            catalogTable,
                            config,
                            classLoader,
                            tableSinkFactory.excludeTablePlaceholderReplaceKeys());
            ConfigValidator.of(context.getOptions()).validate(tableSinkFactory.optionRule());

            LOG.info(
                    "Create sink '{}' with upstream input catalog-table[database: {}, schema: {}, table: {}]",
                    factoryIdentifier,
                    catalogTable.getTablePath().getDatabaseName(),
                    catalogTable.getTablePath().getSchemaName(),
                    catalogTable.getTablePath().getTableName());
            return tableSinkFactory.<IN>createSink(context).createSink();
        } catch (Throwable t) {
            throw new FactoryException(
                    String.format(
                            "Unable to create a sink for identifier '%s'.", factoryIdentifier),
                    t);
        }
    }


    public static Optional<Catalog> createOptionalCatalog(
            String catalogName,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryIdentifier) {
        Optional<CatalogFactory> optionalFactory =
                discoverOptionalFactory(classLoader, CatalogFactory.class, factoryIdentifier);
        return optionalFactory.map(
                catalogFactory -> catalogFactory.createCatalog(catalogName, options));
    }

    public static <T extends Factory> URL getFactoryUrl(T factory) {
        return factory.getClass().getProtectionDomain().getCodeSource().getLocation();
    }

    public static <T extends Factory> Optional<T> discoverOptionalFactory(
            ClassLoader classLoader,
            Class<T> factoryClass,
            String factoryIdentifier,
            Function<String, T> discoverOptionalFactoryFunction) {

        if (discoverOptionalFactoryFunction != null) {
            T apply = discoverOptionalFactoryFunction.apply(factoryIdentifier);
            if (apply != null) {
                return Optional.of(apply);
            } else {
                return Optional.empty();
            }
        }
        return discoverOptionalFactory(classLoader, factoryClass, factoryIdentifier);
    }

    public static <T extends Factory> Optional<T> discoverOptionalFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<T> foundFactories = discoverFactories(classLoader, factoryClass);
        if (foundFactories.isEmpty()) {
            return Optional.empty();
        }
        final List<T> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equalsIgnoreCase(factoryIdentifier))
                        .collect(Collectors.toList());
        if (matchingFactories.isEmpty()) {
            return Optional.empty();
        }
        checkMultipleMatchingFactories(factoryIdentifier, factoryClass, matchingFactories);
        return Optional.of(matchingFactories.get(0));
    }

    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<T> foundFactories = discoverFactories(classLoader, factoryClass);

        if (foundFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<T> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equalsIgnoreCase(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new FactoryException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::factoryIdentifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        checkMultipleMatchingFactories(factoryIdentifier, factoryClass, matchingFactories);

        return matchingFactories.get(0);
    }

    private static <T extends Factory> void checkMultipleMatchingFactories(
            String factoryIdentifier, Class<T> factoryClass, List<T> matchingFactories) {
        if (matchingFactories.size() > 1) {
            throw new FactoryException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Factory> List<T> discoverFactories(
            ClassLoader classLoader, Class<T> factoryClass) {
        return discoverFactories(classLoader).stream()
                .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                .map(f -> (T) f)
                .collect(Collectors.toList());
    }

    public static List<Factory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<Factory> result = new LinkedList<>();
            ServiceLoader.load(Factory.class, classLoader).iterator().forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for factories.", e);
            throw new FactoryException("Could not load service provider for factories.", e);
        }
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a source.
     *
     * @return Option rule
     */
    public static OptionRule sourceFullOptionRule(@NonNull TableSourceFactory factory) {
        OptionRule sourceOptionRule = factory.optionRule();
        if (sourceOptionRule == null) {
            throw new FactoryException("sourceOptionRule can not be null");
        }

        Class<? extends SeaTunnelSource> sourceClass = factory.getSourceClass();
//        if (factory instanceof SupportParallelism
//                // TODO: Implement SupportParallelism in the TableSourceFactory instead of the
//                // SeaTunnelSource
//                || SupportParallelism.class.isAssignableFrom(sourceClass)) {
//            OptionRule sourceCommonOptionRule =
//                    OptionRule.builder().optional(EnvCommonOptions.PARALLELISM).build();
//            sourceOptionRule
//                    .getOptionalOptions()
//                    .addAll(sourceCommonOptionRule.getOptionalOptions());
//        }

        return sourceOptionRule;
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a sink.
     *
     * @return Option rule
     */
    public static OptionRule sinkFullOptionRule(@NonNull TableSinkFactory factory) {
        OptionRule sinkOptionRule = factory.optionRule();
        if (sinkOptionRule == null) {
            throw new FactoryException("sinkOptionRule can not be null");
        }
        return sinkOptionRule;
    }

}
