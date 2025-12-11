package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.plugin.datasource.elasticsearch.enums.ConnectionStrategy;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ElasticsearchClientLoader {

    private static final Map<ConnectionStrategy, String[]> CLASS_DEPENDENCIES =
            Collections.unmodifiableMap(new HashMap<ConnectionStrategy, String[]>() {{
                put(ConnectionStrategy.REST_HIGH_LEVEL_CLIENT,
                        new String[]{
                                "org.elasticsearch.client.RestHighLevelClient",
                        });
                put(ConnectionStrategy.NEW_JAVA_CLIENT,
                        new String[]{
                                "co.elastic.clients.elasticsearch.ElasticsearchClient",
                                "co.elastic.clients.elasticsearch.cluster.HealthRequest"
                        });
            }});

    /**
     * Check if classes are available with external JAR paths
     */
    public boolean isStrategyAvailable(ConnectionStrategy strategy, String... jarPaths) {
        String[] classNames = CLASS_DEPENDENCIES.get(strategy);
        if (classNames == null) return false;

        ClassLoader classLoader = createClassLoaderWithJars(jarPaths);

        for (String className : classNames) {
            try {
                if (classLoader != null) {
                    // Use custom class loader with external JARs
                    Class.forName(className, false, classLoader);
                } else {
                    // Use default class loader
                    Class.forName(className);
                }
            } catch (ClassNotFoundException e) {
                log.info("Class {} is not available, strategy {} is not supported", className, strategy);
                return false;
            }
        }
        return true;
    }

    /**
     * Create a class loader with external JAR files
     */
    public ClassLoader createClassLoaderWithJars(String... jarPaths) {
        if (jarPaths == null || jarPaths.length == 0) {
            return null;
        }

        try {
            URL[] urls = new URL[jarPaths.length];
            for (int i = 0; i < jarPaths.length; i++) {
                File jarFile = new File(jarPaths[i]);
                if (jarFile.exists()) {
                    urls[i] = jarFile.toURI().toURL();
                    log.info("Added JAR to classpath: {}", jarPaths[i]);
                } else {
                    log.warn("JAR file not found: {}", jarPaths[i]);
                }
            }

            // Create a new class loader with the external JARs and parent as the current class loader
            return new URLClassLoader(urls, this.getClass().getClassLoader());
        } catch (Exception e) {
            log.error("Failed to create class loader with external JARs", e);
            return null;
        }
    }

    /**
     * Get available connection strategy (by priority) with external JAR support
     */
    public ConnectionStrategy getAvailableStrategy(ConnectionStrategy preferred, String... jarPaths) {
        if (isStrategyAvailable(preferred, jarPaths)) {
            return preferred;
        }

        for (ConnectionStrategy strategy : ConnectionStrategy.values()) {
            if (strategy != preferred && isStrategyAvailable(strategy, jarPaths)) {
                log.info("Using fallback connection strategy: {}", strategy);
                return strategy;
            }
        }

        return ConnectionStrategy.HTTP;
    }

}