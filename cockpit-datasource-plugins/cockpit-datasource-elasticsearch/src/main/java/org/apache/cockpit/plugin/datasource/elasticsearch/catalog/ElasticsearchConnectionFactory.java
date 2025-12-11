package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.plugin.datasource.elasticsearch.enums.ConnectionStrategy;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchConnectionParam;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Base64;

@Slf4j
public class ElasticsearchConnectionFactory {

    private final ElasticsearchVersionManager versionManager = new ElasticsearchVersionManager();
    private final ElasticsearchClientLoader clientLoader = new ElasticsearchClientLoader();

    /**
     * Test connection with external JAR support
     */
    public boolean checkDataSourceConnectivity(ElasticSearchConnectionParam connectionParam, String... jarPaths) {
        try {
            String version = versionManager.detectVersion(connectionParam);
            log.info("Detected Elasticsearch version: {}", version);

            ConnectionStrategy preferredStrategy = versionManager.getConnectionStrategy(version);

            ConnectionStrategy actualStrategy = clientLoader.getAvailableStrategy(preferredStrategy, jarPaths);
            log.info("Using connection strategy: {}", actualStrategy);

            return testConnection(connectionParam, actualStrategy, jarPaths);

        } catch (Exception e) {
            log.error("Connection test failed", e);
            return false;
        }
    }

    private boolean testConnection(ElasticSearchConnectionParam connectionParam, ConnectionStrategy strategy, String... jarPaths) {
        switch (strategy) {
            case REST_HIGH_LEVEL_CLIENT:
                return testWithRestHighLevelClient(connectionParam, jarPaths);
            case NEW_JAVA_CLIENT:
                return testWithNewJavaClient(connectionParam);
            case HTTP:
            default:
                return testWithHttp(connectionParam);
        }
    }

    /**
     * Test connection using RestHighLevelClient (7.x and below) with external JAR support
     */
    private boolean testWithRestHighLevelClient(ElasticSearchConnectionParam connectionParam, String... jarPaths) {
        return testWithRestHighLevelClientDirect(connectionParam);

    }

    private org.elasticsearch.client.RestClientBuilder createRestClientBuilderDirect(ElasticSearchConnectionParam connectionParam) {
        return org.elasticsearch.client.RestClient.builder(
                new org.apache.http.HttpHost(
                        connectionParam.getHost(),
                        connectionParam.getPort(),
                        "http"
                )
        );
    }

    /**
     * 使用编译时依赖（如果类在classpath中）
     */
    private boolean testWithRestHighLevelClientDirect(ElasticSearchConnectionParam connectionParam) {
        org.elasticsearch.client.RestHighLevelClient client = null;
        try {
            org.elasticsearch.client.RestClientBuilder restClientBuilder = createRestClientBuilderDirect(connectionParam);
            client = new org.elasticsearch.client.RestHighLevelClient(restClientBuilder);
            return client.ping(org.elasticsearch.client.RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("RestHighLevelClient direct connection test failed", e);
            return false;
        } finally {
            closeClientDirect(client);
        }
    }


    private void closeClientDirect(org.elasticsearch.client.RestHighLevelClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Failed to close direct client", e);
            }
        }
    }

    /**
     * Test connection using new Java client (8.x)
     * With compile-time dependencies but provided scope
     */
    private boolean testWithNewJavaClient(ElasticSearchConnectionParam connectionParam) {
        throw new RuntimeException("8.x 暂时不支持");
    }

    /**
     * Create RestClientBuilder directly
     */
    private org.elasticsearch.client.RestClientBuilder createRestClientBuilder(ElasticSearchConnectionParam connectionParam) {
        return org.elasticsearch.client.RestClient.builder(
                new org.apache.http.HttpHost(
                        connectionParam.getHost(),
                        connectionParam.getPort(),
                        "http"
                )
        );
    }

    /**
     * Generic HTTP test
     */
    private boolean testWithHttp(ElasticSearchConnectionParam connectionParam) {
        CloseableHttpClient httpClient = null;
        try {
            httpClient = createSimpleHttpClient();
            String url = String.format("%s://%s:%d/_cluster/health",
                    "http",
                    connectionParam.getHost(),
                    connectionParam.getPort());

            HttpGet request = new HttpGet(url);

            if (connectionParam.getUsername() != null && connectionParam.getPassword() != null) {
                String auth = connectionParam.getUsername() + ":" + connectionParam.getPassword();
                String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
                request.setHeader("Authorization", "Basic " + encodedAuth);
            }

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                return response.getStatusLine().getStatusCode() == 200;
            }
        } catch (Exception e) {
            log.error("HTTP connection test failed", e);
            return false;
        } finally {
            closeQuietly(httpClient);
        }
    }

    /**
     * Create RestClientBuilder using reflection with custom class loader
     */
    private Object createRestClientBuilder(ElasticSearchConnectionParam connectionParam, ClassLoader classLoader) throws Exception {
        Class<?> httpHostClass = Class.forName("org.apache.http.HttpHost", true, classLoader);
        Constructor<?> httpHostConstructor = httpHostClass.getConstructor(
                String.class, int.class, String.class);

        Object httpHost = httpHostConstructor.newInstance(
                connectionParam.getHost(),
                connectionParam.getPort(),
                "http"
        );

        Class<?> restClientBuilderClass = Class.forName("org.elasticsearch.client.RestClientBuilder", true, classLoader);
        Method builderMethod = restClientBuilderClass.getMethod("builder", httpHostClass);
        return builderMethod.invoke(null, httpHost);
    }

    private void closeClient(Object client) {
        if (client != null) {
            try {
                Method closeMethod = client.getClass().getMethod("close");
                closeMethod.invoke(client);
            } catch (Exception e) {
                log.warn("Failed to close client", e);
            }
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                log.warn("Failed to close resource", e);
            }
        }
    }

    private CloseableHttpClient createSimpleHttpClient() {
        return HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setConnectTimeout(5000)
                        .setSocketTimeout(10000)
                        .build())
                .build();
    }
}