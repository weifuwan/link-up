package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.plugin.datasource.elasticsearch.enums.ConnectionStrategy;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchConnectionParam;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;

@Slf4j
public class ElasticsearchVersionManager {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchVersionManager.class);

    // Version range definitions
    private static final String VERSION_6 = "6.";
    private static final String VERSION_7 = "7.";
    private static final String VERSION_8 = "8.";

    /**
     * Detect ES cluster version
     */
    public String detectVersion(ElasticSearchConnectionParam connectionParam) {
        CloseableHttpClient httpClient = null;
        try {
            httpClient = createSimpleHttpClient();
            String url = String.format("%s://%s:%d",
                    "http",
                    "192.168.1.111",
                    9200);

            HttpGet request = new HttpGet(url);

            // Set authentication header
            if (connectionParam.getUsername() != null && connectionParam.getPassword() != null) {
                String auth = connectionParam.getUsername() + ":" + connectionParam.getPassword();
                String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
                request.setHeader("Authorization", "Basic " + encodedAuth);
            }

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    String json = EntityUtils.toString(response.getEntity());
                    return parseVersionFromJson(json);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to detect Elasticsearch version", e);
        } finally {
            closeQuietly(httpClient);
        }
        return null;
    }

    private String parseVersionFromJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);
            JsonNode versionNode = root.path("version").path("number");
            return versionNode.asText();
        } catch (Exception e) {
            logger.error("Failed to parse version information", e);
            return null;
        }
    }

    /**
     * Get connection strategy based on version
     */
    public ConnectionStrategy getConnectionStrategy(String version) {
        if (version == null) {
            return ConnectionStrategy.HTTP; // Default to HTTP
        }

        if (version.startsWith(VERSION_6)) {
            return ConnectionStrategy.REST_HIGH_LEVEL_CLIENT;
        } else if (version.startsWith(VERSION_7)) {
            return ConnectionStrategy.REST_HIGH_LEVEL_CLIENT;
        } else if (version.startsWith(VERSION_8)) {
            return ConnectionStrategy.NEW_JAVA_CLIENT;
        } else {
            return ConnectionStrategy.HTTP; // Use HTTP for unknown versions
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