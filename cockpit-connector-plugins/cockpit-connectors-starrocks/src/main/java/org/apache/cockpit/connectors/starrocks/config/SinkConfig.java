package org.apache.cockpit.connectors.starrocks.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
@ToString
public class SinkConfig implements Serializable {

    public enum StreamLoadFormat {
        CSV,
        JSON;
    }

    private List<String> nodeUrls;
    private String jdbcUrl;
    private String username;
    private String password;
    private String database;
    private String table;
    private String labelPrefix;
    private String columnSeparator;
    private StreamLoadFormat loadFormat;
    private int batchMaxSize;
    private long batchMaxBytes;

    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private boolean enableUpsertDelete;

    private String saveModeCreateTemplate;

    private SchemaSaveMode schemaSaveMode;
    private DataSaveMode dataSaveMode;
    private String customSql;

    private int httpSocketTimeout;

    @Getter private final Map<String, Object> streamLoadProps = new HashMap<>();

    public static SinkConfig of(ReadonlyConfig config) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setNodeUrls(config.get(StarRocksSinkOptions.NODE_URLS));
        sinkConfig.setDatabase(config.get(StarRocksSinkOptions.DATABASE));
        sinkConfig.setJdbcUrl(config.get(StarRocksSinkOptions.BASE_URL));
        config.getOptional(StarRocksSinkOptions.USERNAME).ifPresent(sinkConfig::setUsername);
        config.getOptional(StarRocksSinkOptions.PASSWORD).ifPresent(sinkConfig::setPassword);
        config.getOptional(StarRocksSinkOptions.TABLE).ifPresent(sinkConfig::setTable);
        config.getOptional(StarRocksSinkOptions.LABEL_PREFIX).ifPresent(sinkConfig::setLabelPrefix);
        sinkConfig.setBatchMaxSize(config.get(StarRocksSinkOptions.BATCH_MAX_SIZE));
        sinkConfig.setBatchMaxBytes(config.get(StarRocksSinkOptions.BATCH_MAX_BYTES));
        config.getOptional(StarRocksSinkOptions.MAX_RETRIES).ifPresent(sinkConfig::setMaxRetries);
        config.getOptional(StarRocksSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS)
                .ifPresent(sinkConfig::setRetryBackoffMultiplierMs);
        config.getOptional(StarRocksSinkOptions.MAX_RETRY_BACKOFF_MS)
                .ifPresent(sinkConfig::setMaxRetryBackoffMs);
        config.getOptional(StarRocksSinkOptions.ENABLE_UPSERT_DELETE)
                .ifPresent(sinkConfig::setEnableUpsertDelete);
        sinkConfig.setSaveModeCreateTemplate(
                config.get(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
        config.getOptional(StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE)
                .ifPresent(sinkConfig::setSaveModeCreateTemplate);
        config.getOptional(StarRocksSinkOptions.STARROCKS_CONFIG)
                .ifPresent(options -> sinkConfig.getStreamLoadProps().putAll(options));
        config.getOptional(StarRocksSinkOptions.COLUMN_SEPARATOR)
                .ifPresent(sinkConfig::setColumnSeparator);
        sinkConfig.setLoadFormat(config.get(StarRocksSinkOptions.LOAD_FORMAT));
        sinkConfig.setSchemaSaveMode(config.get(StarRocksSinkOptions.SCHEMA_SAVE_MODE));
        sinkConfig.setDataSaveMode(config.get(StarRocksSinkOptions.DATA_SAVE_MODE));
        sinkConfig.setCustomSql(config.get(StarRocksSinkOptions.CUSTOM_SQL));
        sinkConfig.setHttpSocketTimeout(config.get(StarRocksSinkOptions.HTTP_SOCKET_TIMEOUT_MS));
        return sinkConfig;
    }
}
