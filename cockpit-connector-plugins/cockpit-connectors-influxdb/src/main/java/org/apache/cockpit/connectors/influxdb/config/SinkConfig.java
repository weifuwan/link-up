package org.apache.cockpit.connectors.influxdb.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.util.List;

@Setter
@Getter
@ToString
public class SinkConfig extends InfluxDBConfig {

    public SinkConfig(ReadonlyConfig config) {
        super(config);
        loadConfig(config);
    }

    private static final TimePrecision DEFAULT_TIME_PRECISION = TimePrecision.NS;

    private String rp;
    private String measurement;
    private int writeTimeout;
    private String keyTime;
    private List<String> keyTags;
    private int batchSize;
    private int maxRetries;
    private int retryBackoffMultiplierMs;
    private int maxRetryBackoffMs;
    private TimePrecision precision = DEFAULT_TIME_PRECISION;

    public void loadConfig(ReadonlyConfig config) {
        setKeyTime(config.get(InfluxDBSinkOptions.KEY_TIME));
        setKeyTags(config.get(InfluxDBSinkOptions.KEY_TAGS));
        setBatchSize(config.get(InfluxDBSinkOptions.BATCH_SIZE));
        if (config.getOptional(InfluxDBSinkOptions.MAX_RETRIES).isPresent()) {
            setMaxRetries(config.get(InfluxDBSinkOptions.MAX_RETRIES));
        }
        if (config.getOptional(InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS).isPresent()) {
            setRetryBackoffMultiplierMs(
                    config.get(InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS));
        }
        if (config.getOptional(InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS).isPresent()) {
            setMaxRetryBackoffMs(config.get(InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS));
        }
        setWriteTimeout(config.get(InfluxDBSinkOptions.WRITE_TIMEOUT));
        setRp(config.get(InfluxDBSinkOptions.RETENTION_POLICY));
        setPrecision(TimePrecision.getPrecision(config.get(InfluxDBSinkOptions.EPOCH)));
        setMeasurement(config.get(InfluxDBSinkOptions.KEY_MEASUREMENT));
    }
}
