package org.apache.cockpit.connectors.influxdb.config;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;

@Data
public class InfluxDBConfig implements Serializable {

    private static final String DEFAULT_FORMAT = "MSGPACK";
    private String url;
    private String username;
    private String password;
    private String database;
    private String format = DEFAULT_FORMAT;
    private int queryTimeOut;
    private long connectTimeOut;
    private String epoch;

    public InfluxDBConfig(ReadonlyConfig config) {
        this.url = config.get(InfluxDBCommonOptions.URL);
        this.username = config.get(InfluxDBCommonOptions.USERNAME);
        this.password = config.get(InfluxDBCommonOptions.PASSWORD);
        this.database = config.get(InfluxDBCommonOptions.DATABASES);
        this.epoch = config.get(InfluxDBCommonOptions.EPOCH);
        this.connectTimeOut = config.get(InfluxDBCommonOptions.CONNECT_TIMEOUT_MS);
        this.queryTimeOut = config.get(InfluxDBCommonOptions.QUERY_TIMEOUT_SEC);
    }

    @VisibleForTesting
    public InfluxDBConfig(String url) {
        this.url = url;
    }
}
