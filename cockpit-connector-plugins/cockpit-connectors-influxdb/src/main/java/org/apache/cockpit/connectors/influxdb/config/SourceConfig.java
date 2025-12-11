package org.apache.cockpit.connectors.influxdb.config;

import lombok.Getter;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.util.List;

@Getter
public class SourceConfig extends InfluxDBConfig {

    public static final int DEFAULT_PARTITIONS = InfluxDBSourceOptions.PARTITION_NUM.defaultValue();
    private String sql;
    private int partitionNum = 0;
    private String splitKey;
    private long lowerBound;
    private long upperBound;

    List<Integer> columnsIndex;

    public SourceConfig(ReadonlyConfig config) {
        super(config);
    }

    public static SourceConfig loadConfig(ReadonlyConfig config) {
        SourceConfig sourceConfig = new SourceConfig(config);
        sourceConfig.sql = config.get(InfluxDBSourceOptions.SQL);
        sourceConfig.partitionNum = config.get(InfluxDBSourceOptions.PARTITION_NUM);
        if (config.getOptional(InfluxDBSourceOptions.UPPER_BOUND).isPresent()) {
            sourceConfig.upperBound = config.get(InfluxDBSourceOptions.UPPER_BOUND);
        }
        if (config.getOptional(InfluxDBSourceOptions.LOWER_BOUND).isPresent()) {
            sourceConfig.lowerBound = config.get(InfluxDBSourceOptions.LOWER_BOUND);
        }
        if (config.getOptional(InfluxDBSourceOptions.SPLIT_COLUMN).isPresent()) {
            sourceConfig.splitKey = config.get(InfluxDBSourceOptions.SPLIT_COLUMN);
        }
        return sourceConfig;
    }
}
