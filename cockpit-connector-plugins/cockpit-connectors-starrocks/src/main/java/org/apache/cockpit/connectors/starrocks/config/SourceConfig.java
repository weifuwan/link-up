package org.apache.cockpit.connectors.starrocks.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class SourceConfig extends StarRocksConfig {

    public SourceConfig(ReadonlyConfig config) {
        super(config);
        this.maxRetries = config.get(StarRocksSourceOptions.MAX_RETRIES);
        this.requestTabletSize = config.get(StarRocksSourceOptions.QUERY_TABLET_SIZE);
        this.scanFilter = config.get(StarRocksSourceOptions.SCAN_FILTER);
        this.connectTimeoutMs = config.get(StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT);
        this.batchRows = config.get(StarRocksSourceOptions.SCAN_BATCH_ROWS);
        this.keepAliveMin = config.get(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        this.queryTimeoutSec = config.get(StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC);
        this.memLimit = config.get(StarRocksSourceOptions.SCAN_MEM_LIMIT);

        String prefix = StarRocksSourceOptions.STARROCKS_SCAN_CONFIG_PREFIX.key();
        config.toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(prefix)) {
                                this.sourceOptionProps.put(
                                        key.substring(prefix.length()).toLowerCase(), value);
                            }
                        });
        this.tableConfigList = StarRocksSourceTableConfig.of(config);
    }

    private int maxRetries = StarRocksSourceOptions.MAX_RETRIES.defaultValue();
    private int requestTabletSize = StarRocksSourceOptions.QUERY_TABLET_SIZE.defaultValue();
    private String scanFilter = StarRocksSourceOptions.SCAN_FILTER.defaultValue();
    private long memLimit = StarRocksSourceOptions.SCAN_MEM_LIMIT.defaultValue();
    private int queryTimeoutSec = StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC.defaultValue();
    private int keepAliveMin = StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN.defaultValue();
    private int batchRows = StarRocksSourceOptions.SCAN_BATCH_ROWS.defaultValue();
    private int connectTimeoutMs = StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT.defaultValue();
    private List<StarRocksSourceTableConfig> tableConfigList = new ArrayList<>();

    private Map<String, String> sourceOptionProps = new HashMap<>();
}
