package org.apache.cockpit.connectors.starrocks.config;


import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;
import java.util.Map;

public class StarRocksSourceOptions extends StarRocksBaseOptions {
    private static final long DEFAULT_SCAN_MEM_LIMIT = 1024 * 1024 * 1024L;

    public static final Option<Integer> QUERY_TABLET_SIZE =
            Options.key("request_tablet_size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The number of Tablets corresponding to an Partition");

    public static final Option<String> SCAN_FILTER =
            Options.key("scan_filter").stringType().defaultValue("").withDescription("SQL filter");

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("number of retry requests sent to StarRocks");
    public static final Option<Integer> SCAN_CONNECT_TIMEOUT =
            Options.key("scan_connect_timeout_ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("scan connect timeout");

    public static final Option<Integer> SCAN_BATCH_ROWS =
            Options.key("scan_batch_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("scan batch rows");

    public static final Option<Integer> SCAN_KEEP_ALIVE_MIN =
            Options.key("scan_keep_alive_min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Max keep alive time min");

    public static final Option<Integer> SCAN_QUERY_TIMEOUT_SEC =
            Options.key("scan_query_timeout_sec")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("Query timeout for a single query");

    public static final Option<Long> SCAN_MEM_LIMIT =
            Options.key("scan_mem_limit")
                    .longType()
                    .defaultValue(DEFAULT_SCAN_MEM_LIMIT)
                    .withDescription("Memory byte limit for a single query");

    public static final Option<String> STARROCKS_SCAN_CONFIG_PREFIX =
            Options.key("scan.params.")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The parameter of the scan data from be");

    public static final Option<List<Map<String, Object>>> TABLE_LIST =
            Options.key("table_list")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("table list config");
}
