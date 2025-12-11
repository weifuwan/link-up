package org.apache.cockpit.connectors.clickhouse.config;

import org.apache.cockpit.connectors.api.config.Option;
import org.apache.cockpit.connectors.api.config.Options;

import java.util.List;

public class ClickhouseSourceOptions {

    public static final int CLICKHOUSE_SPLIT_SIZE_MIN = 1;
    public static final int CLICKHOUSE_SPLIT_SIZE_DEFAULT = Integer.MAX_VALUE;
    public static final int CLICKHOUSE_BATCH_SIZE_DEFAULT = 1024;

    public static final Option<Integer> CLICKHOUSE_SPLIT_SIZE =
            Options.key("split.size")
                    .intType()
                    .defaultValue(CLICKHOUSE_SPLIT_SIZE_DEFAULT)
                    .withDescription("The number of parts in each splits");

    public static final Option<List<String>> CLICKHOUSE_PARTITION_LIST =
            Options.key("partition_list")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The partition used to filter data, if not set, the whole table will be queried");

    public static final Option<Integer> CLICKHOUSE_BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(CLICKHOUSE_BATCH_SIZE_DEFAULT)
                    .withDescription(
                            "The maximum rows of data that can be obtained by reading from Clickhouse once.");

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Clickhouse sql used to query data");

    public static final Option<String> CLICKHOUSE_FILTER_QUERY =
            Options.key("filter_query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Filter expression of the query. such as id > 2.");

    public static final Option<List<ClickhouseTableConfig>> TABLE_LIST =
            Options.key("table_list")
                    .listType(ClickhouseTableConfig.class)
                    .noDefaultValue()
                    .withDescription("table list config.");
}
