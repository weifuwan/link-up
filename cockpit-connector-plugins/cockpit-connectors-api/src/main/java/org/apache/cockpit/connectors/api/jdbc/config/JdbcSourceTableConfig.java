package org.apache.cockpit.connectors.api.jdbc.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class JdbcSourceTableConfig implements Serializable {
    private static final int DEFAULT_PARTITION_NUMBER = 10;

    @JsonProperty("table_path")
    private String tablePath;

    @JsonProperty("query")
    private String query;

    @JsonProperty("partition_column")
    private String partitionColumn;

    @JsonProperty("partition_num")
    private Integer partitionNumber;

    @JsonProperty("partition_lower_bound")
    private String partitionStart;

    @JsonProperty("partition_upper_bound")
    private String partitionEnd;

    @JsonProperty("use_select_count")
    private Boolean useSelectCount;

    @JsonProperty("skip_analyze")
    private Boolean skipAnalyze;

    @Tolerate
    public JdbcSourceTableConfig() {}

    public static List<JdbcSourceTableConfig> of(ReadonlyConfig connectorConfig) {
        List<JdbcSourceTableConfig> tableList;
        if (connectorConfig.getOptional(JdbcSourceOptions.TABLE_LIST).isPresent()) {
            if (connectorConfig.getOptional(JdbcOptions.QUERY).isPresent()
                    || connectorConfig.getOptional(JdbcSourceOptions.TABLE_PATH).isPresent()) {
                throw new IllegalArgumentException(
                        "Please configure either `table_list` or `table_path`/`query`, not both");
            }
            tableList = connectorConfig.get(JdbcSourceOptions.TABLE_LIST);
        } else {
            JdbcSourceTableConfig tableProperty =
                    JdbcSourceTableConfig.builder()
                            .tablePath(connectorConfig.get(JdbcSourceOptions.TABLE_PATH))
                            .query(connectorConfig.get(JdbcOptions.QUERY))
                            .partitionColumn(connectorConfig.get(JdbcOptions.PARTITION_COLUMN))
                            .partitionNumber(connectorConfig.get(JdbcOptions.PARTITION_NUM))
                            .partitionStart(connectorConfig.get(JdbcOptions.PARTITION_LOWER_BOUND))
                            .partitionEnd(connectorConfig.get(JdbcOptions.PARTITION_UPPER_BOUND))
                            .build();
            tableList = Collections.singletonList(tableProperty);
        }

        tableList.forEach(
                tableConfig -> {
                    if (tableConfig.getPartitionNumber() == null) {
                        tableConfig.setPartitionNumber(DEFAULT_PARTITION_NUMBER);
                    }
                    tableConfig.setUseSelectCount(
                            connectorConfig.get(JdbcSourceOptions.USE_SELECT_COUNT));
                    tableConfig.setSkipAnalyze(connectorConfig.get(JdbcSourceOptions.SKIP_ANALYZE));
                });

        if (tableList.size() > 1) {
            List<String> tableIds =
                    tableList.stream()
                            .map(JdbcSourceTableConfig::getTablePath)
                            .collect(Collectors.toList());
            Set<String> tableIdSet = new HashSet<>(tableIds);
            if (tableIdSet.size() < tableList.size() - 1) {
                throw new IllegalArgumentException(
                        "Please configure unique `table_path`, not allow null/duplicate table path: "
                                + tableIds);
            }
        }
        return tableList;
    }
}
