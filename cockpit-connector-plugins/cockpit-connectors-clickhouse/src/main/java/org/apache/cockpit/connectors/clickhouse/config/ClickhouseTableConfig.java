package org.apache.cockpit.connectors.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Tolerate;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.constant.PluginType;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceOptions.TABLE_PATH;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSourceOptions.*;


@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClickhouseTableConfig implements Serializable {
    private static final long serialVersionUID = -6133096497433624821L;

    @JsonProperty("table_path")
    private String tablePath;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("filter_query")
    private String filterQuery;

    @JsonProperty("partition_list")
    private List<String> partitionList;

    @JsonProperty("batch_size")
    private int batchSize;

    @JsonProperty("split_size")
    private int splitSize;

    private boolean isSqlStrategyRead;

    @Tolerate
    public ClickhouseTableConfig() {}

    public static List<ClickhouseTableConfig> of(ReadonlyConfig readonlyConfig) {
        List<ClickhouseTableConfig> tableList;
        if (readonlyConfig.getOptional(TABLE_LIST).isPresent()) {
            tableList = readonlyConfig.get(TABLE_LIST);
        } else {
            ClickhouseTableConfig tableConfig =
                    ClickhouseTableConfig.builder()
                            .tablePath(readonlyConfig.get(TABLE_PATH))
                            .sql(readonlyConfig.get(SQL))
                            .filterQuery(readonlyConfig.get(CLICKHOUSE_FILTER_QUERY))
                            .partitionList(readonlyConfig.get(CLICKHOUSE_PARTITION_LIST))
                            .batchSize(readonlyConfig.get(CLICKHOUSE_BATCH_SIZE))
                            .splitSize(readonlyConfig.get(CLICKHOUSE_SPLIT_SIZE))
                            .build();

            tableList = Collections.singletonList(tableConfig);
        }

        if (tableList == null || tableList.isEmpty()) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.GET_TABLE_LIST_CONFIG_ERROR,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "Clickhouse", PluginType.SOURCE, "Get table list config error."));
        }

        for (ClickhouseTableConfig tableConfig : tableList) {
            if (StringUtils.isEmpty(tableConfig.getTablePath())
                    && StringUtils.isEmpty(tableConfig.getSql())) {
                throw new IllegalArgumentException(
                        "`table_path` and `sql` parameter cannot be both empty.");
            }

            if (tableConfig.getBatchSize() <= 0) {
                tableConfig.setBatchSize(CLICKHOUSE_BATCH_SIZE.defaultValue());
            }

            if (tableConfig.getSplitSize() <= 0) {
                tableConfig.setSplitSize(CLICKHOUSE_SPLIT_SIZE.defaultValue());
            }

            tableConfig.setSqlStrategyRead(StringUtils.isNotEmpty(tableConfig.getSql()));
        }

        return tableList;
    }

    public TablePath getTableIdentifier() {
        if (StringUtils.isEmpty(tablePath)) {
            // Extract table identifier from SQL
            return ClickhouseUtil.extractTablePathFromSql(sql);
        }

        return TablePath.of(tablePath);
    }
}
