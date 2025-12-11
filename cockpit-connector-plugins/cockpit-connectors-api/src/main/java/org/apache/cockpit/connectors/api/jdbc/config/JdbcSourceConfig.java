package org.apache.cockpit.connectors.api.jdbc.config;

import lombok.Builder;
import lombok.Data;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.source.StringSplitMode;

import java.io.Serializable;
import java.util.List;

@Data
@Builder(builderClassName = "Builder")
public class JdbcSourceConfig implements Serializable {
    private static final long serialVersionUID = 2L;

    private JdbcConnectionConfig jdbcConnectionConfig;
    private List<JdbcSourceTableConfig> tableConfigList;
    private String whereConditionClause;
    public String compatibleMode;
    private int fetchSize;

    private boolean useDynamicSplitter;
    private int splitSize;
    private double splitEvenDistributionFactorUpperBound;
    private double splitEvenDistributionFactorLowerBound;
    private int splitSampleShardingThreshold;
    private int splitInverseSamplingRate;
    private boolean decimalTypeNarrowing;
    private boolean handleBlobAsString;

    private StringSplitMode stringSplitMode;

    private String stringSplitModeCollate;

    public static JdbcSourceConfig of(ReadonlyConfig config) {
        Builder builder = JdbcSourceConfig.builder();
        builder.jdbcConnectionConfig(JdbcConnectionConfig.of(config));
        builder.tableConfigList(JdbcSourceTableConfig.of(config));
        builder.fetchSize(config.get(JdbcOptions.FETCH_SIZE));
        config.getOptional(JdbcOptions.COMPATIBLE_MODE).ifPresent(builder::compatibleMode);

        boolean isOldVersion =
                config.getOptional(JdbcOptions.QUERY).isPresent()
                        && config.getOptional(JdbcOptions.PARTITION_COLUMN).isPresent();
        builder.useDynamicSplitter(!isOldVersion);
        builder.stringSplitMode(config.get(JdbcOptions.STRING_SPLIT_MODE));
        builder.stringSplitModeCollate(config.get(JdbcOptions.STRING_SPLIT_MODE_COLLATE));
        builder.splitSize(config.get(JdbcSourceOptions.SPLIT_SIZE));
        builder.splitEvenDistributionFactorUpperBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND));
        builder.splitEvenDistributionFactorLowerBound(
                config.get(JdbcSourceOptions.SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND));
        builder.splitSampleShardingThreshold(
                config.get(JdbcSourceOptions.SPLIT_SAMPLE_SHARDING_THRESHOLD));
        builder.splitInverseSamplingRate(config.get(JdbcSourceOptions.SPLIT_INVERSE_SAMPLING_RATE));

        builder.decimalTypeNarrowing(config.get(JdbcOptions.DECIMAL_TYPE_NARROWING));
        builder.handleBlobAsString(config.get(JdbcOptions.HANDLE_BLOB_AS_STRING));

        config.getOptional(JdbcSourceOptions.WHERE_CONDITION)
                .ifPresent(
                        whereConditionClause -> {
                            if (!whereConditionClause.toLowerCase().startsWith("where")) {
                                throw new IllegalArgumentException(
                                        "The where condition clause must start with 'where'. value: "
                                                + whereConditionClause);
                            }
                            builder.whereConditionClause(whereConditionClause);
                        });

        return builder.build();
    }
}
