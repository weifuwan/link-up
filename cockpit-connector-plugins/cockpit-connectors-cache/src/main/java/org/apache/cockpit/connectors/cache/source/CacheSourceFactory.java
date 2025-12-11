package org.apache.cockpit.connectors.cache.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceConfig;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectLoader;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;

import static org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions.*;
import static org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceOptions.*;


@Slf4j
@AutoService(Factory.class)
public class CacheSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DbType.CACHE.getCode();
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        JdbcSourceConfig config = JdbcSourceConfig.of(context.getOptions());
        JdbcDialect jdbcDialect =
                JdbcDialectLoader.load(
                        config.getJdbcConnectionConfig().getJdbcUrl(),
                        config.getJdbcConnectionConfig().getDialect(),
                        config.getJdbcConnectionConfig().getCompatibleMode(),
                        config.getJdbcConnectionConfig());
        jdbcDialect.connectionUrlParse(
                config.getJdbcConnectionConfig().getJdbcUrl(),
                config.getJdbcConnectionConfig().getProperties(),
                jdbcDialect.defaultParameter());
        return () -> (SeaTunnelSource<T, SplitT>) new CacheSource(config);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(JDBC_URL, DRIVER)
                .optional(
                        USERNAME,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        FETCH_SIZE,
                        PARTITION_COLUMN,
                        PARTITION_UPPER_BOUND,
                        PARTITION_LOWER_BOUND,
                        PARTITION_NUM,
                        COMPATIBLE_MODE,
                        PROPERTIES,
                        QUERY,
                        USE_SELECT_COUNT,
                        SKIP_ANALYZE,
                        TABLE_PATH,
                        WHERE_CONDITION,
                        TABLE_LIST,
                        SPLIT_SIZE,
                        SPLIT_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        SPLIT_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        SPLIT_SAMPLE_SHARDING_THRESHOLD,
                        SPLIT_INVERSE_SAMPLING_RATE,
                        DIALECT)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CacheSource.class;
    }
}
