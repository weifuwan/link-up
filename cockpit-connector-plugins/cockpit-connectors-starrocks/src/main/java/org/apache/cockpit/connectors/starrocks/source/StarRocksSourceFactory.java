package org.apache.cockpit.connectors.starrocks.source;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.config.ConnectorCommonOptions;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.starrocks.config.SourceConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksBaseOptions;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSourceOptions;

@AutoService(Factory.class)
public class StarRocksSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        StarRocksSourceOptions.NODE_URLS,
                        StarRocksSourceOptions.USERNAME,
                        StarRocksSourceOptions.PASSWORD,
                        StarRocksSourceOptions.DATABASE)
                .optional(
                        ConnectorCommonOptions.SCHEMA,
                        StarRocksSourceOptions.MAX_RETRIES,
                        StarRocksSourceOptions.QUERY_TABLET_SIZE,
                        StarRocksSourceOptions.SCAN_FILTER,
                        StarRocksSourceOptions.SCAN_MEM_LIMIT,
                        StarRocksSourceOptions.SCAN_QUERY_TIMEOUT_SEC,
                        StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN,
                        StarRocksSourceOptions.SCAN_BATCH_ROWS,
                        StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT)
                .exclusive(StarRocksSourceOptions.TABLE, StarRocksSourceOptions.TABLE_LIST)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return StarRocksSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        SourceConfig starRocksSourceConfig = new SourceConfig(config);
        return () ->
                (SeaTunnelSource<T, SplitT>) new StarRocksSource(starRocksSourceConfig);
    }
}
