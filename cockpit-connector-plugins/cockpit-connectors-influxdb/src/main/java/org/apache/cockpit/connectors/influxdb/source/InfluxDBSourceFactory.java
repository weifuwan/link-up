package org.apache.cockpit.connectors.influxdb.source;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.CatalogTableUtil;
import org.apache.cockpit.connectors.api.config.ConnectorCommonOptions;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.influxdb.config.InfluxDBSourceOptions;
import org.apache.cockpit.connectors.influxdb.config.SourceConfig;

import java.io.Serializable;

@AutoService(Factory.class)
public class InfluxDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        InfluxDBSourceOptions.URL,
                        InfluxDBSourceOptions.SQL,
                        InfluxDBSourceOptions.DATABASES,
                        ConnectorCommonOptions.SCHEMA)
                .bundled(InfluxDBSourceOptions.USERNAME, InfluxDBSourceOptions.PASSWORD)
                .bundled(
                        InfluxDBSourceOptions.LOWER_BOUND,
                        InfluxDBSourceOptions.UPPER_BOUND,
                        InfluxDBSourceOptions.PARTITION_NUM,
                        InfluxDBSourceOptions.SPLIT_COLUMN)
                .optional(
                        InfluxDBSourceOptions.EPOCH,
                        InfluxDBSourceOptions.SQL_WHERE,
                        InfluxDBSourceOptions.CONNECT_TIMEOUT_MS,
                        InfluxDBSourceOptions.QUERY_TIMEOUT_SEC)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT>)
                        new InfluxDBSource(
                                CatalogTableUtil.buildWithConfig(context.getOptions()),
                                SourceConfig.loadConfig(context.getOptions()));
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InfluxDBSource.class;
    }
}
