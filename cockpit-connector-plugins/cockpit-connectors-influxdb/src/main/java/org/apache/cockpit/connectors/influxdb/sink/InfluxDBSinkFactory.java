package org.apache.cockpit.connectors.influxdb.sink;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.api.jdbc.sink.SinkConnectorCommonOptions;
import org.apache.cockpit.connectors.influxdb.config.InfluxDBSinkOptions;
import org.apache.cockpit.connectors.influxdb.config.SinkConfig;

import java.util.HashMap;
import java.util.Map;

@AutoService(Factory.class)
@Slf4j
public class InfluxDBSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(InfluxDBSinkOptions.URL, InfluxDBSinkOptions.DATABASES)
                .bundled(InfluxDBSinkOptions.USERNAME, InfluxDBSinkOptions.PASSWORD)
                .optional(
                        InfluxDBSinkOptions.CONNECT_TIMEOUT_MS,
                        InfluxDBSinkOptions.KEY_MEASUREMENT,
                        InfluxDBSinkOptions.KEY_TAGS,
                        InfluxDBSinkOptions.KEY_TIME,
                        InfluxDBSinkOptions.BATCH_SIZE,
                        InfluxDBSinkOptions.MAX_RETRIES,
                        InfluxDBSinkOptions.WRITE_TIMEOUT,
                        InfluxDBSinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        InfluxDBSinkOptions.MAX_RETRY_BACKOFF_MS,
                        InfluxDBSinkOptions.RETENTION_POLICY,
                        InfluxDBSinkOptions.QUERY_TIMEOUT_SEC,
                        InfluxDBSinkOptions.EPOCH,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        if (!config.getOptional(InfluxDBSinkOptions.KEY_MEASUREMENT).isPresent()) {
            Map<String, String> map = config.toMap();
            map.put(
                    InfluxDBSinkOptions.KEY_MEASUREMENT.key(),
                    catalogTable.getTableId().toTablePath().getFullName());
            config = ReadonlyConfig.fromMap(new HashMap<>(map));
        }
        SinkConfig sinkConfig = new SinkConfig(config);
        return () -> new InfluxDBSink(sinkConfig, catalogTable);
    }
}
