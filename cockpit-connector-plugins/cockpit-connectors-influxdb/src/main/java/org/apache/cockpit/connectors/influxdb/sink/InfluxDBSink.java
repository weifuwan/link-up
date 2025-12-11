package org.apache.cockpit.connectors.influxdb.sink;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.common.sink.AbstractSimpleSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.influxdb.config.SinkConfig;

import java.io.IOException;
import java.util.Optional;

public class InfluxDBSink extends AbstractSimpleSink<SeaTunnelRow> {

    private final SeaTunnelRowType seaTunnelRowType;
    private final SinkConfig sinkConfig;
    private final CatalogTable catalogTable;

    @Override
    public String getPluginName() {
        return "InfluxDB";
    }

    public InfluxDBSink(SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        this.catalogTable = catalogTable;
    }

    @Override
    public InfluxDBSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new InfluxDBSinkWriter(sinkConfig, seaTunnelRowType);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
