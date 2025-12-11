package org.apache.cockpit.connectors.clickhouse.source;

import com.clickhouse.client.ClickHouseNode;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseSourceConfig;
import org.apache.cockpit.connectors.clickhouse.source.split.ClickhouseSourceSplit;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickhouseSource
        implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit> {

    private final Map<TablePath, List<ClickHouseNode>> servers;
    private final ClickhouseSourceConfig clickhouseSourceConfig;
    private final Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables;

    public ClickhouseSource(
            Map<TablePath, List<ClickHouseNode>> servers,
            Map<TablePath, ClickhouseSourceTable> clickhouseSourceTables,
            ClickhouseSourceConfig clickhouseSourceConfig) {
        this.servers = servers;
        this.clickhouseSourceTables = clickhouseSourceTables;
        this.clickhouseSourceConfig = clickhouseSourceConfig;
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {

        return clickhouseSourceTables.values().stream()
                .map(ClickhouseSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ClickhouseSourceReader(servers, readerContext, clickhouseSourceTables);
    }

}
