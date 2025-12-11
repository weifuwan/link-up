package org.apache.cockpit.connectors.starrocks.source;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.starrocks.config.SourceConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksBaseOptions;
import org.apache.cockpit.connectors.starrocks.config.StarRocksSourceTableConfig;

import java.util.List;
import java.util.stream.Collectors;

public class StarRocksSource
        implements SeaTunnelSource<SeaTunnelRow, StarRocksSourceSplit> {

    private SourceConfig sourceConfig;

    @Override
    public String getPluginName() {
        return StarRocksBaseOptions.CONNECTOR_IDENTITY;
    }

    public StarRocksSource(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getTableConfigList().stream()
                .map(StarRocksSourceTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) {
        return new StarRocksSourceReader(readerContext, sourceConfig);
    }

}
