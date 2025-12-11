package org.apache.cockpit.connectors.doris.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.doris.config.DorisSourceConfig;
import org.apache.cockpit.connectors.doris.source.reader.DorisSourceReader;
import org.apache.cockpit.connectors.doris.source.split.DorisSourceSplit;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DorisSource
        implements SeaTunnelSource<SeaTunnelRow, DorisSourceSplit> {

    private final DorisSourceConfig config;
    private final Map<TablePath, DorisSourceTable> dorisSourceTables;

    public DorisSource(
            DorisSourceConfig config, Map<TablePath, DorisSourceTable> dorisSourceTables) {
        this.config = config;
        this.dorisSourceTables = dorisSourceTables;
    }

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return dorisSourceTables.values().stream()
                .map(DorisSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, DorisSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new DorisSourceReader(readerContext, config, dorisSourceTables);
    }


}
