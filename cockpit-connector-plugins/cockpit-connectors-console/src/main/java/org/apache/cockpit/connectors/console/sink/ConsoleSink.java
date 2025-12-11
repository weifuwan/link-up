package org.apache.cockpit.connectors.console.sink;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.common.sink.AbstractSimpleSink;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;

import java.util.Optional;

public class ConsoleSink extends AbstractSimpleSink<SeaTunnelRow> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final boolean isPrintData;
    private final int delayMs;
    private final CatalogTable catalogTable;

    public ConsoleSink(CatalogTable catalogTable, ReadonlyConfig options) {
        this.catalogTable = catalogTable;
        this.isPrintData = options.get(ConsoleSinkOptions.LOG_PRINT_DATA);
        this.delayMs = options.get(ConsoleSinkOptions.LOG_PRINT_DELAY);
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public ConsoleSinkWriter createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowType, context, isPrintData, delayMs);
    }

    @Override
    public String getPluginName() {
        return "CONSOLE";
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

}
