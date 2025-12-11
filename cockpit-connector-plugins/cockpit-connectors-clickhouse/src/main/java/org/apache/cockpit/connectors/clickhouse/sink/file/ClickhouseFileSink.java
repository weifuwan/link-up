package org.apache.cockpit.connectors.clickhouse.sink.file;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.serialization.DefaultSerializer;
import org.apache.cockpit.connectors.api.serialization.Serializer;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.clickhouse.config.FileReaderOption;
import org.apache.cockpit.connectors.clickhouse.state.CKFileCommitInfo;

import java.io.IOException;
import java.util.Optional;

public class ClickhouseFileSink
        implements SeaTunnelSink<
        SeaTunnelRow> {

    private FileReaderOption readerOption;

    public ClickhouseFileSink(FileReaderOption readerOption) {
        this.readerOption = readerOption;
    }

    @Override
    public String getPluginName() {
        return "ClickhouseFile";
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(
            SinkWriter.Context context) throws IOException {
        return new ClickhouseFileSinkWriter(readerOption, context);
    }

    public Optional<Serializer<CKFileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }


    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return SeaTunnelSink.super.getWriteCatalogTable();
    }
}
