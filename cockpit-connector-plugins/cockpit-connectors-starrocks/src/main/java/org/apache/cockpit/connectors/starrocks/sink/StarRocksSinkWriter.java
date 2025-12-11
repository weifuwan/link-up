package org.apache.cockpit.connectors.starrocks.sink;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.common.sink.AbstractSinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.starrocks.client.StarRocksSinkManager;
import org.apache.cockpit.connectors.starrocks.config.SinkConfig;
import org.apache.cockpit.connectors.starrocks.config.StarRocksBaseOptions;
import org.apache.cockpit.connectors.starrocks.serialize.StarRocksCsvSerializer;
import org.apache.cockpit.connectors.starrocks.serialize.StarRocksISerializer;
import org.apache.cockpit.connectors.starrocks.serialize.StarRocksJsonSerializer;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class StarRocksSinkWriter extends AbstractSinkWriter<SeaTunnelRow> {
    private StarRocksISerializer serializer;
    private StarRocksSinkManager manager;
    private TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final TablePath sinkTablePath;

    public StarRocksSinkWriter(
            SinkConfig sinkConfig, TableSchema tableSchema, TablePath tablePath) {
        this.tableSchema = tableSchema;
        SeaTunnelRowType seaTunnelRowType = tableSchema.toPhysicalRowDataType();
        this.serializer = createSerializer(sinkConfig, seaTunnelRowType);
        this.manager = new StarRocksSinkManager(sinkConfig, tableSchema);
        this.sinkConfig = sinkConfig;
        this.sinkTablePath = tablePath;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record;
        try {
            record = serializer.serialize(element);
        } catch (Exception e) {
            throw CommonError.seatunnelRowSerializeFailed(element.toString(), e);
        }
        manager.write(record);
    }


    @SneakyThrows
    public Optional prepareCommit() {
        // Flush to storage before snapshot state is performed
        manager.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            log.error("Close starRocks manager failed.", e);
            throw CommonError.closeFailed(StarRocksBaseOptions.CONNECTOR_IDENTITY, e);
        }
    }

    public StarRocksISerializer createSerializer(
            SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksCsvSerializer(
                    sinkConfig.getColumnSeparator(),
                    seaTunnelRowType,
                    sinkConfig.isEnableUpsertDelete());
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new StarRocksJsonSerializer(seaTunnelRowType, sinkConfig.isEnableUpsertDelete());
        }
        throw CommonError.illegalArgument(
                sinkConfig.getLoadFormat().name(), "starrocks stream load");
    }
}
