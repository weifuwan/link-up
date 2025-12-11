package org.apache.cockpit.connectors.influxdb.sink;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.common.sink.AbstractSinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.JsonUtils;
import org.apache.cockpit.connectors.influxdb.client.InfluxDBClient;
import org.apache.cockpit.connectors.influxdb.config.SinkConfig;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorException;
import org.apache.cockpit.connectors.influxdb.serialize.DefaultSerializer;
import org.apache.cockpit.connectors.influxdb.serialize.Serializer;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class InfluxDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow> {

    private final Serializer serializer;
    private InfluxDB influxdb;
    private final SinkConfig sinkConfig;
    private final List<Point> batchList;
    private volatile Exception flushException;

    public InfluxDBSinkWriter(SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType)
            throws ConnectException {
        this.sinkConfig = sinkConfig;
        log.info("sinkConfig is {}", JsonUtils.toJsonString(sinkConfig));
        this.serializer =
                new DefaultSerializer(
                        seaTunnelRowType,
                        sinkConfig.getPrecision().getTimeUnit(),
                        sinkConfig.getKeyTags(),
                        sinkConfig.getKeyTime(),
                        sinkConfig.getMeasurement());
        this.batchList = new ArrayList<>();

        connect();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Point record = serializer.serialize(element);
        write(record);
    }

    @SneakyThrows
//    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        flush();
//        return super.prepareCommit();
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        flush();

        if (influxdb != null) {
            influxdb.close();
            influxdb = null;
        }
    }

    public void write(Point record) throws IOException {
        checkFlushException();

        batchList.add(record);
        if (sinkConfig.getBatchSize() > 0 && batchList.size() >= sinkConfig.getBatchSize()) {
            flush();
        }
    }

    public void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        BatchPoints.Builder batchPoints = BatchPoints.database(sinkConfig.getDatabase());
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                batchPoints.points(batchList);
                influxdb.write(batchPoints.build());
            } catch (Exception e) {
                log.error("Writing records to influxdb failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new InfluxdbConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "Writing records to InfluxDB failed.",
                            e);
                }

                try {
                    long backoff =
                            Math.min(
                                    sinkConfig.getRetryBackoffMultiplierMs() * i,
                                    sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new InfluxdbConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "Unable to flush; interrupted while doing another attempt.",
                            e);
                }
            }
        }

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new InfluxdbConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to InfluxDB failed.",
                    flushException);
        }
    }

    public void connect() throws ConnectException {
        if (influxdb == null) {
            influxdb = InfluxDBClient.getWriteClient(sinkConfig);
            String version = influxdb.version();
            if (!influxdb.ping().isGood()) {
                throw new InfluxdbConnectorException(
                        InfluxdbConnectorErrorCode.CONNECT_FAILED,
                        String.format(
                                "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                                sinkConfig.getUrl()));
            }
            log.info("connect influxdb successful. sever version :{}.", version);
        }
    }
}
