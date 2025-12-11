package org.apache.cockpit.connectors.influxdb.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.influxdb.client.InfluxDBClient;
import org.apache.cockpit.connectors.influxdb.config.InfluxDBConfig;
import org.apache.cockpit.connectors.influxdb.converter.InfluxDBRowConverter;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.cockpit.connectors.influxdb.exception.InfluxdbConnectorException;
import org.apache.commons.collections.CollectionUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.net.ConnectException;
import java.util.*;

@Slf4j
public class InfluxdbSourceReader implements SourceReader<SeaTunnelRow, InfluxDBSourceSplit> {
    private InfluxDB influxdb;
    InfluxDBConfig config;

    private final SourceReader.Context context;

    private final SeaTunnelRowType seaTunnelRowType;

    List<Integer> columnsIndexList;
    private final Queue<InfluxDBSourceSplit> pendingSplits;

    private volatile boolean noMoreSplitsAssignment;

    InfluxdbSourceReader(
            InfluxDBConfig config,
            Context readerContext,
            SeaTunnelRowType seaTunnelRowType,
            List<Integer> columnsIndexList) {
        this.config = config;
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.seaTunnelRowType = seaTunnelRowType;
        this.columnsIndexList = columnsIndexList;
    }

    public void connect() throws ConnectException {
        if (influxdb == null) {
            influxdb = InfluxDBClient.getInfluxDB(config);
            String version = influxdb.version();
            if (!influxdb.ping().isGood()) {
                throw new InfluxdbConnectorException(
                        InfluxdbConnectorErrorCode.CONNECT_FAILED,
                        String.format(
                                "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                                config.getUrl()));
            }
            log.info("connect influxdb successful. sever version :{}.", version);
        }
    }

    @Override
    public void open() throws Exception {
        connect();
    }

    @Override
    public CatalogTable getJdbcSourceTables() {

        return null;
    }

    @Override
    public void close() {
        if (influxdb != null) {
            influxdb.close();
            influxdb = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                InfluxDBSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
                && noMoreSplitsAssignment
                && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded influxDB source");
            context.signalNoMoreElement();
        }
    }


    private void read(InfluxDBSourceSplit split, Collector<SeaTunnelRow> output) {
        QueryResult queryResult = influxdb.query(new Query(split.getQuery(), config.getDatabase()));
        for (QueryResult.Result result : queryResult.getResults()) {
            List<QueryResult.Series> serieList = result.getSeries();
            if (CollectionUtils.isNotEmpty(serieList)) {
                for (QueryResult.Series series : serieList) {
                    for (List<Object> values : series.getValues()) {
                        SeaTunnelRow row =
                                InfluxDBRowConverter.convert(
                                        values, seaTunnelRowType, columnsIndexList);
                        try {
                            output.collect(row);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                log.debug("split[{}] reader influxDB series is empty.", split.splitId());
            }
        }
    }
}
