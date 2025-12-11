package org.apache.cockpit.connectors.starrocks.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.starrocks.client.source.StarRocksBeReadClient;
import org.apache.cockpit.connectors.starrocks.client.source.StarRocksQueryPlanReadClient;
import org.apache.cockpit.connectors.starrocks.client.source.model.QueryPartition;
import org.apache.cockpit.connectors.starrocks.config.SourceConfig;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorException;

import java.io.IOException;
import java.util.*;

@Slf4j
public class StarRocksSourceReader implements SourceReader<SeaTunnelRow, StarRocksSourceSplit> {

    private final SourceReader.Context context;
    private final SourceConfig sourceConfig;
    private Map<String, StarRocksBeReadClient> clientsPools;

    private final Map<String, SeaTunnelRowType> tables;
    private final StarRocksQueryPlanReadClient starRocksQueryPlanReadClient;

    public StarRocksSourceReader(SourceReader.Context readerContext, SourceConfig sourceConfig) {
        this.context = readerContext;
        this.sourceConfig = sourceConfig;
        this.starRocksQueryPlanReadClient = new StarRocksQueryPlanReadClient(sourceConfig);

        Map<String, SeaTunnelRowType> tables = new HashMap<>();
        sourceConfig
                .getTableConfigList()
                .forEach(
                        starRocksSourceTableConfig ->
                                tables.put(
                                        starRocksSourceTableConfig.getTable(),
                                        starRocksSourceTableConfig
                                                .getCatalogTable()
                                                .getSeaTunnelRowType()));
        this.tables = tables;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            Set<String> keySet = tables.keySet();
            for (String sourceTable : keySet) {
                List<StarRocksSourceSplit> starRocksSourceSplit = getStarRocksSourceSplit(sourceTable);

                for (StarRocksSourceSplit split : starRocksSourceSplit) {
                    QueryPartition partition = split.getPartition();
                    String table = partition.getTable();
                    String beAddress = partition.getBeAddress();
                    StarRocksBeReadClient client = null;
                    if (clientsPools.containsKey(beAddress)) {
                        client = clientsPools.get(beAddress);
                    } else {
                        client = new StarRocksBeReadClient(beAddress, sourceConfig);
                        clientsPools.put(beAddress, client);
                    }
                    SeaTunnelRowType seaTunnelRowType = tables.get(partition.getTable());
                    // open scanner to be
                    client.openScanner(partition, seaTunnelRowType);
                    while (client.hasNext()) {
                        SeaTunnelRow seaTunnelRow = client.getNext();
                        seaTunnelRow.setTableId(TablePath.of(table).toString());
                        try {
                            output.collect(seaTunnelRow);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded StarRocks source");
            context.signalNoMoreElement();
        }
    }

    private List<StarRocksSourceSplit> getStarRocksSourceSplit(String table) {
        List<StarRocksSourceSplit> sourceSplits = new ArrayList<>();
        List<QueryPartition> partitions = starRocksQueryPlanReadClient.findPartitions(table);
        for (QueryPartition partition : partitions) {
            sourceSplits.add(
                    new StarRocksSourceSplit(
                            partition, String.valueOf(partition.hashCode())));
        }
        return sourceSplits;
    }

    @Override
    public void open() throws Exception {
        clientsPools = new HashMap<>();
    }

    @Override
    public CatalogTable getJdbcSourceTables() {
//        Collection<SeaTunnelRowType> sourceTables = tables.values();
//        if (sourceTables.isEmpty()) {
//            throw new RuntimeException("source table is null");
//        }
//        if (sourceTables.size() == 1) {
//            for (SeaTunnelRowType jdbcSourceTable : sourceTables) {
//                return jdbcSourceTable.ge();
//            }
//        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (!clientsPools.isEmpty()) {
            clientsPools
                    .values()
                    .forEach(
                            client -> {
                                if (client != null) {
                                    try {
                                        client.close();
                                    } catch (StarRocksConnectorException e) {
                                        log.error("Failed to close reader: ", e);
                                    }
                                }
                            });
        }
    }

}
