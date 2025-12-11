package org.apache.cockpit.connectors.doris.source.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.doris.config.DorisSourceConfig;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;
import org.apache.cockpit.connectors.doris.rest.PartitionDefinition;
import org.apache.cockpit.connectors.doris.rest.RestService;
import org.apache.cockpit.connectors.doris.source.DorisSourceTable;
import org.apache.cockpit.connectors.doris.source.split.DorisSourceSplit;

import java.io.IOException;
import java.util.*;

@Slf4j
public class DorisSourceReader implements SourceReader<SeaTunnelRow, DorisSourceSplit> {

    private final SourceReader.Context context;
    private final DorisSourceConfig dorisSourceConfig;

    private final Queue<DorisSourceSplit> splitsQueue;
    private volatile boolean noMoreSplits;

    private DorisValueReader valueReader;

    private final Map<TablePath, DorisSourceTable> tables;

    public DorisSourceReader(
            Context context,
            DorisSourceConfig dorisSourceConfig,
            Map<TablePath, DorisSourceTable> tables) {
        this.splitsQueue = new ArrayDeque<>();
        this.context = context;
        this.dorisSourceConfig = dorisSourceConfig;
        this.tables = tables;
    }

    @Override
    public void open() throws Exception {
    }

    @Override
    public CatalogTable getJdbcSourceTables() {
        Collection<DorisSourceTable> sourceTables = tables.values();
        if (sourceTables.isEmpty()) {
            throw new RuntimeException("source table is null");
        }
        if (sourceTables.size() == 1) {
            for (DorisSourceTable dorisSourceTable : sourceTables) {
                return dorisSourceTable.getCatalogTable();
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (valueReader != null) {
            valueReader.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            List<DorisSourceSplit> dorisSourceSplit = getDorisSourceSplit();
            for (DorisSourceSplit nextSplit : dorisSourceSplit) {
                if (nextSplit != null) {
                    PartitionDefinition partition = nextSplit.getPartitionDefinition();
                    DorisSourceTable dorisSourceTable =
                            tables.get(TablePath.of(partition.getDatabase(), partition.getTable()));
                    if (dorisSourceTable == null) {
                        throw new DorisConnectorException(
                                DorisConnectorErrorCode.SHOULD_NEVER_HAPPEN,
                                String.format(
                                        "the table '%s.%s' cannot be found in table_list of job configuration.",
                                        partition.getDatabase(), partition.getTable()));
                    }
                    valueReader = new DorisValueReader(partition, dorisSourceConfig, dorisSourceTable);
                    while (valueReader.hasNext()) {
                        SeaTunnelRow record = valueReader.next();
                        output.collect(record);
                    }
                }
                if (Boundedness.BOUNDED.equals(context.getBoundedness())
                        && noMoreSplits
                        && splitsQueue.isEmpty()) {
                    // signal to the source that we have reached the end of the data.
                    log.info("Closed the bounded Doris source");
                    context.signalNoMoreElement();
                }
            }

        }
    }

    private List<DorisSourceSplit> getDorisSourceSplit() {
        List<DorisSourceSplit> splits = new ArrayList<>();
        for (DorisSourceTable dorisSourceTable : tables.values()) {
            List<PartitionDefinition> partitions =
                    RestService.findPartitions(dorisSourceConfig, dorisSourceTable, log);
            for (PartitionDefinition partition : partitions) {
                splits.add(new DorisSourceSplit(partition, String.valueOf(partition.hashCode())));
            }
        }
        return splits;
    }

}
