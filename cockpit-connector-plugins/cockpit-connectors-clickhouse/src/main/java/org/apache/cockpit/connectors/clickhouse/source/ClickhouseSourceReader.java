package org.apache.cockpit.connectors.clickhouse.source;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.source.split.ClickhouseSourceSplit;

import java.io.IOException;
import java.util.*;

@Slf4j
public class ClickhouseSourceReader implements SourceReader<SeaTunnelRow, ClickhouseSourceSplit> {

    private final Map<TablePath, List<ClickHouseNode>> servers;
    private ClickHouseClient client;
    private final Context context;
    private volatile boolean noMoreSplits;
    private final Queue<ClickhouseSourceSplit> splitQueue;
    private final Map<TablePath, ClickhouseSourceTable> tables;

    ClickhouseSourceReader(
            Map<TablePath, List<ClickHouseNode>> servers,
            Context readerContext,
            Map<TablePath, ClickhouseSourceTable> tables) {
        this.servers = servers;
        this.context = readerContext;
        this.splitQueue = new ArrayDeque<>();
        this.tables = tables;
    }

    @Override
    public void open() {}

    @Override
    public CatalogTable getJdbcSourceTables() {
        Collection<ClickhouseSourceTable> sourceTables = tables.values();
        if (sourceTables.isEmpty()) {
            throw new RuntimeException("source table is null");
        }
        if (sourceTables.size() == 1) {
            for (ClickhouseSourceTable dorisSourceTable : sourceTables) {
                return dorisSourceTable.getCatalogTable();
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            ClickhouseSourceSplit split = splitQueue.poll();
            if (split != null) {
                ClickhouseValueReader clickhouseValueReader = null;
                try {
                    ClickhouseSourceTable clickhouseSourceTable =
                            tables.get(split.getConfigTablePath());
                    if (clickhouseSourceTable == null) {
                        throw new ClickhouseConnectorException(
                                ClickhouseConnectorErrorCode.TABLE_NOT_FOUND_ERROR,
                                String.format(
                                        "Table %s.%s not found in table list of job configuration.",
                                        split.getConfigTablePath().getDatabaseName(),
                                        split.getConfigTablePath().getTableName()));
                    }

                    CatalogTable catalogTable = clickhouseSourceTable.getCatalogTable();

                    clickhouseValueReader =
                            new ClickhouseValueReader(
                                    split,
                                    catalogTable.getSeaTunnelRowType(),
                                    clickhouseSourceTable);
                    while (clickhouseValueReader.hasNext()) {
                        List<SeaTunnelRow> next = clickhouseValueReader.next();
                        next.forEach(item -> {
                            try {
                                output.collect(item);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                } finally {
                    if (clickhouseValueReader != null) {
                        clickhouseValueReader.close();
                    }
                }
            } else if (noMoreSplits && splitQueue.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                signalNoMoreElement();
            }
        }
    }


    private void signalNoMoreElement() {
        log.info("Closed the bounded ClickHouse source");
        this.context.signalNoMoreElement();
    }

}
