package org.apache.cockpit.connectors.oracle.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceConfig;
import org.apache.cockpit.connectors.api.jdbc.format.JdbcInputFormat;
import org.apache.cockpit.connectors.api.jdbc.source.ChunkSplitter;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceSplit;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class OracleSourceReader implements SourceReader<SeaTunnelRow, JdbcSourceSplit> {
    private final Context context;
    private final JdbcInputFormat inputFormat;
    private final ChunkSplitter chunkSplitter;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    public OracleSourceReader(
            Context context, JdbcSourceConfig config, Map<TablePath, CatalogTable> tables, Map<TablePath, JdbcSourceTable> jdbcSourceTables) {
        this.chunkSplitter = ChunkSplitter.create(config);
        this.inputFormat = new JdbcInputFormat(config, tables, chunkSplitter);
        this.jdbcSourceTables = jdbcSourceTables;
        this.context = context;
    }

    @Override
    public CatalogTable getJdbcSourceTables() {
        Collection<JdbcSourceTable> sourceTables = jdbcSourceTables.values();
        if (sourceTables.isEmpty()) {
            throw new RuntimeException("source table is null");
        }
        if (sourceTables.size() == 1) {
            for (JdbcSourceTable jdbcSourceTable : sourceTables) {
                return jdbcSourceTable.getCatalogTable();
            }
        }
        return null;
    }

    @Override
    public void open() throws Exception {
        inputFormat.openInputFormat();
    }

    @Override
    public void close() throws IOException {
        inputFormat.closeInputFormat();

    }

    @Override
    @SuppressWarnings("magicnumber")
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            if (jdbcSourceTables.size() == 1) {
                Collection<JdbcSourceTable> values = jdbcSourceTables.values();
                for (JdbcSourceTable jdbcSourceTable : values) {
                    Collection<JdbcSourceSplit> jdbcSourceSplits = chunkSplitter.generateSplits(jdbcSourceTable);
                    for (JdbcSourceSplit split : jdbcSourceSplits) {
                        try {
                            inputFormat.open(split);
                            while (!inputFormat.reachedEnd()) {
                                SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                                output.collect(seaTunnelRow);
                            }
                        } finally {

                            inputFormat.close();
                        }
                    }

                }

            }
        }
    }

}
