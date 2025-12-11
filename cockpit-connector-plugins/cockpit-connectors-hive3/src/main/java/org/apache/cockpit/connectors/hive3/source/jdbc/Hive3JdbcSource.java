package org.apache.cockpit.connectors.hive3.source.jdbc;

import lombok.SneakyThrows;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSourceConfig;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceSplit;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.JdbcCatalogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Hive3JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(Hive3JdbcSource.class);

    private final JdbcSourceConfig jdbcSourceConfig;
    private final Map<TablePath, JdbcSourceTable> jdbcSourceTables;

    @SneakyThrows
    public Hive3JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.jdbcSourceTables =
                JdbcCatalogUtils.getTables(
                        jdbcSourceConfig.getJdbcConnectionConfig(),
                        jdbcSourceConfig.getTableConfigList());
    }

    @Override
    public String getPluginName() {
        return DbType.MYSQL.getCode();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return jdbcSourceTables.values().stream()
                .map(JdbcSourceTable::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        Map<TablePath, CatalogTable> tables = new HashMap<>();
        for (TablePath tablePath : jdbcSourceTables.keySet()) {
            tables.put(tablePath, jdbcSourceTables.get(tablePath).getCatalogTable());
        }
        return new Hive3JdbcSourceReader(readerContext, jdbcSourceConfig, tables, jdbcSourceTables);
    }

}
