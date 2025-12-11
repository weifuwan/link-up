package org.apache.cockpit.connectors.clickhouse.sink.file;

import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.clickhouse.util.DistributedEngine;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ClickhouseTable implements Serializable {

    private String database;
    private String tableName;
    private String engine;
    private String engineFull;
    private String createTableDDL;
    private List<String> dataPaths;
    private String sortingKey;
    private final DistributedEngine distributedEngine;
    private Map<String, String> tableSchema;

    public ClickhouseTable(
            String database,
            String tableName,
            DistributedEngine distributedEngine,
            String engine,
            String createTableDDL,
            String engineFull,
            List<String> dataPaths,
            String sortingKey,
            Map<String, String> tableSchema) {
        this.database = database;
        this.tableName = tableName;
        this.distributedEngine = distributedEngine;
        this.engine = engine;
        this.engineFull = engineFull;
        this.createTableDDL = createTableDDL;
        this.dataPaths = dataPaths;
        this.sortingKey = sortingKey;
        this.tableSchema = tableSchema;
    }

    public String getLocalTableName() {
        if (distributedEngine != null) {
            return distributedEngine.getTable();
        } else {
            return tableName;
        }
    }

    public String getLocalDatabase() {
        if (distributedEngine != null) {
            return distributedEngine.getDatabase();
        } else {
            return database;
        }
    }

    public String getLocalTableIdentifier() {
        if (distributedEngine != null) {
            return String.format("%s.%s", getLocalDatabase(), getLocalTableName());
        } else {
            return String.format("%s.%s", database, tableName);
        }
    }
}
