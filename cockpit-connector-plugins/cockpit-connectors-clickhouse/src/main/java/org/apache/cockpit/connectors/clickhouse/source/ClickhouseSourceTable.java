package org.apache.cockpit.connectors.clickhouse.source;

import lombok.Builder;
import lombok.Data;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.clickhouse.sink.file.ClickhouseTable;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class ClickhouseSourceTable implements Serializable {
    private static final long serialVersionUID = -457477523311211973L;

    private TablePath tablePath;
    private String originQuery;
    private String filterQuery;
    private Integer splitSize;
    private Integer batchSize;
    private List<String> partitionList;
    private ClickhouseTable clickhouseTable;
    private boolean isSqlStrategyRead;
    private boolean isComplexSql;
    private CatalogTable catalogTable;
}
