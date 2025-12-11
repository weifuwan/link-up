package org.apache.cockpit.connectors.api.jdbc.source;

import lombok.Builder;
import lombok.Data;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;

import java.io.Serializable;

@Data
@Builder
public class JdbcSourceTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final String query;
    private final String partitionColumn;
    private final Integer partitionNumber;
    private final String partitionStart;
    private final String partitionEnd;
    private final Boolean useSelectCount;
    private final Boolean skipAnalyze;
    private final CatalogTable catalogTable;
}
