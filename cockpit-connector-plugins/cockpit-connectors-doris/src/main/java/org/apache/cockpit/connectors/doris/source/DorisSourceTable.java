package org.apache.cockpit.connectors.doris.source;

import lombok.Builder;
import lombok.Data;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;

import java.io.Serializable;

@Data
@Builder
public class DorisSourceTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private String readField;
    private String filterQuery;
    private int batchSize;
    private Integer tabletSize;
    private Long execMemLimit;
    private final CatalogTable catalogTable;
}
