package org.apache.cockpit.connectors.clickhouse.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@Getter
public class DistributedEngine implements Serializable {

    private static final long serialVersionUID = -1L;
    private String clusterName;
    private String database;
    private String table;
    private String tableEngine;
    private String tableDDL;
    private String sortingKey;
}
