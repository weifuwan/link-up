package org.apache.cockpit.connectors.hive3.config;

import java.util.List;

public interface PartitionConfig {
    List<String> getPartitionFieldList();

    boolean isPartitionFieldWriteInFile();
}
