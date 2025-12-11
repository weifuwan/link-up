package org.apache.cockpit.connectors.hive3.storage;

import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;

public interface Storage {
    HadoopConf buildHadoopConfWithReadOnlyConfig(ReadonlyConfig readonlyConfig);
}
