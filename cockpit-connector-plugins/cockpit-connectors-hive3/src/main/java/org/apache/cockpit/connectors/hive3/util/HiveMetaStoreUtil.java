package org.apache.cockpit.connectors.hive3.util;

import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import static org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions.THRIFT_URL;

public class HiveMetaStoreUtil {

    public static HiveMetaStoreClient createHiveMetaStoreClient(HadoopConf hadoopConf, ReadonlyConfig readonlyConfig) throws MetaException {
        HiveConf hiveConf = new HiveConf();

        hadoopConf.toConfiguration().iterator().forEachRemaining(entry -> {
            hiveConf.set(entry.getKey(), entry.getValue());
        });
        String thriftUrl = readonlyConfig.get(THRIFT_URL);
        if (StringUtils.isNoneBlank(thriftUrl)) {
            hiveConf.set("hive.metastore.uris", readonlyConfig.get(THRIFT_URL));
        }
        return new HiveMetaStoreClient(hiveConf);
    }
}
