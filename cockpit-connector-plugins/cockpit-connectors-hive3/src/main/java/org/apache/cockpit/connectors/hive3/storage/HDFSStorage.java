package org.apache.cockpit.connectors.hive3.storage;

import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorErrorCode;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class HDFSStorage extends AbstractStorage {

    private String hiveSdLocation;

    public HDFSStorage(String hiveSdLocation) {
        this.hiveSdLocation = hiveSdLocation;
    }

    @Override
    public HadoopConf buildHadoopConfWithReadOnlyConfig(ReadonlyConfig readonlyConfig) {
        try {
            String path = new URI(hiveSdLocation).getPath();
            HadoopConf hadoopConf = new HadoopConf(hiveSdLocation.replace(path, StringUtils.EMPTY));
            Configuration configuration = loadHiveBaseHadoopConfig(readonlyConfig);
            Map<String, String> propsInConfiguration =
                    configuration.getPropsWithPrefix(StringUtils.EMPTY);
            hadoopConf.setExtraOptions(propsInConfiguration);
            return hadoopConf;
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hiveSdLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }
}
