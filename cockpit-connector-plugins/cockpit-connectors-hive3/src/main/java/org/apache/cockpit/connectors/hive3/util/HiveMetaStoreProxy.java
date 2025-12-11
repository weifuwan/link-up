package org.apache.cockpit.connectors.hive3.util;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.HiveConfig;
import org.apache.cockpit.connectors.hive3.config.HiveOptions;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorErrorCode;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy implements Closeable, Serializable {
    private static final List<String> HADOOP_CONF_FILES = ImmutableList.of("hive-site.xml");

    private final String metastoreUri;
    private final String hadoopConfDir;
    private final String hiveSitePath;

    private transient HiveMetaStoreClient hiveClient;

    public HiveMetaStoreProxy(ReadonlyConfig config) {
        this.metastoreUri = config.get(HiveOptions.METASTORE_URI);
        this.hadoopConfDir = config.get(HiveConfig.HADOOP_CONF_PATH);
        this.hiveSitePath = config.get(HiveConfig.HIVE_SITE_PATH);
    }

    private synchronized HiveMetaStoreClient getClient() {
        if (hiveClient == null) {
            hiveClient = initializeClient();
        }
        return hiveClient;
    }

    private HiveMetaStoreClient initializeClient() {
        HiveConf hiveConf = buildHiveConf();
        try {
            return new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            String errMsg =
                    String.format(
                            "Failed to initialize HiveMetaStoreClient [uris=%s, hiveSite=%s]",
                            metastoreUri, hiveSitePath);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errMsg, e);
        }
    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);

        if (StringUtils.isNotBlank(hadoopConfDir)) {
            for (String fileName : HADOOP_CONF_FILES) {
                Path path = Paths.get(hadoopConfDir, fileName);
                if (Files.exists(path)) {
                    try {
                        hiveConf.addResource(path.toUri().toURL());
                    } catch (IOException e) {
                        log.warn("Error adding Hadoop config {}", path, e);
                    }
                }
            }
        }
        if (StringUtils.isNotBlank(hiveSitePath)) {
            try {
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            } catch (MalformedURLException e) {
                log.warn("Invalid hiveSitePath {}", hiveSitePath, e);
            }
        }
        log.info("Hive client configuration: {}", hiveConf);
        return hiveConf;
    }


    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return getClient().getTable(dbName, tableName);
        } catch (TException e) {
            String msg = String.format("Failed to get table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            try {
                getClient().appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException ae) {
                log.warn("Partition {} already exists", partition);
            }
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            getClient().dropPartition(dbName, tableName, partition, false);
        }
    }

    @Override
    public synchronized void close() {
        if (Objects.nonNull(hiveClient)) {
            hiveClient.close();
        }
    }
}
