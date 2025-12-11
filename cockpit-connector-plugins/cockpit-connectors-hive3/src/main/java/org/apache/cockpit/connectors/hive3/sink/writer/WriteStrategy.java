package org.apache.cockpit.connectors.hive3.sink.writer;

import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.hive3.commit.FileCommitInfo;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.cockpit.connectors.hive3.exception.FileConnectorException;
import org.apache.cockpit.connectors.hive3.hadoop.HadoopFileSystemProxy;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public interface WriteStrategy<T> extends Transaction, Serializable, Closeable {
    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex);

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param conf hadoop conf
     * @return Configuration
     */
    Configuration getConfiguration(HadoopConf conf);

    /**
     * write seaTunnelRow to target datasource
     *
     * @param seaTunnelRow seaTunnelRow
     * @throws FileConnectorException Exceptions
     */
    void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException;

    void moveFilesToTarget(FileCommitInfo commitInfo) throws IOException;

    List<String> getTargetPathsForHive(FileCommitInfo commitInfo);
    /**
     * set catalog table to write strategy
     *
     * @param catalogTable catalogTable
     */
    void setCatalogTable(CatalogTable catalogTable);

    /**
     * use seaTunnelRow generate partition directory
     *
     * @param seaTunnelRow seaTunnelRow
     * @return the map of partition directory
     */
    LinkedHashMap<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow);

    T getOrCreateOutputStream(String path) throws IOException;

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    String generateFileName(String transactionId);

    Optional<FileCommitInfo> prepareCommit();

    /**
     * when a transaction is triggered, release resources
     */
    void finishAndCloseFile();

    /**
     * get current checkpoint id
     *
     * @return checkpoint id
     */
    long getCheckpointId();

    /**
     * get sink configuration
     *
     * @return sink configuration
     */
    FileSinkConfig getFileSinkConfig();

    /**
     * get file system utils
     *
     * @return file system utils
     */
    HadoopFileSystemProxy getHadoopFileSystemProxy();
}
