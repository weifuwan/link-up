package org.apache.cockpit.connectors.hive3.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSinkConfig;
import org.apache.cockpit.connectors.api.jdbc.connection.JdbcConnectionProvider;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.hive3.commit.FileCommitInfo;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategy;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive数据加载器 - 负责Hive表的加载和分区管理
 */
@Slf4j
public class HiveDataLoader {
    private final HiveMetaStoreClient hiveMetaStoreClient;
    private final WriteStrategy writeStrategy;
    private final JdbcDialect dialect;
    protected final JdbcConnectionProvider connectionProvider;

    public HiveDataLoader(HiveMetaStoreClient hiveMetaStoreClient,
                          WriteStrategy writeStrategy,
                          JdbcDialect dialect,
                          JdbcSinkConfig jdbcSinkConfig) {
        this.hiveMetaStoreClient = hiveMetaStoreClient;
        this.writeStrategy = writeStrategy;
        this.dialect = dialect;
        this.connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
    }

    /**
     * 加载数据到Hive表
     */
    public void loadDataToHiveTable(TablePath tablePath, FileCommitInfo commitInfo) throws Exception {
        Table hiveTable = getHiveTable(tablePath);
        String tableLocation = hiveTable.getSd().getLocation();

        boolean isPartitioned = isPartitionedTable(commitInfo, hiveTable);

        if (isPartitioned) {
            loadDataToPartitionedTable(hiveTable, commitInfo, tableLocation);
        } else {
            loadDataToNonPartitionedTable(hiveTable, commitInfo, tableLocation);
        }
    }

    /**
     * 获取Hive表信息
     */
    private Table getHiveTable(TablePath tablePath) throws Exception {
        String database = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();

        try {
            Table hiveTable = hiveMetaStoreClient.getTable(database, tableName);
            log.info("Found Hive table: {}.{}", database, tableName);
            return hiveTable;
        } catch (NoSuchObjectException e) {
            log.error("Hive table {}.{} does not exist", database, tableName);
            throw new Exception(
                    String.format("Hive table %s.%s does not exist", database, tableName),
                    e
            );
        }
    }

    /**
     * 判断是否为分区表
     */
    private boolean isPartitionedTable(FileCommitInfo commitInfo, Table hiveTable) {
        LinkedHashMap<String, List<String>> partitionMap = commitInfo.getPartitionDirAndValuesMap();

        boolean hasPartitionKeys = !hiveTable.getPartitionKeys().isEmpty();
        boolean hasPartitionData = !partitionMap.containsKey(
                org.apache.cockpit.connectors.hive3.config.FileBaseSinkOptions.NON_PARTITION
        ) && !partitionMap.isEmpty();

        return hasPartitionKeys && hasPartitionData;
    }

    /**
     * 加载数据到非分区表
     */
    private void loadDataToNonPartitionedTable(Table hiveTable, FileCommitInfo commitInfo,
                                               String tableLocation) throws Exception {
        String database = hiveTable.getDbName();
        String tableName = hiveTable.getTableName();

        for (Map.Entry<String, String> entry : commitInfo.getNeedMoveFiles().entrySet()) {
            String targetPath = entry.getValue();
            String fileName = new Path(targetPath).getName();
            String tableFilePath = tableLocation + File.separator + fileName;

            if (!targetPath.startsWith(tableLocation)) {
                writeStrategy.getHadoopFileSystemProxy().moveFile(
                        targetPath,
                        tableFilePath,
                        true
                );
                log.debug("Moved file to table directory: {} -> {}", targetPath, tableFilePath);
            }
        }

        refreshHiveTable(hiveTable);
        log.info("Successfully loaded data to non-partitioned table {}.{}", database, tableName);
    }

    /**
     * 加载数据到分区表
     */
    private void loadDataToPartitionedTable(Table hiveTable, FileCommitInfo commitInfo,
                                            String tableLocation) throws Exception {
        String database = hiveTable.getDbName();
        String tableName = hiveTable.getTableName();

        for (Map.Entry<String, List<String>> partitionEntry :
                commitInfo.getPartitionDirAndValuesMap().entrySet()) {

            String partitionDir = partitionEntry.getKey();
            List<String> partitionValues = partitionEntry.getValue();

            if (partitionDir.equals(
                    org.apache.cockpit.connectors.hive3.config.FileBaseSinkOptions.NON_PARTITION)) {
                continue;
            }

            String partitionPath = tableLocation + File.separator + partitionDir;
            log.info("Partition path: {}", partitionPath);

            moveFilesToPartition(commitInfo, partitionDir, partitionPath);
            addOrUpdatePartition(hiveTable, partitionValues, partitionPath);
        }

        log.info("Successfully loaded data to partitioned table {}.{}", database, tableName);
    }

    /**
     * 移动文件到分区目录
     */
    private void moveFilesToPartition(FileCommitInfo commitInfo, String partitionDir,
                                      String partitionPath) throws Exception {
        for (Map.Entry<String, String> fileEntry : commitInfo.getNeedMoveFiles().entrySet()) {
            String targetPath = fileEntry.getValue();

            if (targetPath.contains(partitionDir)) {
                String fileName = new Path(targetPath).getName();
                String partitionFilePath = partitionPath + File.separator + fileName;

                writeStrategy.getHadoopFileSystemProxy().moveFile(
                        targetPath,
                        partitionFilePath,
                        true
                );
                log.debug("Moved file to partition: {} -> {}", targetPath, partitionFilePath);
            }
        }
    }

    /**
     * 添加或更新分区
     */
    private void addOrUpdatePartition(Table hiveTable, List<String> partitionValues,
                                      String partitionPath) throws Exception {
        List<String> partitionColumns = hiveTable.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(java.util.stream.Collectors.toList());

        if (partitionColumns.size() != partitionValues.size()) {
            throw new Exception(
                    String.format("Partition columns size %d does not match values size %d",
                            partitionColumns.size(), partitionValues.size())
            );
        }

        StringBuilder partitionSpec = new StringBuilder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (i > 0) partitionSpec.append(", ");
            partitionSpec.append(String.format("%s='%s'", partitionColumns.get(i), partitionValues.get(i)));
        }

        String sql = String.format(
                "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s'",
                hiveTable.getTableName(),
                partitionSpec.toString(),
                partitionPath
        );
        String normalizeSql = normalizePath(sql);
        executeHiveSql(normalizeSql);
        log.debug("Added/updated partition: {}", partitionSpec.toString());
    }

    /**
     * 刷新Hive表元数据
     */
    private void refreshHiveTable(Table hiveTable) throws Exception {
        String sql = String.format(
                "MSCK REPAIR TABLE %s.%s",
                hiveTable.getDbName(),
                hiveTable.getTableName()
        );

        executeHiveSql(sql);
        log.debug("Refreshed table metadata: {}.{}",
                hiveTable.getDbName(), hiveTable.getTableName());
    }

    private String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return path;
        }

        String normalized = path.replace('\\', '/');

        if (normalized.startsWith("hdfs://")) {
            String prefix = "hdfs://";
            String rest = normalized.substring(prefix.length());
            rest = rest.replaceAll("/+", "/");
            normalized = prefix + rest;
        } else {
        }

        if (normalized.length() > 2 && normalized.charAt(1) == ':') {
            normalized = "/" + normalized.charAt(0) + normalized.substring(2).replace('\\', '/');
        }

        log.debug("Path normalized: {} -> {}", path, normalized);
        return normalized;
    }

    private void executeHiveSql(String sql) throws Exception {
        try (Connection connection = connectionProvider.getOrEstablishConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);

        } catch (SQLException e) {
            throw new Exception("执行Hive SQL时发生错误: " + e.getMessage(), e);
        }
    }

}
