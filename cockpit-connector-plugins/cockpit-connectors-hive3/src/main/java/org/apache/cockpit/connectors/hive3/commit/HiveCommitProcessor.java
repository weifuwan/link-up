package org.apache.cockpit.connectors.hive3.commit;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.hive3.sink.HiveDataLoader;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategy;

/**
 * Hive提交处理器 - 负责提交流程的整体协调
 */
@Slf4j
public class HiveCommitProcessor {
    private final TablePath tablePath;
    private final WriteStrategy writeStrategy;
    private final HiveDataLoader hiveDataLoader;

    public HiveCommitProcessor(TablePath tablePath,
                               WriteStrategy writeStrategy,
                               HiveDataLoader hiveDataLoader) {
        this.tablePath = tablePath;
        this.writeStrategy = writeStrategy;
        this.hiveDataLoader = hiveDataLoader;
    }

    /**
     * 处理完整的提交流程
     */
    public void processCommit(FileCommitInfo commitInfo) throws Exception {
        // 1. 将文件从临时目录移动到目标目录
        writeStrategy.moveFilesToTarget(commitInfo);

        // 2. 加载数据到Hive表
        hiveDataLoader.loadDataToHiveTable(tablePath, commitInfo);

        // 3. 清理临时目录
        cleanUpTransactionDir(commitInfo);
    }

    /**
     * 清理事务临时目录
     */
    private void cleanUpTransactionDir(FileCommitInfo commitInfo) {
        try {
            String transactionDir = commitInfo.getTransactionDir();
            if (transactionDir != null && !transactionDir.isEmpty()) {
                writeStrategy.getHadoopFileSystemProxy().deleteFile(transactionDir);
                log.debug("Cleaned up transaction directory: {}", transactionDir);
            }
        } catch (Exception e) {
            log.warn("Failed to clean up transaction directory, but continue", e);
        }
    }
}
