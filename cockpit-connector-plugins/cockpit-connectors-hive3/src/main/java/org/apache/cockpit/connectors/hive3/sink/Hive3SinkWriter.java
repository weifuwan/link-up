package org.apache.cockpit.connectors.hive3.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCode;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSinkConfig;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.hive3.commit.FileCommitInfo;
import org.apache.cockpit.connectors.hive3.commit.HiveCommitProcessor;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategy;
import org.apache.cockpit.connectors.hive3.util.HiveMetaStoreUtil;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class Hive3SinkWriter implements SinkWriter<SeaTunnelRow> {

    protected JdbcDialect dialect;
    protected TablePath sinkTablePath;
    protected TableSchema tableSchema;
    protected TableSchema databaseTableSchema;
    protected FileSinkConfig fileSinkConfig;
    protected final WriteStrategy writeStrategy;
    protected final HadoopConf hadoopConf;
    protected HiveMetaStoreClient hiveMetaStoreClient;
    private final JdbcSinkConfig jdbcSinkConfig;

    private final HiveDataLoader hiveDataLoader;

    private Optional<FileCommitInfo> pendingCommitInfo = Optional.empty();

    private final ReadonlyConfig readonlyConfig;
    public Hive3SinkWriter(TablePath sinkTablePath,
                           JdbcDialect dialect,
                           FileSinkConfig fileSinkConfig,
                           TableSchema tableSchema,
                           TableSchema databaseTableSchema,
                           WriteStrategy writeStrategy,
                           HadoopConf hadoopConf,
                           JdbcSinkConfig jdbcSinkConfig,
                           ReadonlyConfig readonlyConfig) {
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.readonlyConfig = readonlyConfig;
        this.sinkTablePath = sinkTablePath;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.databaseTableSchema = databaseTableSchema;
        this.fileSinkConfig = fileSinkConfig;
        this.writeStrategy = writeStrategy;
        this.hadoopConf = hadoopConf;

        String uuidPrefix = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
        String jobId = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 6);
        this.writeStrategy.init(hadoopConf, jobId, uuidPrefix, 1);

        initHiveMetaStoreClient();

        // 初始化Hive数据加载器
        this.hiveDataLoader = new HiveDataLoader(hiveMetaStoreClient, writeStrategy, dialect, jdbcSinkConfig);

        writeStrategy.beginTransaction(System.currentTimeMillis());
    }

    private void initHiveMetaStoreClient() {
        try {
            this.hiveMetaStoreClient = HiveMetaStoreUtil.createHiveMetaStoreClient(hadoopConf, readonlyConfig);
            log.info("Hive MetaStore client initialized successfully for table: {}", sinkTablePath);
        } catch (Exception e) {
            log.error("Failed to initialize Hive MetaStore client", e);
            throw new SeaTunnelRuntimeException(
                    CommonErrorCode.CONVERT_TO_CONNECTOR_TYPE_ERROR,
                    String.format("Failed to initialize Hive MetaStore client for table %s", sinkTablePath),
                    e
            );
        }
    }

    @Override
    public void close() throws IOException {
        prepareCommit();
        try {
            if (hiveMetaStoreClient != null) {
                hiveMetaStoreClient.close();
            }
        } catch (Exception e) {
            log.warn("Error closing Hive MetaStore client", e);
        }

        if (writeStrategy != null) {
            writeStrategy.close();
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            writeStrategy.write(element);
        } catch (SeaTunnelRuntimeException e) {
            throw CommonError.writeSeaTunnelRowFailed("FileConnector", element.toString(), e);
        }
    }

    @Override
    public Optional prepareCommit() throws IOException {
        Optional<FileCommitInfo> commitInfo = writeStrategy.prepareCommit();

        if (commitInfo.isPresent()) {
            this.pendingCommitInfo = commitInfo;

            try {
                HiveCommitProcessor commitProcessor = new HiveCommitProcessor(
                        sinkTablePath, writeStrategy, hiveDataLoader);
                commitProcessor.processCommit(commitInfo.get());

                log.info("Successfully prepared commit for table: {}", sinkTablePath);
                return commitInfo;

            } catch (Exception e) {
                log.error("Failed to prepare commit for table: {}", sinkTablePath, e);
                abortCommit();
                throw new SeaTunnelRuntimeException(
                        CommonErrorCode.CONVERT_TO_SEATUNNEL_PROPS_BLANK_ERROR,
                        String.format("Failed to prepare commit for table %s", sinkTablePath),
                        e
                );
            }
        }

        return Optional.empty();
    }

    /**
     * 回滚提交操作
     */
    private void abortCommit() {
        try {
            if (pendingCommitInfo.isPresent()) {
                writeStrategy.abortPrepare();
                log.info("Aborted commit for table: {}", sinkTablePath);
            }
        } catch (Exception e) {
            log.error("Error during abort commit", e);
        }
    }
}