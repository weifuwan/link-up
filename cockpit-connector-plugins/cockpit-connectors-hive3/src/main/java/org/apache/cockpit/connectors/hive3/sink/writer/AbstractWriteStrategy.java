package org.apache.cockpit.connectors.hive3.sink.writer;

import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.config.Constants;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.VariablesSubstitute;
import org.apache.cockpit.connectors.hive3.commit.FileCommitInfo;
import org.apache.cockpit.connectors.hive3.config.FileBaseSinkOptions;
import org.apache.cockpit.connectors.hive3.config.FileFormat;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.cockpit.connectors.hive3.exception.FileConnectorException;
import org.apache.cockpit.connectors.hive3.hadoop.HadoopFileSystemProxy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public abstract class AbstractWriteStrategy<T> implements WriteStrategy<T> {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final FileSinkConfig fileSinkConfig;
    protected final List<Integer> sinkColumnsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected HadoopConf hadoopConf;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    protected String transactionId;
    /**
     * The uuid prefix to make sure same job different file sink will not conflict.
     */
    protected String uuidPrefix;

    protected String transactionDirectory;
    protected LinkedHashMap<String, String> needMoveFiles;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    private LinkedHashMap<String, List<String>> partitionDirAndValuesMap;
    protected SeaTunnelRowType seaTunnelRowType;

    // Checkpoint id from engine is start with 1
    protected Long checkpointId = 0L;
    protected int partId = 0;
    protected int batchSize;
    protected boolean singleFileMode;
    protected int currentBatchSize = 0;

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        this.batchSize = fileSinkConfig.getBatchSize();
        this.singleFileMode = fileSinkConfig.isSingleFileMode();
    }

    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(conf);
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.uuidPrefix = uuidPrefix;
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) throws FileConnectorException {
        if (currentBatchSize >= batchSize && !singleFileMode) {
            newFilePart();
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    public synchronized void newFilePart() {
        this.partId++;
        beingWrittenFile.clear();
        log.debug("new file part: {}", partId);
    }

    /**
     * 将文件从临时目录移动到目标目录
     * 这个方法可以在WriteStrategy中直接调用，避免在Writer中处理复杂的文件移动逻辑
     */
    public void moveFilesToTarget(FileCommitInfo commitInfo) throws IOException {
        LinkedHashMap<String, String> needMoveFiles = commitInfo.getNeedMoveFiles();

        for (Map.Entry<String, String> entry : needMoveFiles.entrySet()) {
            String sourcePath = entry.getKey();
            String targetPath = entry.getValue();

            // 关键修复：规范化目标路径，将反斜杠替换为正斜杠
            String normalizedTargetPath = normalizePath(targetPath);

            try {
                // 检查源文件是否存在
                if (!hadoopFileSystemProxy.fileExist(sourcePath)) {
                    log.warn("Source file does not exist: {}, skip moving", sourcePath);
                    continue;
                }

                // 确保目标目录存在（使用规范化后的路径）
                Path targetDir = new Path(normalizedTargetPath).getParent();
                String targetDirStr = targetDir.toString();

                // 修复：确保目录路径也规范化
                targetDirStr = normalizePath(targetDirStr);

                if (!hadoopFileSystemProxy.fileExist(targetDirStr)) {
                    hadoopFileSystemProxy.mkdirs(targetDirStr);
                    log.debug("Created target directory: {}", targetDirStr);
                }

                // 检查是否为目录（可能是分区目录）
                if (hadoopFileSystemProxy.isFile(sourcePath)) {
                    // 单个文件移动，使用规范化后的目标路径
                    hadoopFileSystemProxy.moveFile(sourcePath, normalizedTargetPath, true);
                    log.debug("Moved file from {} to {}", sourcePath, normalizedTargetPath);
                } else {
                    // 目录移动（处理分区目录），使用规范化后的目标路径
                    hadoopFileSystemProxy.moveDirectory(sourcePath, normalizedTargetPath, true);
                    log.debug("Moved directory from {} to {}", sourcePath, normalizedTargetPath);
                }

            } catch (IOException e) {
                log.error("Failed to move from {} to {}", sourcePath, normalizedTargetPath, e);
                throw new IOException(
                        String.format("Failed to move from %s to %s", sourcePath, normalizedTargetPath),
                        e
                );
            }
        }
    }

    /**
     * 规范化路径：将所有反斜杠替换为正斜杠，并清理多余斜杠
     */
    private String normalizePath(String path) {
        if (path == null || path.isEmpty()) {
            return path;
        }

        // 1. 将所有反斜杠替换为正斜杠
        String normalized = path.replace('\\', '/');

        // 2. 清理多个连续斜杠（除了开头的//，因为可能是hdfs://）
        if (normalized.startsWith("hdfs://")) {
            // 保留hdfs://开头的双斜杠
            String prefix = "hdfs://";
            String rest = normalized.substring(prefix.length());
            rest = rest.replaceAll("/+", "/");
            normalized = prefix + rest;
        } else {
            // 其他情况清理所有连续斜杠
            normalized = normalized.replaceAll("/+", "/");
        }

        // 3. 如果是Windows绝对路径（如C:\path\to\file），转换为HDFS格式（/C/path/to/file）
        if (normalized.length() > 2 && normalized.charAt(1) == ':') {
            normalized = "/" + normalized.charAt(0) + normalized.substring(2).replace('\\', '/');
        }

        log.debug("Path normalized: {} -> {}", path, normalized);
        return normalized;
    }

    /**
     * 获取目标文件路径（用于Hive加载）
     */
    public List<String> getTargetPathsForHive(FileCommitInfo commitInfo) {
        List<String> targetPaths = new ArrayList<>();

        // 收集所有需要移动的文件的目标路径
        for (Map.Entry<String, String> entry : commitInfo.getNeedMoveFiles().entrySet()) {
            String targetPath = entry.getValue();

            // 如果是分区表，需要将目录路径转换为Hive可识别的格式
            if (targetPath.contains("=")) {
                // 分区目录，需要获取父目录作为Hive分区路径
                Path path = new Path(targetPath);
                Path parent = path.getParent();
                if (parent != null) {
                    targetPaths.add(parent.toString());
                }
            } else {
                // 非分区表，直接使用目标文件路径
                targetPaths.add(targetPath);
            }
        }

        // 去重（同一个分区可能有多个文件）
        return targetPaths.stream().distinct().collect(Collectors.toList());
    }

    protected SeaTunnelRowType buildSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType, List<Integer> sinkColumnsIndex) {
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        List<String> newFieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> newFieldTypes = new ArrayList<>();
        sinkColumnsIndex.forEach(
                index -> {
                    newFieldNames.add(fieldNames[index]);
                    newFieldTypes.add(fieldTypes[index]);
                });
        return new SeaTunnelRowType(
                newFieldNames.toArray(new String[0]),
                newFieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param hadoopConf hadoop conf
     * @return Configuration
     */
    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = hadoopConf.toConfiguration();
        this.hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    /**
     * set seaTunnelRowTypeInfo in writer
     *
     * @param catalogTable seaTunnelRowType
     */
//    @Override
    public void setCatalogTable(CatalogTable catalogTable) {
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    /**
     * use seaTunnelRow generate partition directory
     *
     * @param seaTunnelRow seaTunnelRow
     * @return the map of partition directory
     */
    @Override
    public LinkedHashMap<String, List<String>> generatorPartitionDir(SeaTunnelRow seaTunnelRow) {
        List<Integer> partitionFieldsIndexInRow = fileSinkConfig.getPartitionFieldsIndexInRow();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>(1);
        if (CollectionUtils.isEmpty(partitionFieldsIndexInRow)) {
            partitionDirAndValuesMap.put(FileBaseSinkOptions.NON_PARTITION, null);
            return partitionDirAndValuesMap;
        }
        List<String> partitionFieldList = fileSinkConfig.getPartitionFieldList();
        String partitionDirExpression = fileSinkConfig.getPartitionDirExpression();
        String[] keys = new String[partitionFieldList.size()];
        String[] values = new String[partitionFieldList.size()];
        for (int i = 0; i < partitionFieldList.size(); i++) {
            keys[i] = "k" + i;
            values[i] = "v" + i;
        }
        List<String> vals = new ArrayList<>(partitionFieldsIndexInRow.size());
        String partitionDir;
        if (StringUtils.isBlank(partitionDirExpression)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                stringBuilder
                        .append(partitionFieldList.get(i))
                        .append("=")
                        .append(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)]);
                if (i < partitionFieldsIndexInRow.size() - 1) {
                    stringBuilder.append("/");
                }
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = stringBuilder.toString();
        } else {
            Map<String, String> valueMap = new HashMap<>(partitionFieldList.size() * 2);
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                valueMap.put(keys[i], partitionFieldList.get(i));
                valueMap.put(
                        values[i],
                        seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
                vals.add(seaTunnelRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = VariablesSubstitute.substitute(partitionDirExpression, valueMap);
        }
        partitionDirAndValuesMap.put(partitionDir, vals);
        return partitionDirAndValuesMap;
    }

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    @Override
    public final String generateFileName(String transactionId) {
        String fileNameExpression = fileSinkConfig.getFileNameExpression();
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix;
        if (StringUtils.isNotEmpty(fileSinkConfig.getFilenameExtension())) {
            suffix =
                    fileSinkConfig.getFilenameExtension().startsWith(".")
                            ? fileSinkConfig.getFilenameExtension()
                            : "." + fileSinkConfig.getFilenameExtension();
        } else {
            suffix = fileFormat.getSuffix();
//            suffix = compressFormat.getCompressCodec() + suffix;
        }
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + suffix;
        }
        String timeFormat = fileSinkConfig.getFileNameTimeFormat();
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(FileBaseSinkOptions.TRANSACTION_EXPRESSION, transactionId);
        String substitute = VariablesSubstitute.substitute(fileNameExpression, valuesMap);
        if (!singleFileMode) {
            substitute += "_" + partId;
        }
        return substitute + suffix;
    }

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    @SneakyThrows
    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        if (this.needMoveFiles.isEmpty() && fileSinkConfig.isCreateEmptyFileWhenNoData()) {
            String filePath = createFilePathWithoutPartition();
            this.getOrCreateOutputStream(filePath);
        }
        this.finishAndCloseFile();
        LinkedHashMap<String, String> commitMap = new LinkedHashMap<>(this.needMoveFiles);
        LinkedHashMap<String, List<String>> copyMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));

        return Optional.of(new FileCommitInfo(commitMap, copyMap, transactionDirectory));
    }

    /**
     * abort prepare commit operation
     */
    @Override
    public void abortPrepare() {
        abortPrepare(transactionId);
    }

    /**
     * abort prepare commit operation using transaction directory
     *
     * @param transactionId transaction id
     */
    public void abortPrepare(String transactionId) {
        try {
            hadoopFileSystemProxy.deleteFile(getTransactionDir(transactionId));
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "Abort transaction "
                            + transactionId
                            + " error, delete transaction directory failed",
                    e);
        }
    }

    /**
     * when a checkpoint completed, file connector should begin a new transaction and generate new
     * transaction id
     *
     * @param checkpointId checkpoint id
     */
    public void beginTransaction(Long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = getTransactionId(checkpointId);
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new LinkedHashMap<>();
        this.partitionDirAndValuesMap = new LinkedHashMap<>();
    }

    private String getTransactionId(Long checkpointId) {
        return "T"
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + jobId
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + uuidPrefix
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + subTaskIndex
                + FileBaseSinkOptions.TRANSACTION_ID_SPLIT
                + checkpointId;
    }

    /**
     * using transaction id generate transaction directory
     *
     * @param transactionId transaction id
     * @return transaction directory
     */
    private String getTransactionDir(@NonNull String transactionId) {
        String transactionDirectoryPrefix =
                getTransactionDirPrefix(fileSinkConfig.getTmpPath(), jobId, uuidPrefix);
        return String.join(
                File.separator, new String[]{transactionDirectoryPrefix, transactionId});
    }

    public static String getTransactionDirPrefix(String tmpPath, String jobId, String uuidPrefix) {
        String[] strings = new String[]{tmpPath, FileBaseSinkOptions.SEATUNNEL, jobId, uuidPrefix};
        return String.join(File.separator, strings);
    }

    public String createFilePathWithoutPartition() {
        return getPathWithPartitionInfo(null, true);
    }

    public String getOrCreateFilePathBeingWritten(@NonNull SeaTunnelRow seaTunnelRow) {
        LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap =
                generatorPartitionDir(seaTunnelRow);
        boolean noPartition =
                FileBaseSinkOptions.NON_PARTITION.equals(
                        dataPartitionDirAndValuesMap.keySet().toArray()[0].toString());
        return getPathWithPartitionInfo(dataPartitionDirAndValuesMap, noPartition);
    }

    private String getPathWithPartitionInfo(
            LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap, boolean noPartition) {
        String beingWrittenFileKey =
                noPartition
                        ? FileBaseSinkOptions.NON_PARTITION
                        : dataPartitionDirAndValuesMap.keySet().toArray()[0].toString();
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments =
                    new String[]{
                            transactionDirectory, beingWrittenFileKey, generateFileName(transactionId)
                    };
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            if (!noPartition) {
                partitionDirAndValuesMap.putAll(dataPartitionDirAndValuesMap);
            }
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath =
                seaTunnelFilePath.replaceAll(
                        Matcher.quoteReplacement(transactionDirectory),
                        Matcher.quoteReplacement(fileSinkConfig.getPath()));
        return tmpPath.replaceAll(
                FileBaseSinkOptions.NON_PARTITION + Matcher.quoteReplacement(File.separator), "");
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    @Override
    public HadoopFileSystemProxy getHadoopFileSystemProxy() {
        return hadoopFileSystemProxy;
    }

    @Override
    public void close() throws IOException {
        try {
            if (hadoopFileSystemProxy != null) {
                hadoopFileSystemProxy.close();
            }
        } catch (Exception ignore) {
        }
    }
}
