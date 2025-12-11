package org.apache.cockpit.connectors.hive3.hadoop;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HadoopFileSystemProxy implements Serializable, Closeable {

    private transient UserGroupInformation userGroupInformation;
    private transient FileSystem fileSystem;

    private transient Configuration configuration;
    private final HadoopConf hadoopConf;
    private boolean isAuthTypeKerberos;

    public HadoopFileSystemProxy(@NonNull HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
        // eager initialization
        initialize();
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        return execute(() -> getFileSystem().exists(new Path(filePath)));
    }

    public boolean isFile(@NonNull String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)).isFile());
    }

    public void createFile(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    if (!getFileSystem().createNewFile(new Path(filePath))) {
                        throw CommonError.fileOperationFailed("SeaTunnel", "create", filePath);
                    }
                    return Void.class;
                });
    }

    public void moveFile(String sourcePath, String targetPath, boolean overwrite) throws IOException {
        execute(() -> {
            Path srcPath = new Path(sourcePath);
            Path dstPath = new Path(targetPath);

            // 检查源文件是否存在
            if (!fileExist(sourcePath)) {
                log.warn("Source file does not exist: {}, skip moving", sourcePath);
                return Void.class;
            }

            // 确保目标目录存在
            Path dstParent = dstPath.getParent();
            if (dstParent != null && !fileExist(dstParent.toString())) {
                mkdirs(dstParent.toString());
                log.debug("Created parent directory: {}", dstParent);
            }

            // 如果目标文件已存在且允许覆盖，先删除
            if (fileExist(targetPath)) {
                if (overwrite) {
                    deleteFile(targetPath);
                    log.debug("Deleted existing target file: {}", targetPath);
                } else {
                    throw new IOException(String.format("Target file already exists: %s", targetPath));
                }
            }

            // 尝试直接重命名（同目录下最快）
            if (getFileSystem().rename(srcPath, dstPath)) {
                log.debug("Successfully renamed file from {} to {}", sourcePath, targetPath);
                return Void.class;
            }

            // 如果重命名失败（可能是跨目录），使用copy+delete方式
            log.debug("Direct rename failed for cross-directory move, using copy+delete strategy");
            copyFileThenDelete(sourcePath, targetPath, overwrite);

            return Void.class;
        });
    }

    /**
     * 复制文件然后删除源文件
     */
    private void copyFileThenDelete(String sourcePath, String targetPath, boolean overwrite) throws IOException {
        Path srcPath = new Path(sourcePath);
        Path dstPath = new Path(targetPath);

        // 使用Hadoop的copy API
        FileUtil.copy(
                getFileSystem(),
                srcPath,
                getFileSystem(),
                dstPath,
                false,  // 不删除源文件
                overwrite,  // 是否覆盖
                getFileSystem().getConf()
        );

        // 删除源文件
        getFileSystem().delete(srcPath, true);
        log.debug("Copied and deleted source file: {} -> {}", sourcePath, targetPath);
    }

    /**
     * 递归创建目录
     */
    public void mkdirs(String dirPath) throws IOException {
        execute(() -> {
            Path path = new Path(dirPath);
            if (!getFileSystem().mkdirs(path)) {
                throw CommonError.fileOperationFailed("SeaTunnel", "mkdirs", dirPath);
            }
            return Void.class;
        });
    }

    /**
     * 移动整个目录
     */
    public void moveDirectory(String sourceDir, String targetDir, boolean overwrite) throws IOException {
        execute(() -> {
            Path srcDir = new Path(sourceDir);
            Path dstDir = new Path(targetDir);

            if (!fileExist(sourceDir)) {
                log.warn("Source directory does not exist: {}, skip moving", sourceDir);
                return Void.class;
            }

            // 确保目标父目录存在
            Path dstParent = dstDir.getParent();
            if (dstParent != null && !fileExist(dstParent.toString())) {
                mkdirs(dstParent.toString());
            }

            // 如果目标目录已存在
            if (fileExist(targetDir)) {
                if (overwrite) {
                    // 删除目标目录
                    deleteFile(targetDir);
                } else {
                    throw new IOException(String.format("Target directory already exists: %s", targetDir));
                }
            }

            // 尝试重命名整个目录
            if (getFileSystem().rename(srcDir, dstDir)) {
                log.debug("Successfully renamed directory from {} to {}", sourceDir, targetDir);
                return Void.class;
            }

            // 如果重命名失败，递归复制目录
            log.debug("Direct rename failed, copying directory recursively");
            copyDirectoryThenDelete(sourceDir, targetDir);

            return Void.class;
        });
    }

    /**
     * 递归复制目录然后删除源目录
     */
    private void copyDirectoryThenDelete(String sourceDir, String targetDir) throws IOException {
        Path srcDir = new Path(sourceDir);
        Path dstDir = new Path(targetDir);

        // 使用Hadoop的copy API递归复制目录
        FileUtil.copy(
                getFileSystem(),
                srcDir,
                getFileSystem(),
                dstDir,
                false,  // 不删除源
                true,   // 递归复制
                getFileSystem().getConf()
        );

        // 删除源目录
        getFileSystem().delete(srcDir, true);
        log.debug("Copied and deleted directory: {} -> {}", sourceDir, targetDir);
    }

    public void deleteFile(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    Path path = new Path(filePath);
                    if (getFileSystem().exists(path)) {
                        if (!getFileSystem().delete(path, true)) {
                            throw CommonError.fileOperationFailed("SeaTunnel", "delete", filePath);
                        }
                    }
                    return Void.class;
                });
    }

    public void renameFile(
            @NonNull String oldFilePath,
            @NonNull String newFilePath,
            boolean removeWhenNewFilePathExist)
            throws IOException {
        execute(
                () -> {
                    Path oldPath = new Path(oldFilePath);
                    Path newPath = new Path(newFilePath);

                    if (!fileExist(oldPath.toString())) {
                        log.warn(
                                "rename file :["
                                        + oldPath
                                        + "] to ["
                                        + newPath
                                        + "] already finished in the last commit, skip");
                        return Void.class;
                    }

                    if (removeWhenNewFilePathExist) {
                        if (fileExist(newFilePath)) {
                            getFileSystem().delete(newPath, true);
                            log.info("Delete already file: {}", newPath);
                        }
                    }
                    if (!fileExist(newPath.getParent().toString())) {
                        createDir(newPath.getParent().toString());
                    }

                    if (getFileSystem().rename(oldPath, newPath)) {
                        log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
                    } else {
                        throw CommonError.fileOperationFailed(
                                "SeaTunnel", "rename", oldFilePath + " -> " + newFilePath);
                    }
                    return Void.class;
                });
    }

    public void createDir(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    Path dfs = new Path(filePath);
                    if (!getFileSystem().mkdirs(dfs)) {
                        throw CommonError.fileOperationFailed("SeaTunnel", "create", filePath);
                    }
                    return Void.class;
                });
    }

    public List<LocatedFileStatus> listFile(String path) throws IOException {
        return execute(
                () -> {
                    List<LocatedFileStatus> fileList = new ArrayList<>();
                    if (!fileExist(path)) {
                        return fileList;
                    }
                    Path fileName = new Path(path);
                    RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                            getFileSystem().listFiles(fileName, false);
                    while (locatedFileStatusRemoteIterator.hasNext()) {
                        fileList.add(locatedFileStatusRemoteIterator.next());
                    }
                    return fileList;
                });
    }

    public List<Path> getAllSubFiles(@NonNull String filePath) throws IOException {
        return execute(
                () -> {
                    List<Path> pathList = new ArrayList<>();
                    if (!fileExist(filePath)) {
                        return pathList;
                    }
                    Path fileName = new Path(filePath);
                    FileStatus[] status = getFileSystem().listStatus(fileName);
                    if (status != null) {
                        for (FileStatus fileStatus : status) {
                            if (fileStatus.isDirectory()) {
                                pathList.add(fileStatus.getPath());
                            }
                        }
                    }
                    return pathList;
                });
    }

    public FileStatus[] listStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().listStatus(new Path(filePath)));
    }

    public FileStatus getFileStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)));
    }

    public FSDataOutputStream getOutputStream(String filePath) throws IOException {
        return execute(() -> getFileSystem().create(new Path(filePath), true));
    }

    public FSDataInputStream getInputStream(String filePath) throws IOException {
        return execute(() -> getFileSystem().open(new Path(filePath)));
    }

    public FileSystem getFileSystem() {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem;
    }

    @Override
    public void close() throws IOException {
        try {
            if (userGroupInformation != null && isAuthTypeKerberos) {
                userGroupInformation.logoutUserFromKeytab();
            }
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    @SneakyThrows
    private void initialize() {
        this.configuration = createConfiguration();

        fileSystem = FileSystem.get(configuration);
        fileSystem.setWriteChecksum(false);
        isAuthTypeKerberos = false;
    }

    private Configuration createConfiguration() {
        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    private <T> T execute(PrivilegedExceptionAction<T> action) throws IOException {
        // The execute method is used to handle privileged actions, ensuring that the correct
        // user context (Kerberos or otherwise) is applied when performing file system operations.
        // This is necessary to maintain security and proper access control in a Hadoop environment.
        // If kerberos is disabled, the action is run directly. If kerberos is enabled, the action
        // is run as a privileged action using the doAsPrivileged method.
        if (isAuthTypeKerberos) {
            return doAsPrivileged(action);
        } else {
            try {
                return action.run();
            } catch (IOException | SeaTunnelRuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private <T> T doAsPrivileged(PrivilegedExceptionAction<T> action) throws IOException {
        if (fileSystem == null || userGroupInformation == null) {
            initialize();
        }

        try {
            return userGroupInformation.doAs(action);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
