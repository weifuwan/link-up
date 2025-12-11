package org.apache.cockpit.connectors.clickhouse.sink.file;

import java.util.List;

public interface FileTransfer {

    void init();

    void transferAndChown(String sourcePath, String targetPath);

    void transferAndChown(List<String> sourcePath, String targetPath);

    void close();
}
