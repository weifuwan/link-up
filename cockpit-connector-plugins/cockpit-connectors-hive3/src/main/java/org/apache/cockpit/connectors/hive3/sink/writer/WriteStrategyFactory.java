package org.apache.cockpit.connectors.hive3.sink.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.hive3.config.FileFormat;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.exception.FileConnectorException;

@Slf4j
public class WriteStrategyFactory {

    private WriteStrategyFactory() {}

    public static WriteStrategy of(String fileType, FileSinkConfig fileSinkConfig) {
        try {
            FileFormat fileFormat = FileFormat.valueOf(fileType.toUpperCase());
            return fileFormat.getWriteStrategy(fileSinkConfig);
        } catch (IllegalArgumentException e) {
            String errorMsg =
                    String.format(
                            "File sink connector not support this file type [%s], please check your config",
                            fileType);
            throw new FileConnectorException(CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    public static WriteStrategy of(FileFormat fileFormat, FileSinkConfig fileSinkConfig) {
        return fileFormat.getWriteStrategy(fileSinkConfig);
    }
}
