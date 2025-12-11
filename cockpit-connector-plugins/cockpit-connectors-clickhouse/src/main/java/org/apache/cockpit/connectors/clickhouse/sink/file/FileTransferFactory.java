package org.apache.cockpit.connectors.clickhouse.sink.file;


import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseFileCopyMethod;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;

public class FileTransferFactory {
    public static FileTransfer createFileTransfer(
            ClickhouseFileCopyMethod type,
            String host,
            String user,
            String password,
            String keyPath) {
        switch (type) {
            case SCP:
                return new ScpFileTransfer(host, user, password, keyPath);
            case RSYNC:
                return new RsyncFileTransfer(host, user, password, keyPath);
            default:
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "unsupported clickhouse file copy method:" + type);
        }
    }
}
