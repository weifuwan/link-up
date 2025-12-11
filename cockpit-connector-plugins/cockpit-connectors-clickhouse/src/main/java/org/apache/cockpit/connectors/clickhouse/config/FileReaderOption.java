package org.apache.cockpit.connectors.clickhouse.config;

import lombok.Data;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.clickhouse.shard.ShardMetadata;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class FileReaderOption implements Serializable {

    private ShardMetadata shardMetadata;
    private Map<String, String> tableSchema;
    private List<String> fields;
    private String clickhouseLocalPath;
    private ClickhouseFileCopyMethod copyMethod;
    private boolean nodeFreePass;
    private Map<String, String> nodeUser;
    private Map<String, String> nodePassword;
    private SeaTunnelRowType seaTunnelRowType;
    private boolean compatibleMode;
    private String fileTempPath;
    private String fileFieldsDelimiter;
    private String keyPath;

    public FileReaderOption(
            ShardMetadata shardMetadata,
            Map<String, String> tableSchema,
            List<String> fields,
            String clickhouseLocalPath,
            ClickhouseFileCopyMethod copyMethod,
            Map<String, String> nodeUser,
            boolean nodeFreePass,
            Map<String, String> nodePassword,
            boolean compatibleMode,
            String fileTempPath,
            String fileFieldsDelimiter,
            String keyPath) {
        this.shardMetadata = shardMetadata;
        this.tableSchema = tableSchema;
        this.fields = fields;
        this.clickhouseLocalPath = clickhouseLocalPath;
        this.copyMethod = copyMethod;
        this.nodeUser = nodeUser;
        this.nodeFreePass = nodeFreePass;
        this.nodePassword = nodePassword;
        this.compatibleMode = compatibleMode;
        this.fileFieldsDelimiter = fileFieldsDelimiter;
        this.fileTempPath = fileTempPath;
        this.keyPath = keyPath;
    }
}
