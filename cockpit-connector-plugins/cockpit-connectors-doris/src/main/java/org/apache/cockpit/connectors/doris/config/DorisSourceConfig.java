package org.apache.cockpit.connectors.doris.config;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.List;

import static org.apache.cockpit.connectors.doris.config.DorisBaseOptions.*;
import static org.apache.cockpit.connectors.doris.config.DorisSourceOptions.*;


@Data
@SuperBuilder
public class DorisSourceConfig implements Serializable {

    private String frontends;
    private Integer queryPort;
    private String username;
    private String password;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private Boolean deserializeArrowAsync;
    private int deserializeQueueSize;
    private boolean useOldApi;
    private List<DorisTableConfig> tableConfigList;

    public static DorisSourceConfig of(ReadonlyConfig config) {
        DorisSourceConfigBuilder<?, ?> builder = DorisSourceConfig.builder();
        builder.tableConfigList(DorisTableConfig.of(config));
        builder.frontends(config.get(FENODES));
        builder.queryPort(config.get(QUERY_PORT));
        builder.username(config.get(USERNAME));
        builder.password(config.get(PASSWORD));
        builder.requestConnectTimeoutMs(config.get(DORIS_REQUEST_CONNECT_TIMEOUT_MS));
        builder.requestReadTimeoutMs(config.get(DORIS_REQUEST_READ_TIMEOUT_MS));
        builder.requestQueryTimeoutS(config.get(DORIS_REQUEST_QUERY_TIMEOUT_S));
        builder.requestRetries(config.get(DORIS_REQUEST_RETRIES));
        builder.deserializeArrowAsync(config.get(DORIS_DESERIALIZE_ARROW_ASYNC));
        builder.deserializeQueueSize(config.get(DORIS_DESERIALIZE_QUEUE_SIZE));
        return builder.build();
    }
}
