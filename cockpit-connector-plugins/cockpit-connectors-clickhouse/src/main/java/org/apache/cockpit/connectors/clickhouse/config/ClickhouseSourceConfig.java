package org.apache.cockpit.connectors.clickhouse.config;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder(builderClassName = "Builder")
@Slf4j
public class ClickhouseSourceConfig implements Serializable {

    private static final long serialVersionUID = -5139627460951339176L;

    private String host;
    private String username;
    private String password;
    private Map<String, String> clickhouseConfig;
    private String serverTimeZone;
    private List<ClickhouseTableConfig> tableconfigList;

    public static ClickhouseSourceConfig of(ReadonlyConfig config) {
        ClickhouseSourceConfig.Builder builder = ClickhouseSourceConfig.builder();
        builder.host(config.get(ClickhouseBaseOptions.HOST));
        builder.username(config.get(ClickhouseBaseOptions.USERNAME));
        builder.password(config.get(ClickhouseBaseOptions.PASSWORD));
        builder.clickhouseConfig(config.get(ClickhouseBaseOptions.CLICKHOUSE_CONFIG));
        builder.serverTimeZone(config.get(ClickhouseBaseOptions.SERVER_TIME_ZONE));

        builder.tableconfigList(ClickhouseTableConfig.of(config));

        return builder.build();
    }
}
