package org.apache.cockpit.connectors.doris.config;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

import static org.apache.cockpit.connectors.doris.config.DorisBaseOptions.*;
import static org.apache.cockpit.connectors.doris.config.DorisSinkOptions.*;


@Setter
@Getter
@ToString
public class DorisSinkConfig implements Serializable {

    // common option
    private String frontends;
    private String database;
    private String table;
    private String username;
    private String password;
    private Integer queryPort;
    private int batchSize;

    // sink option
    private Boolean enable2PC;
    private Boolean enableDelete;
    private String labelPrefix;
    private Integer checkInterval;
    private Integer maxRetries;
    private Integer bufferSize;
    private Integer bufferCount;
    private Properties streamLoadProps;
    private boolean needsUnsupportedTypeCasting;
    private boolean caseSensitive;

    // create table option
    private String createTableTemplate;

    public static DorisSinkConfig of(Config pluginConfig) {
        return of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    public static DorisSinkConfig of(ReadonlyConfig config) {

        DorisSinkConfig dorisSinkConfig = new DorisSinkConfig();

        // common option
        dorisSinkConfig.setFrontends(config.get(FENODES));
        dorisSinkConfig.setUsername(config.get(USERNAME));
        dorisSinkConfig.setPassword(config.get(PASSWORD));
        dorisSinkConfig.setQueryPort(config.get(QUERY_PORT));
        dorisSinkConfig.setStreamLoadProps(parseStreamLoadProperties(config));
        dorisSinkConfig.setDatabase(config.get(DATABASE));
        dorisSinkConfig.setTable(config.get(TABLE));
        dorisSinkConfig.setBatchSize(config.get(DORIS_BATCH_SIZE));

        // sink option
        dorisSinkConfig.setEnable2PC(config.get(SINK_ENABLE_2PC));
        dorisSinkConfig.setLabelPrefix(config.get(SINK_LABEL_PREFIX));
        dorisSinkConfig.setCheckInterval(config.get(SINK_CHECK_INTERVAL));
        dorisSinkConfig.setMaxRetries(config.get(SINK_MAX_RETRIES));
        dorisSinkConfig.setBufferSize(config.get(SINK_BUFFER_SIZE));
        dorisSinkConfig.setBufferCount(config.get(SINK_BUFFER_COUNT));
        dorisSinkConfig.setEnableDelete(config.get(SINK_ENABLE_DELETE));
        dorisSinkConfig.setNeedsUnsupportedTypeCasting(config.get(NEEDS_UNSUPPORTED_TYPE_CASTING));
        dorisSinkConfig.setCaseSensitive(config.get(CASE_SENSITIVE));
        // create table option
        dorisSinkConfig.setCreateTableTemplate(config.get(SAVE_MODE_CREATE_TEMPLATE));

        return dorisSinkConfig;
    }

    private static Properties parseStreamLoadProperties(ReadonlyConfig config) {
        Properties streamLoadProps = new Properties();
        if (config.getOptional(DORIS_SINK_CONFIG_PREFIX).isPresent()) {
            Map<String, String> map = config.getOptional(DORIS_SINK_CONFIG_PREFIX).get();
            map.forEach(
                    (key, value) -> {
                        streamLoadProps.put(key.toLowerCase(), value);
                    });
        }
        return streamLoadProps;
    }
}
