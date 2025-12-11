package org.apache.cockpit.connectors.clickhouse.config;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.clickhouse.shard.ShardMetadata;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

@Builder
@Getter
public class ReaderOption implements Serializable {

    private ShardMetadata shardMetadata;
    private String[] primaryKeys;
    private boolean allowExperimentalLightweightDelete;
    private boolean supportUpsert;
    private String tableEngine;
    private Map<String, String> tableSchema;
    @Setter private SeaTunnelRowType seaTunnelRowType;
    private Properties properties;
    private int bulkSize;
}
