package org.apache.cockpit.connectors.clickhouse.sink.client;

import com.clickhouse.client.ClickHouseRequest;
import lombok.Getter;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.shard.ShardMetadata;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.DistributedEngine;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

public class ShardRouter implements Serializable {

    private static final long serialVersionUID = -1L;

    private String shardTable;
    private String shardTableEngine;
    private final String table;
    private final String tableEngine;
    private int shardWeightCount;
    private final TreeMap<Integer, Shard> shards;
    private final String shardKey;
    private final String shardKeyType;
    @Getter private final String sortingKey;
    private final boolean splitMode;

    private static final XXHash64 HASH_INSTANCE = XXHashFactory.fastestInstance().hash64();
    private final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

    public ShardRouter(ClickhouseProxy proxy, ShardMetadata shardMetadata) {
        this.shards = new TreeMap<>();
        this.shardKey = shardMetadata.getShardKey();
        this.shardKeyType = shardMetadata.getShardKeyType();
        this.sortingKey = shardMetadata.getSortingKey();
        this.splitMode = shardMetadata.isSplitMode();
        this.table = shardMetadata.getTable();
        this.tableEngine = shardMetadata.getTableEngine();
        if (StringUtils.isNotEmpty(shardKey) && StringUtils.isEmpty(shardKeyType)) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.SHARD_KEY_NOT_FOUND,
                    "Shard key " + shardKey + " not found in table " + table);
        }
        ClickHouseRequest<?> connection = proxy.getClickhouseConnection();
        if (splitMode) {
            DistributedEngine localTable =
                    proxy.getClickhouseDistributedTable(
                            connection, shardMetadata.getDatabase(), table);
            this.shardTable = localTable.getTable();
            this.shardTableEngine = localTable.getTableEngine();
            List<Shard> shardList =
                    proxy.getClusterShardList(
                            connection,
                            localTable.getClusterName(),
                            localTable.getDatabase(),
                            shardMetadata.getDefaultShard().getNode().getPort(),
                            shardMetadata.getUsername(),
                            shardMetadata.getPassword(),
                            shardMetadata.getDefaultShard().getNode().getOptions());
            int weight = 0;
            for (Shard shard : shardList) {
                shards.put(weight, shard);
                weight += shard.getNode().getWeight();
            }
            shardWeightCount = weight;
        } else {
            shards.put(0, shardMetadata.getDefaultShard());
        }
    }

    public String getShardTable() {
        return splitMode ? shardTable : table;
    }

    public String getShardTableEngine() {
        return splitMode ? shardTableEngine : tableEngine;
    }

    public Shard getShard(Object shardValue) {
        if (!splitMode) {
            return shards.firstEntry().getValue();
        }
        if (StringUtils.isEmpty(shardKey) || shardValue == null) {
            return shards.lowerEntry(threadLocalRandom.nextInt(shardWeightCount) + 1).getValue();
        }
        int offset =
                (int)
                        ((HASH_INSTANCE.hash(
                                                ByteBuffer.wrap(
                                                        shardValue
                                                                .toString()
                                                                .getBytes(StandardCharsets.UTF_8)),
                                                0)
                                        & Long.MAX_VALUE)
                                % shardWeightCount);
        return shards.lowerEntry(offset + 1).getValue();
    }

    public TreeMap<Integer, Shard> getShards() {
        return shards;
    }
}
