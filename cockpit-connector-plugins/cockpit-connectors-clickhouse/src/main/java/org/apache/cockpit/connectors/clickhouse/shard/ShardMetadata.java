package org.apache.cockpit.connectors.clickhouse.shard;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class ShardMetadata implements Serializable {

    private static final long serialVersionUID = -1L;

    private String shardKey;
    private String shardKeyType;
    private String sortingKey;
    private String database;
    private String table;
    private String tableEngine;
    private boolean splitMode;
    private Shard defaultShard;
    private String username;
    private String password;

    public ShardMetadata(
            String shardKey,
            String shardKeyType,
            String sortingKey,
            String database,
            String table,
            String tableEngine,
            boolean splitMode,
            Shard defaultShard) {
        this(
                shardKey,
                shardKeyType,
                sortingKey,
                database,
                table,
                tableEngine,
                splitMode,
                defaultShard,
                null,
                null);
    }

    public ShardMetadata(
            String shardKey,
            String shardKeyType,
            String database,
            String table,
            String tableEngine,
            boolean splitMode,
            Shard defaultShard,
            String username,
            String password) {
        this(
                shardKey,
                shardKeyType,
                null,
                database,
                table,
                tableEngine,
                splitMode,
                defaultShard,
                username,
                password);
    }
}
