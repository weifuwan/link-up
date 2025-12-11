package org.apache.cockpit.connectors.clickhouse.source.split;


import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.source.ClickhouseSourceTable;

import java.util.List;

public interface Splitter {

    List<ClickhouseSourceSplit> generateSplits(
            ClickhouseSourceTable clickhouseSourceTable, List<Shard> clusterShardList);

    String createSplitId(TablePath tablePath, Shard shard, int index);

    void close();

    static Splitter createSplitter(ClickhouseSourceTable clickhouseSourceTable) {
        if (clickhouseSourceTable.isSqlStrategyRead()) {
            return new SqlStrategySplitter();
        } else {
            return new PartStrategySplitter();
        }
    }
}
