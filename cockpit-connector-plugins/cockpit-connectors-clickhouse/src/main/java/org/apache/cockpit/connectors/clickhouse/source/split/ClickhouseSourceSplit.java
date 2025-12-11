package org.apache.cockpit.connectors.clickhouse.source.split;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.source.ClickhousePart;

import java.util.List;

@Data
@AllArgsConstructor
public class ClickhouseSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 8626697814676246066L;

    private final TablePath tablePath;
    private final TablePath configTablePath;
    private final List<ClickhousePart> parts;
    private final Shard shard;
    private final String splitQuery;
    @Setter private int sqlOffset;

    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "ClickhouseSourceSplit{"
                + "tablePath='"
                + tablePath
                + "'"
                + ", configTablePath='"
                + configTablePath
                + "'"
                + ", parts='"
                + parts
                + "'"
                + ", shard='"
                + shard
                + "'"
                + ", splitQuery='"
                + splitQuery
                + "'"
                + ", sqlOffset="
                + sqlOffset
                + ", splitId='"
                + splitId
                + "'"
                + "}";
    }
}
