package org.apache.cockpit.connectors.starrocks.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.starrocks.client.source.model.QueryPartition;

@AllArgsConstructor
@Getter
@Setter
public class StarRocksSourceSplit implements SourceSplit {
    private final QueryPartition partition;
    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }
}
