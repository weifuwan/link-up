package org.apache.cockpit.connectors.clickhouse.state;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.cockpit.connectors.clickhouse.source.split.ClickhouseSourceSplit;


import java.io.Serializable;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Getter
public class ClickhouseSourceState implements Serializable {
    private static final long serialVersionUID = 286679054882099834L;
    private boolean shouldEnumerate;
    private Map<Integer, List<ClickhouseSourceSplit>> pendingSplit;
}
