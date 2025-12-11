package org.apache.cockpit.connectors.starrocks.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
@Setter
@AllArgsConstructor
public class StarRocksSourceState implements Serializable {
    private static final long serialVersionUID = -147928488869915694L;
    private Map<Integer, List<StarRocksSourceSplit>> pendingSplit;
    private final ConcurrentLinkedQueue<String> pendingTables;
}
