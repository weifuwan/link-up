package org.apache.cockpit.connectors.clickhouse.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;


import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class CKFileAggCommitInfo implements Serializable {

    private static final long serialVersionUID = 1815170158201953697L;
    private Map<Shard, List<String>> detachedFiles;
}
