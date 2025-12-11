package org.apache.cockpit.connectors.clickhouse.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class CKFileCommitInfo implements Serializable {

    private static final long serialVersionUID = 5967888460683065639L;
    private Map<Shard, List<String>> detachedFiles;
}
