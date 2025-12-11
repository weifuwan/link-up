package org.apache.cockpit.connectors.mongodb.sink.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class MongodbAggregatedCommitInfo implements Serializable {
    private static final long serialVersionUID = 2347040237946273020L;
    List<MongodbCommitInfo> commitInfos;
}
