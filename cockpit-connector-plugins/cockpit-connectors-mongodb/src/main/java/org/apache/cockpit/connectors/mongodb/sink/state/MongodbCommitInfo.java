package org.apache.cockpit.connectors.mongodb.sink.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
public class MongodbCommitInfo implements Serializable {
    private static final long serialVersionUID = -8437379022903705979L;
    List<DocumentBulk> documentBulks;
}
