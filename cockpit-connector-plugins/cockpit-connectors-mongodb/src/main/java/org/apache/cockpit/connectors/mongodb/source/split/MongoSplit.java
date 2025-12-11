package org.apache.cockpit.connectors.mongodb.source.split;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.bson.BsonDocument;

/** MongoSplit is composed a query and a start offset. */
@Getter
@AllArgsConstructor
public class MongoSplit implements SourceSplit {

    private static final long serialVersionUID = 6349181541535290370L;
    private final String splitId;

    private final BsonDocument query;

    private final BsonDocument projection;

    private final long startOffset;

    @Override
    public String splitId() {
        return splitId;
    }
}
