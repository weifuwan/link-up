package org.apache.cockpit.connectors.mongodb.sink;

import org.apache.cockpit.connectors.mongodb.serde.SerializableFunction;
import org.bson.BsonDocument;

import java.util.Arrays;
import java.util.stream.Collectors;

public class MongoKeyExtractor implements SerializableFunction<BsonDocument, BsonDocument> {

    private static final long serialVersionUID = 1L;

    private final String[] primaryKey;

    public MongoKeyExtractor(MongodbWriterOptions options) {
        primaryKey = options.getPrimaryKey();
    }

    @Override
    public BsonDocument apply(BsonDocument bsonDocument) {
        return Arrays.stream(primaryKey)
                .filter(bsonDocument::containsKey)
                .collect(
                        Collectors.toMap(
                                key -> key, bsonDocument::get, (v1, v2) -> v1, BsonDocument::new));
    }
}
