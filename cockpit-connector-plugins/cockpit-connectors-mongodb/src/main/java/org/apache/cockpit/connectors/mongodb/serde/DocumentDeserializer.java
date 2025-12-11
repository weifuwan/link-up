package org.apache.cockpit.connectors.mongodb.serde;

import org.bson.BsonDocument;

import java.io.Serializable;

public interface DocumentDeserializer<T> extends Serializable {

    T deserialize(BsonDocument bsonDocument);
}
