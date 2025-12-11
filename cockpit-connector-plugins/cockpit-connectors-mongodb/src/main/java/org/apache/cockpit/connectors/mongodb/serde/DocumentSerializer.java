package org.apache.cockpit.connectors.mongodb.serde;

import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;

import java.io.Serializable;

public interface DocumentSerializer<T> extends Serializable {

    WriteModel<BsonDocument> serializeToWriteModel(T object);
}
