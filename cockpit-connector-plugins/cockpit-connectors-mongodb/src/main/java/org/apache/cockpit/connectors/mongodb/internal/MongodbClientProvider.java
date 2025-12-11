package org.apache.cockpit.connectors.mongodb.internal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;

import java.io.Serializable;

/** Provided for initiate and recreate {@link MongoClient}. */
public interface MongodbClientProvider extends Serializable {

    /**
     * Create one or get the current {@link MongoClient}.
     *
     * @return Current {@link MongoClient}.
     */
    MongoClient getClient();

    /**
     * Get the default database.
     *
     * @return Current {@link MongoDatabase}.
     */
    MongoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     *
     * @return Current {@link MongoCollection}.
     */
    MongoCollection<BsonDocument> getDefaultCollection();

    /** Close the underlying MongoDB connection. */
    void close();
}
