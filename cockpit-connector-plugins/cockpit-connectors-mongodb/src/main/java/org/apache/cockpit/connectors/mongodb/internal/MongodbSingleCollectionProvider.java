package org.apache.cockpit.connectors.mongodb.internal;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import com.google.common.base.Preconditions;
import org.bson.BsonDocument;

@Slf4j
public class MongodbSingleCollectionProvider implements MongodbClientProvider {

    private final String connectionString;

    private final String defaultDatabase;

    private final String defaultCollection;

    private MongoClient client;

    private MongoDatabase database;

    private MongoCollection<BsonDocument> collection;

    public MongodbSingleCollectionProvider(
            String connectionString, String defaultDatabase, String defaultCollection) {
        Preconditions.checkNotNull(connectionString);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.connectionString = connectionString;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
    }

    @Override
    public MongoClient getClient() {
        synchronized (this) {
            if (client == null) {
                client = MongoClients.create(connectionString);
            }
        }
        return client;
    }

    @Override
    public MongoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (database == null) {
                database = getClient().getDatabase(defaultDatabase);
            }
        }
        return database;
    }

    @Override
    public MongoCollection<BsonDocument> getDefaultCollection() {
        synchronized (this) {
            if (collection == null) {
                collection =
                        getDefaultDatabase().getCollection(defaultCollection, BsonDocument.class);
            }
        }
        return collection;
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Failed to close Mongo client", e);
        } finally {
            client = null;
        }
    }
}
