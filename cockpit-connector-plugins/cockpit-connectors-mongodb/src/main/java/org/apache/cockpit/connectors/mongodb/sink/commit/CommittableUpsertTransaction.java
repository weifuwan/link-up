package org.apache.cockpit.connectors.mongodb.sink.commit;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class CommittableUpsertTransaction extends CommittableTransaction {

    private final String[] upsertKeys;
    private final UpdateOptions updateOptions = new UpdateOptions();
    private final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();

    public CommittableUpsertTransaction(
            MongoCollection<BsonDocument> collection,
            List<BsonDocument> documents,
            String[] upsertKeys) {
        super(collection, documents);
        this.upsertKeys = upsertKeys;
        updateOptions.upsert(true);
        bulkWriteOptions.ordered(true);
    }

    @Override
    public Integer execute() {
        List<UpdateOneModel<BsonDocument>> upserts = new ArrayList<>();
        for (BsonDocument document : bufferedDocuments) {
            List<Bson> filters = new ArrayList<>(upsertKeys.length);
            for (String upsertKey : upsertKeys) {
                Object o = document.get("$set").asDocument().get(upsertKey);
                Bson eq = Filters.eq(upsertKey, o);
                filters.add(eq);
            }
            Bson filter = Filters.and(filters);
            UpdateOneModel<BsonDocument> updateOneModel =
                    new UpdateOneModel<>(filter, document, updateOptions);
            upserts.add(updateOneModel);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(upserts, bulkWriteOptions);
        return bulkWriteResult.getUpserts().size() + bulkWriteResult.getInsertedCount();
    }
}
