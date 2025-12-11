package org.apache.cockpit.connectors.mongodb.sink.state;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.bson.BsonDocument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DocumentBulk is buffered {@link BsonDocument} in memory, which would be written to MongoDB in a
 * single transaction. Due to execution efficiency, each DocumentBulk maybe be limited to a maximum
 * size, typically 1,000 documents. But for the transactional mode, the maximum size should not be
 * respected because all that data must be written in one transaction.
 */
@ToString
@EqualsAndHashCode
public class DocumentBulk implements Serializable {

    public static final int BUFFER_SIZE = 1024;
    private static final long serialVersionUID = 7203410284346755522L;

    private final List<BsonDocument> bufferedDocuments;

    public DocumentBulk() {
        bufferedDocuments = new ArrayList<>(BUFFER_SIZE);
    }

    public void add(BsonDocument document) {
        if (bufferedDocuments.size() == BUFFER_SIZE) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
    }

    public int size() {
        return bufferedDocuments.size();
    }

    public List<BsonDocument> getDocuments() {
        return bufferedDocuments;
    }
}
