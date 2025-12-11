package org.apache.cockpit.connectors.mongodb.source.reader;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.mongodb.internal.MongodbClientProvider;
import org.apache.cockpit.connectors.mongodb.serde.DocumentDeserializer;
import org.apache.cockpit.connectors.mongodb.source.config.MongodbReadOptions;
import org.apache.cockpit.connectors.mongodb.source.split.MongoSplit;
import org.bson.BsonDocument;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * MongoReader reads MongoDB by splits (queries).
 */
@Slf4j
public class MongodbReader implements SourceReader<SeaTunnelRow, MongoSplit> {

    private final Queue<MongoSplit> pendingSplits;

    private final DocumentDeserializer<SeaTunnelRow> deserializer;

    private final SourceReader.Context context;

    private final MongodbClientProvider clientProvider;

    private MongoCursor<BsonDocument> cursor;

    private final MongodbReadOptions readOptions;

    private volatile boolean noMoreSplit;

    private final CatalogTable catalogTable;

    public MongodbReader(
            SourceReader.Context context,
            MongodbClientProvider clientProvider,
            DocumentDeserializer<SeaTunnelRow> deserializer,
            MongodbReadOptions mongodbReadOptions,
            CatalogTable catalogTable) {
        this.deserializer = deserializer;
        this.context = context;
        this.clientProvider = clientProvider;
        pendingSplits = new ConcurrentLinkedDeque<>();
        this.readOptions = mongodbReadOptions;
        this.catalogTable = catalogTable;
    }

    @Override
    public void open() {
        if (cursor != null) {
            cursor.close();
        }
    }

    @Override
    public CatalogTable getJdbcSourceTables() {
        return catalogTable;
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
        if (clientProvider != null) {
            clientProvider.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        synchronized (output.getCheckpointLock()) {
            MongoSplit currentSplit = pendingSplits.poll();
            if (currentSplit != null) {
                if (cursor != null) {
                    // current split is in-progress
                    return;
                }
                log.info("Prepared to read split {}", currentSplit.splitId());
                try {
                    getCursor(currentSplit);
                    cursorToStream().map(deserializer::deserialize).forEach(item -> {
                        try {
                            output.collect(item);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                } finally {
                    closeCurrentSplit();
                }
            }
            if (noMoreSplit && pendingSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded mongodb source");
                context.signalNoMoreElement();
            }
        }
    }

    private void getCursor(MongoSplit split) {
        cursor =
                clientProvider
                        .getDefaultCollection()
                        .find(split.getQuery())
                        .projection(split.getProjection())
                        .batchSize(readOptions.getFetchSize())
                        .noCursorTimeout(readOptions.isNoCursorTimeout())
                        .maxTime(readOptions.getMaxTimeMS(), TimeUnit.MINUTES)
                        .iterator();
    }

    private Stream<BsonDocument> cursorToStream() {
        Iterable<BsonDocument> iterable = () -> cursor;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private void closeCurrentSplit() {
        Preconditions.checkNotNull(cursor);
        cursor.close();
        cursor = null;
    }
}
