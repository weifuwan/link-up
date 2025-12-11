package org.apache.cockpit.connectors.mongodb.serde;

import com.mongodb.client.model.*;
import org.apache.cockpit.connectors.api.type.RowKind;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.mongodb.exception.MongodbConnectorException;
import org.apache.cockpit.connectors.mongodb.sink.MongodbWriterOptions;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;


public class RowDataDocumentSerializer implements DocumentSerializer<SeaTunnelRow> {

    private final RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter;
    private final boolean isUpsertEnable;
    private final Function<BsonDocument, BsonDocument> filterConditions;

    private final Map<RowKind, WriteModelSupplier> writeModelSuppliers;

    public RowDataDocumentSerializer(
            RowDataToBsonConverters.RowDataToBsonConverter rowDataToBsonConverter,
            MongodbWriterOptions options,
            Function<BsonDocument, BsonDocument> filterConditions) {
        this.rowDataToBsonConverter = rowDataToBsonConverter;
        this.isUpsertEnable = options.isUpsertEnable();
        this.filterConditions = filterConditions;

        writeModelSuppliers = createWriteModelSuppliers();
    }

    public WriteModel<BsonDocument> serializeToWriteModel(SeaTunnelRow row) {
        WriteModelSupplier writeModelSupplier = writeModelSuppliers.get(row.getRowKind());
        if (writeModelSupplier == null) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Unsupported message kind: " + row.getRowKind());
        }
        return writeModelSupplier.get(row);
    }

    private Map<RowKind, WriteModelSupplier> createWriteModelSuppliers() {
        Map<RowKind, WriteModelSupplier> writeModelSuppliers = new HashMap<>();

        WriteModelSupplier upsertSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    bsonDocument.remove("_id");
                    BsonDocument update = new BsonDocument("$set", bsonDocument);
                    return new UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));
                };

        WriteModelSupplier updateSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    bsonDocument.remove("_id");
                    BsonDocument update = new BsonDocument("$set", bsonDocument);
                    return new UpdateOneModel<>(filter, update);
                };

        WriteModelSupplier insertSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    return new InsertOneModel<>(bsonDocument);
                };

        WriteModelSupplier deleteSupplier =
                row -> {
                    final BsonDocument bsonDocument = rowDataToBsonConverter.convert(row);
                    Bson filter = generateFilter(filterConditions.apply(bsonDocument));
                    return new DeleteOneModel<>(filter);
                };

        writeModelSuppliers.put(RowKind.INSERT, isUpsertEnable ? upsertSupplier : insertSupplier);
        writeModelSuppliers.put(
                RowKind.UPDATE_AFTER, isUpsertEnable ? upsertSupplier : updateSupplier);
        writeModelSuppliers.put(RowKind.DELETE, deleteSupplier);

        return writeModelSuppliers;
    }

    public static Bson generateFilter(BsonDocument filterConditions) {
        List<Bson> filters =
                filterConditions.entrySet().stream()
                        .map(entry -> Filters.eq(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

        return Filters.and(filters);
    }

    private interface WriteModelSupplier {
        WriteModel<BsonDocument> get(SeaTunnelRow row);
    }
}
