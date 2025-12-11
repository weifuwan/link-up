package org.apache.cockpit.connectors.mongodb.source;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.mongodb.config.MongodbConfig;
import org.apache.cockpit.connectors.mongodb.internal.MongodbClientProvider;
import org.apache.cockpit.connectors.mongodb.internal.MongodbCollectionProvider;
import org.apache.cockpit.connectors.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.cockpit.connectors.mongodb.source.config.MongodbReadOptions;
import org.apache.cockpit.connectors.mongodb.source.reader.MongodbReader;
import org.apache.cockpit.connectors.mongodb.source.split.MongoSplit;
import org.apache.cockpit.connectors.mongodb.source.split.MongoSplitStrategy;
import org.apache.cockpit.connectors.mongodb.source.split.SamplingSplitStrategy;
import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.cockpit.connectors.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;


public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit> {

    private static final long serialVersionUID = 1L;

    private final CatalogTable catalogTable;
    private final ReadonlyConfig options;

    public MongodbSource(CatalogTable catalogTable, ReadonlyConfig options) {
        this.catalogTable = catalogTable;
        this.options = options;
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext) {
        return new MongodbReader(
                readerContext,
                crateClientProvider(options),
                createDeserializer(options, catalogTable.getSeaTunnelRowType()),
                createMongodbReadOptions(options),
                catalogTable);
    }


    private MongodbClientProvider crateClientProvider(ReadonlyConfig config) {
        return MongodbCollectionProvider.builder()
                .connectionString(config.get(MongodbConfig.URI))
                .database(config.get(MongodbConfig.DATABASE))
                .collection(config.get(MongodbConfig.COLLECTION))
                .build();
    }

    private DocumentRowDataDeserializer createDeserializer(
            ReadonlyConfig config, SeaTunnelRowType rowType) {
        return new DocumentRowDataDeserializer(
                rowType.getFieldNames(), rowType, config.get(MongodbConfig.FLAT_SYNC_STRING));
    }

    private MongoSplitStrategy createSplitStrategy(
            ReadonlyConfig config, MongodbClientProvider clientProvider) {
        SamplingSplitStrategy.Builder splitStrategyBuilder = SamplingSplitStrategy.builder();
        splitStrategyBuilder.setSplitKey(config.get(MongodbConfig.SPLIT_KEY));
        splitStrategyBuilder.setSizePerSplit(config.get(MongodbConfig.SPLIT_SIZE));
        config.getOptional(MongodbConfig.MATCH_QUERY)
                .ifPresent(s -> splitStrategyBuilder.setMatchQuery(BsonDocument.parse(s)));
        config.getOptional(MongodbConfig.PROJECTION)
                .ifPresent(s -> splitStrategyBuilder.setProjection(BsonDocument.parse(s)));
        return splitStrategyBuilder.setClientProvider(clientProvider).build();
    }

    private MongodbReadOptions createMongodbReadOptions(ReadonlyConfig config) {
        MongodbReadOptions.MongoReadOptionsBuilder mongoReadOptionsBuilder =
                MongodbReadOptions.builder();
        mongoReadOptionsBuilder.setMaxTimeMS(config.get(MongodbConfig.MAX_TIME_MIN));
        mongoReadOptionsBuilder.setFetchSize(config.get(MongodbConfig.FETCH_SIZE));
        mongoReadOptionsBuilder.setNoCursorTimeout(config.get(MongodbConfig.CURSOR_NO_TIMEOUT));
        return mongoReadOptionsBuilder.build();
    }
}
