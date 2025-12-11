package org.apache.cockpit.connectors.mongodb.sink;

import com.google.auto.service.AutoService;
import com.typesafe.config.Config;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.api.util.PrepareFailException;
import org.apache.cockpit.connectors.mongodb.config.MongodbConfig;
import org.apache.cockpit.connectors.mongodb.serde.RowDataDocumentSerializer;
import org.apache.cockpit.connectors.mongodb.serde.RowDataToBsonConverters;

import java.util.List;

import static org.apache.cockpit.connectors.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;


@AutoService(SeaTunnelSink.class)
public class MongodbSink
        implements SeaTunnelSink<SeaTunnelRow> {

    private MongodbWriterOptions options;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.hasPath(MongodbConfig.URI.key())
                && pluginConfig.hasPath(MongodbConfig.DATABASE.key())
                && pluginConfig.hasPath(MongodbConfig.COLLECTION.key())) {
            String connection = pluginConfig.getString(MongodbConfig.URI.key());
            String database = pluginConfig.getString(MongodbConfig.DATABASE.key());
            String collection = pluginConfig.getString(MongodbConfig.COLLECTION.key());
            MongodbWriterOptions.Builder builder =
                    MongodbWriterOptions.builder()
                            .withConnectString(connection)
                            .withDatabase(database)
                            .withCollection(collection);
            if (pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key())) {
                builder.withFlushSize(
                        pluginConfig.getInt(MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_INTERVAL.key())) {
                builder.withBatchIntervalMs(
                        pluginConfig.getLong(MongodbConfig.BUFFER_FLUSH_INTERVAL.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.PRIMARY_KEY.key())) {
                builder.withPrimaryKey(
                        pluginConfig
                                .getStringList(MongodbConfig.PRIMARY_KEY.key())
                                .toArray(new String[0]));
            }
            List<String> fallbackKeys = MongodbConfig.PRIMARY_KEY.getFallbackKeys();
            fallbackKeys.forEach(
                    key -> {
                        if (pluginConfig.hasPath(key)) {
                            builder.withPrimaryKey(
                                    pluginConfig.getStringList(key).toArray(new String[0]));
                        }
                    });
            if (pluginConfig.hasPath(MongodbConfig.UPSERT_ENABLE.key())) {
                builder.withUpsertEnable(
                        pluginConfig.getBoolean(MongodbConfig.UPSERT_ENABLE.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.RETRY_MAX.key())) {
                builder.withRetryMax(pluginConfig.getInt(MongodbConfig.RETRY_MAX.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.RETRY_INTERVAL.key())) {
                builder.withRetryInterval(pluginConfig.getLong(MongodbConfig.RETRY_INTERVAL.key()));
            }

            if (pluginConfig.hasPath(MongodbConfig.TRANSACTION.key())) {
                builder.withTransaction(pluginConfig.getBoolean(MongodbConfig.TRANSACTION.key()));
            }
            this.options = builder.build();
        }
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(
            SinkWriter.Context context) {
        return new MongodbWriter(
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(seaTunnelRowType),
                        options,
                        new MongoKeyExtractor(options)),
                options,
                context);
    }

}
