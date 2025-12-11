package org.apache.cockpit.connectors.mongodb.sink;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSink;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactory;
import org.apache.cockpit.connectors.api.factory.TableSinkFactoryContext;
import org.apache.cockpit.connectors.mongodb.config.MongodbConfig;

import static org.apache.cockpit.connectors.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;


@AutoService(Factory.class)
public class MongodbSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }



    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MongodbConfig.URI, MongodbConfig.DATABASE, MongodbConfig.COLLECTION)
                .optional(
                        MongodbConfig.BUFFER_FLUSH_INTERVAL,
                        MongodbConfig.BUFFER_FLUSH_MAX_ROWS,
                        MongodbConfig.RETRY_MAX,
                        MongodbConfig.RETRY_INTERVAL,
                        MongodbConfig.UPSERT_ENABLE,
                        MongodbConfig.PRIMARY_KEY)
                .build();
    }
}
