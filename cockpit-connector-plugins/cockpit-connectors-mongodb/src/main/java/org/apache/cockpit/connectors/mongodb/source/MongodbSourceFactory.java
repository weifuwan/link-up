package org.apache.cockpit.connectors.mongodb.source;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.CatalogTableUtil;
import org.apache.cockpit.connectors.api.config.ConnectorCommonOptions;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.mongodb.config.MongodbConfig;
import org.apache.cockpit.connectors.mongodb.source.split.MongoSplit;

import java.util.ArrayList;

import static org.apache.cockpit.connectors.mongodb.config.MongodbConfig.*;


@AutoService(Factory.class)
public class MongodbSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongodbConfig.URI,
                        MongodbConfig.DATABASE,
                        MongodbConfig.COLLECTION,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        MongodbConfig.PROJECTION,
                        MongodbConfig.MATCH_QUERY,
                        MongodbConfig.SPLIT_SIZE,
                        MongodbConfig.SPLIT_KEY,
                        MongodbConfig.CURSOR_NO_TIMEOUT,
                        MongodbConfig.FETCH_SIZE,
                        MongodbConfig.MAX_TIME_MIN)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource<SeaTunnelRow, MongoSplit>>
            getSourceClass() {
        return MongodbSource.class;
    }

    @Override
    public TableSource<SeaTunnelRow, MongoSplit> createSource(
            TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig options = context.getOptions();
            CatalogTable table;
            if (options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                table = CatalogTableUtil.buildWithConfig(options);
            } else {
                table = CatalogTableUtil.buildSimpleTextTable();
            }
            return new MongodbSource(table, options);
        };
    }
}
