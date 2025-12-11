package org.apache.cockpit.connectors.elasticsearch.source;

import com.google.auto.service.AutoService;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;

import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchBaseOptions.*;
import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSourceOptions.*;


@AutoService(Factory.class)
public class ElasticsearchSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DbType.ELASTICSEARCH.getCode();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS)
                .optional(
                        INDEX,
                        INDEX_LIST,
                        USERNAME,
                        PASSWORD,
                        SCROLL_TIME,
                        SCROLL_SIZE,
                        QUERY,
                        PIT_KEEP_ALIVE,
                        PIT_BATCH_SIZE,
                        SEARCH_API_TYPE,
                        SEARCH_TYPE,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT>) new ElasticsearchSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ElasticsearchSource.class;
    }
}
