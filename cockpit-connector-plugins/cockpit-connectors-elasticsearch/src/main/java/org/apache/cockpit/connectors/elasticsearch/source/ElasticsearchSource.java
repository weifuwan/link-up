package org.apache.cockpit.connectors.elasticsearch.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.config.ConnectorCommonOptions;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.source.Boundedness;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.elasticsearch.catalog.ElasticSearchTypeConverter;
import org.apache.cockpit.connectors.elasticsearch.client.EsRestClient;
import org.apache.cockpit.connectors.elasticsearch.client.EsType;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchConfig;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSourceOptions;
import org.apache.cockpit.connectors.elasticsearch.config.SearchApiTypeEnum;
import org.apache.cockpit.connectors.elasticsearch.config.SearchTypeEnum;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSourceOptions.*;


@Slf4j
public class ElasticsearchSource
        implements SeaTunnelSource<SeaTunnelRow, ElasticsearchSourceSplit> {

    private final List<ElasticsearchConfig> elasticsearchConfigList;
    private final ReadonlyConfig connectionConfig;

    public ElasticsearchSource(ReadonlyConfig config) {
        this.connectionConfig = config;
        boolean multiSource = config.getOptional(ElasticsearchSourceOptions.INDEX_LIST).isPresent();
        boolean singleSource = config.getOptional(ElasticsearchSourceOptions.INDEX).isPresent();

        boolean sqlQuery = config.getOptional(SQL_QUERY).isPresent();

        if (SearchTypeEnum.SQL.equals(config.get(SEARCH_TYPE)) && !sqlQuery) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_02,
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_02.getDescription());
        }

        if (multiSource && singleSource) {
            log.warn(
                    "Elasticsearch Source config warn: when both 'index' and 'index_list' are present in the configuration, only the 'index_list' configuration will take effect");
        }
        if (!multiSource && !singleSource) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_01,
                    ElasticsearchConnectorErrorCode.SOURCE_CONFIG_ERROR_01.getDescription());
        }
        if (multiSource) {
            this.elasticsearchConfigList = createMultiSource(config);
        } else {
            this.elasticsearchConfigList =
                    Collections.singletonList(parseOneIndexQueryConfig(config));
        }
    }

    private List<ElasticsearchConfig> createMultiSource(ReadonlyConfig config) {
        List<Map<String, Object>> configMaps = config.get(ElasticsearchSourceOptions.INDEX_LIST);
        List<ReadonlyConfig> configList =
                configMaps.stream().map(ReadonlyConfig::fromMap).collect(Collectors.toList());
        List<ElasticsearchConfig> elasticsearchConfigList = new ArrayList<>(configList.size());
        for (ReadonlyConfig readonlyConfig : configList) {
            ElasticsearchConfig elasticsearchConfig = parseOneIndexQueryConfig(readonlyConfig);
            elasticsearchConfigList.add(elasticsearchConfig);
        }
        return elasticsearchConfigList;
    }

    private ElasticsearchConfig parseOneIndexQueryConfig(ReadonlyConfig readonlyConfig) {

        Map<String, Object> query = readonlyConfig.get(ElasticsearchSourceOptions.QUERY);
        String index = readonlyConfig.get(ElasticsearchSourceOptions.INDEX);

        CatalogTable catalogTable;
        List<String> source;
        Map<String, String> arrayColumn;

        if (readonlyConfig.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            // todo: We need to remove the schema in ES.
            log.warn(
                    "The schema config in ElasticSearch source/sink is deprecated, please use source config instead!");
            catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
            source = Arrays.asList(catalogTable.getSeaTunnelRowType().getFieldNames());
        } else {
            source = readonlyConfig.get(ElasticsearchSourceOptions.SOURCE);
            arrayColumn = readonlyConfig.get(ElasticsearchSourceOptions.ARRAY_COLUMN);
            Map<String, BasicTypeDefine<EsType>> esFieldType;
            if (SearchTypeEnum.SQL.equals(readonlyConfig.get(SEARCH_TYPE))) {
                esFieldType = getSqlFieldTypeMapping(readonlyConfig.get(SQL_QUERY), source);
            } else {
                esFieldType = getFieldTypeMapping(index, source);
            }

            if (CollectionUtils.isEmpty(source)) {
                source = new ArrayList<>(esFieldType.keySet());
            }
            SeaTunnelDataType[] fieldTypes = getSeaTunnelDataType(esFieldType, source);
            TableSchema.Builder builder = TableSchema.builder();

            for (int i = 0; i < source.size(); i++) {
                String key = source.get(i);
                String sourceType = esFieldType.get(key).getDataType();
                if (arrayColumn.containsKey(key)) {
                    String value = arrayColumn.get(key);
                    SeaTunnelDataType<?> dataType =
                            SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(key, value);
                    builder.column(
                            PhysicalColumn.of(
                                    key, dataType, 0L, true, null, null, sourceType, null));
                    continue;
                }

                builder.column(
                        PhysicalColumn.of(
                                source.get(i),
                                fieldTypes[i],
                                0L,
                                true,
                                null,
                                null,
                                sourceType,
                                null));
            }
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of("elasticsearch", null, index),
                            builder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        }
        SearchTypeEnum searchType = readonlyConfig.get(SEARCH_TYPE);
        SearchApiTypeEnum searchApiType = readonlyConfig.get(SEARCH_API_TYPE);
        String sqlQuery = readonlyConfig.get(SQL_QUERY);
        String scrollTime = readonlyConfig.get(ElasticsearchSourceOptions.SCROLL_TIME);
        int scrollSize = readonlyConfig.get(ElasticsearchSourceOptions.SCROLL_SIZE);

        long pitKeepAlive = readonlyConfig.get(ElasticsearchSourceOptions.PIT_KEEP_ALIVE);
        int pitBatchSize = readonlyConfig.get(ElasticsearchSourceOptions.PIT_BATCH_SIZE);

        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setSource(source);
        elasticsearchConfig.setCatalogTable(catalogTable);
        elasticsearchConfig.setQuery(query);
        elasticsearchConfig.setScrollTime(scrollTime);
        elasticsearchConfig.setScrollSize(scrollSize);
        elasticsearchConfig.setIndex(index);
        elasticsearchConfig.setCatalogTable(catalogTable);
        elasticsearchConfig.setSqlQuery(sqlQuery);
        elasticsearchConfig.setSearchType(searchType);
        elasticsearchConfig.setSearchApiType(searchApiType);

        elasticsearchConfig.setPitKeepAlive(pitKeepAlive);
        elasticsearchConfig.setPitBatchSize(pitBatchSize);
        return elasticsearchConfig;
    }

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return elasticsearchConfigList.stream()
                .map(ElasticsearchConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ElasticsearchSourceReader(readerContext, connectionConfig);
    }



    public static SeaTunnelDataType[] getSeaTunnelDataType(
            Map<String, BasicTypeDefine<EsType>> esFieldType, List<String> source) {
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType[source.size()];
        for (int i = 0; i < source.size(); i++) {
            BasicTypeDefine<EsType> esType = esFieldType.get(source.get(i));
            SeaTunnelDataType<?> seaTunnelDataType =
                    ElasticSearchTypeConverter.INSTANCE.convert(esType).getDataType();
            fieldTypes[i] = seaTunnelDataType;
        }
        return fieldTypes;
    }

    private Map<String, BasicTypeDefine<EsType>> getSqlFieldTypeMapping(
            String query, List<String> source) {
        // EsRestClient#getFieldTypeMapping may throw runtime exception
        // so here we use try-resources-finally to close the resource
        try (EsRestClient esRestClient = EsRestClient.createInstance(connectionConfig)) {
            return esRestClient.getSqlMapping(query, source);
        }
    }

    private Map<String, BasicTypeDefine<EsType>> getFieldTypeMapping(
            String index, List<String> source) {
        // EsRestClient#getFieldTypeMapping may throw runtime exception
        // so here we use try-resources-finally to close the resource
        try (EsRestClient esRestClient = EsRestClient.createInstance(connectionConfig)) {
            return esRestClient.getFieldTypeMapping(index, source);
        }
    }
}
