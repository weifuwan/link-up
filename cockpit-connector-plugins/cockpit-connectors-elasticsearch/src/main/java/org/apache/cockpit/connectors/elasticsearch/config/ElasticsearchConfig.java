package org.apache.cockpit.connectors.elasticsearch.config;



import lombok.Getter;
import lombok.Setter;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ElasticsearchConfig implements Serializable {

    private String index;
    private List<String> source;
    private Map<String, Object> query;
    private String scrollTime;
    private int scrollSize;
    private SearchTypeEnum searchType;
    private SearchApiTypeEnum searchApiType;
    private String sqlQuery;

    private long pitKeepAlive;
    private int pitBatchSize;
    private String pitId;
    private Object[] searchAfter;

    private CatalogTable catalogTable;

    public ElasticsearchConfig clone() {
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        elasticsearchConfig.setIndex(index);
        elasticsearchConfig.setSource(new ArrayList<>(source));
        elasticsearchConfig.setQuery(new HashMap<>(query));
        elasticsearchConfig.setScrollTime(scrollTime);
        elasticsearchConfig.setScrollSize(scrollSize);
        elasticsearchConfig.setCatalogTable(catalogTable);
        elasticsearchConfig.setSearchType(searchType);
        elasticsearchConfig.setSearchApiType(searchApiType);
        elasticsearchConfig.setSqlQuery(sqlQuery);
        elasticsearchConfig.setPitKeepAlive(pitKeepAlive);
        elasticsearchConfig.setPitBatchSize(pitBatchSize);
        elasticsearchConfig.setPitId(pitId);
        elasticsearchConfig.setSearchAfter(searchAfter != null ? searchAfter.clone() : null);

        return elasticsearchConfig;
    }
}
