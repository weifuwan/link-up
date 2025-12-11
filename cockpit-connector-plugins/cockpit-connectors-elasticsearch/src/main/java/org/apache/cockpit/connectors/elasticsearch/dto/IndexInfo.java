package org.apache.cockpit.connectors.elasticsearch.dto;


import lombok.Data;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchSinkOptions;

/**
 * index config by seatunnel
 */
@Data
public class IndexInfo {

    private String index;
    private String type;
    private String[] primaryKeys;
    private String keyDelimiter;

    public IndexInfo(String index, ReadonlyConfig config) {
        this.index = index;
        type = config.get(ElasticsearchSinkOptions.INDEX_TYPE);
        if (config.getOptional(ElasticsearchSinkOptions.PRIMARY_KEYS).isPresent()) {
            primaryKeys = config.get(ElasticsearchSinkOptions.PRIMARY_KEYS).toArray(new String[0]);
        }
        keyDelimiter = config.get(ElasticsearchSinkOptions.KEY_DELIMITER);
    }
}
