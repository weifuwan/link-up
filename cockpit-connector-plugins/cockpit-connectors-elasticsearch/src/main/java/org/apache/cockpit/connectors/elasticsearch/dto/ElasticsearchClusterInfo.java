package org.apache.cockpit.connectors.elasticsearch.dto;


import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.cockpit.connectors.elasticsearch.constant.ElasticsearchVersion;

@Getter
@Builder
@ToString
public class ElasticsearchClusterInfo {
    private String distribution;
    private String clusterVersion;

    public ElasticsearchVersion getElasticsearchVersion() {
        return ElasticsearchVersion.get(clusterVersion);
    }

    public boolean isOpensearch() {
        return !Strings.isNullOrEmpty(distribution) && "opensearch".equalsIgnoreCase(distribution);
    }
}
