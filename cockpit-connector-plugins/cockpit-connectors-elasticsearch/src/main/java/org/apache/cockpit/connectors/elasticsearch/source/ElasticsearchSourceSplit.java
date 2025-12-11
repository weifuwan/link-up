package org.apache.cockpit.connectors.elasticsearch.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchConfig;

@ToString
@AllArgsConstructor
public class ElasticsearchSourceSplit implements SourceSplit {

    private static final long serialVersionUID = -1L;

    private String splitId;

    @Getter private ElasticsearchConfig elasticsearchConfig;

    public SeaTunnelRowType getSeaTunnelRowType() {
        return elasticsearchConfig.getCatalogTable().getSeaTunnelRowType();
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
