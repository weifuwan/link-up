package org.apache.cockpit.connectors.elasticsearch.serialize.source;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Getter
@ToString
@AllArgsConstructor
public class ElasticsearchRecord {
    private Map<String, Object> doc;
    private List<String> source;

    private String tableId;
}
