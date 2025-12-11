package org.apache.cockpit.connectors.elasticsearch.dto.source;


import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ScrollResult {

    private String scrollId;
    private List<Map<String, Object>> docs;
    private JsonNode columnNodes;
}
