package org.apache.cockpit.connectors.elasticsearch.dto.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/** DTO for Elasticsearch Point-in-Time search results. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PointInTimeResult {

    /** The PIT ID used for this search */
    private String pitId;

    /** Documents returned by the search */
    private List<Map<String, Object>> docs;

    /** Total number of hits matching the query */
    private long totalHits;

    /** Sort values of the last document, used for pagination with search_after */
    private Object[] searchAfter;

    /** Whether there are more results to fetch */
    private boolean hasMore;
}
