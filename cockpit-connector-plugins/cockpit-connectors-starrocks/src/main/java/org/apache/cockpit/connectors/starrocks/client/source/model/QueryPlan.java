package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class QueryPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private int status;

    @JsonProperty("opaqued_query_plan")
    private String queryPlan;

    private Map<String, Tablet> partitions;
}
