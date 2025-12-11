package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class QueryInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final QueryPlan queryPlan;
    private final List<QueryBeXTablets> beXTablets;
}
