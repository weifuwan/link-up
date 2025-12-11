package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class QueryBeXTablets implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String beNode;
    private final List<Long> tabletIds;
}
