package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class Column {
    private String name;
    private String type;
    private String comment;
    private int precision;
    private int scale;
}
