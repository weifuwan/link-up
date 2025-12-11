package org.apache.cockpit.connectors.starrocks.client.source.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Tablet implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<String> routings;
    private int version;
    private long versionHash;
    private long schemaHash;
}
