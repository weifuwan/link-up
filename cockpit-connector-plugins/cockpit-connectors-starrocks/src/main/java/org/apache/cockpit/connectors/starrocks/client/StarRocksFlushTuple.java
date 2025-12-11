package org.apache.cockpit.connectors.starrocks.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@AllArgsConstructor
@Getter
@Setter
public class StarRocksFlushTuple {
    private String label;
    private Long bytes;
    private List<byte[]> rows;
}
