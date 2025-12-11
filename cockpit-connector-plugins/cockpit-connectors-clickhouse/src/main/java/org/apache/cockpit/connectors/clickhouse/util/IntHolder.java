package org.apache.cockpit.connectors.clickhouse.util;

import java.io.Serializable;

public class IntHolder implements Serializable {

    private static final long serialVersionUID = -1L;

    private int value;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
