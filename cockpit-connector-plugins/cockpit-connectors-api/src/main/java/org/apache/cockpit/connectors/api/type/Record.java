package org.apache.cockpit.connectors.api.type;

import java.io.Serializable;


public class Record<T> implements Serializable {

    private final T data;

    public Record(T data) {
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
