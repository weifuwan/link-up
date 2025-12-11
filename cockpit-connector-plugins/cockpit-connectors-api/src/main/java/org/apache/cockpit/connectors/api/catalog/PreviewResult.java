package org.apache.cockpit.connectors.api.catalog;

public abstract class PreviewResult {

    private final Type type;

    public PreviewResult(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        SQL,
        INFO,
        OTHER
    }
}
