package org.apache.cockpit.connectors.api.catalog;

public class InfoPreviewResult extends PreviewResult {
    private final String info;

    public String getInfo() {
        return info;
    }

    public InfoPreviewResult(String info) {
        super(Type.INFO);
        this.info = info;
    }

    @Override
    public String toString() {
        return info;
    }
}
