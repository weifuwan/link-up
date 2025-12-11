package org.apache.cockpit.connectors.api.catalog;

public class SQLPreviewResult extends PreviewResult {

    private final String sql;

    public String getSql() {
        return sql;
    }

    public SQLPreviewResult(String sql) {
        super(Type.SQL);
        this.sql = sql;
    }

    @Override
    public String toString() {
        return sql;
    }
}
