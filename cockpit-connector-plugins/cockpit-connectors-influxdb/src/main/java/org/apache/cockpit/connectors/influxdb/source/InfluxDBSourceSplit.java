package org.apache.cockpit.connectors.influxdb.source;


import org.apache.cockpit.connectors.api.source.SourceSplit;

public class InfluxDBSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 7936658588681424786L;
    private final String splitId;

    private final String query;

    public InfluxDBSourceSplit(String splitId, String query) {
        this.query = query;
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getQuery() {
        return query;
    }
}
