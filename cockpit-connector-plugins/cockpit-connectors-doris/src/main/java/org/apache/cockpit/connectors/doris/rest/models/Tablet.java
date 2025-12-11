package org.apache.cockpit.connectors.doris.rest.models;

import java.util.List;
import java.util.Objects;

public class Tablet {
    private List<String> routings;
    private int version;
    private long versionHash;
    private long schemaHash;

    public List<String> getRoutings() {
        return routings;
    }

    public void setRoutings(List<String> routings) {
        this.routings = routings;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getVersionHash() {
        return versionHash;
    }

    public void setVersionHash(long versionHash) {
        this.versionHash = versionHash;
    }

    public long getSchemaHash() {
        return schemaHash;
    }

    public void setSchemaHash(long schemaHash) {
        this.schemaHash = schemaHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tablet tablet = (Tablet) o;
        return version == tablet.version
                && versionHash == tablet.versionHash
                && schemaHash == tablet.schemaHash
                && Objects.equals(routings, tablet.routings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routings, version, versionHash, schemaHash);
    }
}
