package org.apache.cockpit.connectors.api.common.metrics;

import java.io.Serializable;
import java.util.Arrays;

public final class RawJobMetrics implements Serializable {

    private long timestamp;
    private byte[] blob;

    RawJobMetrics() {}

    private RawJobMetrics(long timestamp, byte[] blob) {
        this.timestamp = timestamp;
        this.blob = blob;
    }

    public static RawJobMetrics empty() {
        return of(null);
    }

    public static RawJobMetrics of(byte[] blob) {
        return new RawJobMetrics(System.currentTimeMillis(), blob);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getBlob() {
        return blob;
    }

    @Override
    public int hashCode() {
        return (int) timestamp * 31 + Arrays.hashCode(blob);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        RawJobMetrics that;
        return Arrays.equals(blob, (that = (RawJobMetrics) obj).blob)
                && this.timestamp == that.timestamp;
    }

    @Override
    public String toString() {
        return Arrays.toString(blob) + " @ " + timestamp;
    }
}
