package org.apache.cockpit.connectors.influxdb.config;

import java.util.concurrent.TimeUnit;

public enum TimePrecision {
    NS("NS", TimeUnit.NANOSECONDS),
    U("U", TimeUnit.MICROSECONDS),
    MS("MS", TimeUnit.MILLISECONDS),
    S("S", TimeUnit.SECONDS),
    M("M", TimeUnit.MINUTES),
    H("H", TimeUnit.HOURS);
    private String desc;
    private TimeUnit precision;

    TimePrecision(String desc, TimeUnit precision) {
        this.desc = desc;
        this.precision = precision;
    }

    public TimeUnit getTimeUnit() {
        return this.precision;
    }

    public static TimePrecision getPrecision(String desc) {
        for (TimePrecision timePrecision : TimePrecision.values()) {
            if (desc.equals(timePrecision.desc)) {
                return timePrecision;
            }
        }
        return TimePrecision.NS;
    }
}
