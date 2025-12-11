package org.apache.cockpit.connectors.api.common.metrics;

public enum Unit {
    /** Size, counter, represented in bytes */
    BYTES,
    /** Timestamp or duration represented in ms */
    MS,
    /** An integer in range 0..100 */
    PERCENT,
    /** Number of items: size, counter... */
    COUNT,
    /** 0 or 1 */
    BOOLEAN,
    /** 0..n, ordinal of an enum */
    ENUM,
}
