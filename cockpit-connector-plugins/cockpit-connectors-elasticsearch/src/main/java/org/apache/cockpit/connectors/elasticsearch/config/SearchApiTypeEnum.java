package org.apache.cockpit.connectors.elasticsearch.config;

public enum SearchApiTypeEnum {
    /** Use Scroll API for pagination */
    SCROLL,

    /** Use Point-in-Time (PIT) API for pagination */
    PIT
}
