package org.apache.cockpit.plugin.datasource.elasticsearch.enums;


public enum ConnectionStrategy {
    REST_HIGH_LEVEL_CLIENT,  // 7.x及以下
    NEW_JAVA_CLIENT,         // 8.x
    HTTP                     // 通用fallback
}
