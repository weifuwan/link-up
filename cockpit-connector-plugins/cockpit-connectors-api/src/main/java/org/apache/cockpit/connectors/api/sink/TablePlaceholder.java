package org.apache.cockpit.connectors.api.sink;

import java.util.HashSet;
import java.util.Set;

public enum TablePlaceholder {

    // Placeholder ${database_name} or${database_name:default_value}
    REPLACE_DATABASE_NAME_KEY("database_name"),
    // Placeholder ${schema_name} or${schema_name:default_value}
    REPLACE_SCHEMA_NAME_KEY("schema_name"),
    // Placeholder ${schema_full_name} or${schema_full_name:default_value}
    REPLACE_SCHEMA_FULL_NAME_KEY("schema_full_name"),
    // Placeholder ${table_name} or${table_name:default_value}
    REPLACE_TABLE_NAME_KEY("table_name"),
    // Placeholder ${table_full_name} or${table_full_name:default_value}
    REPLACE_TABLE_FULL_NAME_KEY("table_full_name"),
    // Placeholder ${primary_key} or${primary_key:default_value}
    REPLACE_PRIMARY_KEY("primary_key"),
    // Placeholder ${unique_key} or${unique_key:default_value}
    REPLACE_UNIQUE_KEY("unique_key"),
    // Placeholder ${field_names} or${field_names:default_value}
    REPLACE_FIELD_NAMES_KEY("field_names");

    private static Set<String> PLACEHOLDER_KEYS = new HashSet<>();

    static {
        // O(1) complexity, using static to load all system placeholders
        for (TablePlaceholder placeholder : TablePlaceholder.values()) {
            PLACEHOLDER_KEYS.add(placeholder.getPlaceholder());
        }
    }

    private final String key;

    TablePlaceholder(String placeholder) {
        this.key = placeholder;
    }

    public String getPlaceholder() {
        return key;
    }

    public static boolean isSystemPlaceholder(String str) {
        return PLACEHOLDER_KEYS.contains(str);
    }
}
