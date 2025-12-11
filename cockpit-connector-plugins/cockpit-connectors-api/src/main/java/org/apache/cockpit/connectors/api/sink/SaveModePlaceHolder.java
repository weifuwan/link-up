package org.apache.cockpit.connectors.api.sink;

import java.util.Arrays;
import java.util.Optional;

public enum SaveModePlaceHolder {
    ROWTYPE_PRIMARY_KEY("rowtype_primary_key", "primary keys"),
    ROWTYPE_UNIQUE_KEY("rowtype_unique_key", "unique keys"),
    ROWTYPE_DUPLICATE_KEY("rowtype_duplicate_key", "duplicate keys"),
    ROWTYPE_FIELDS("rowtype_fields", "fields"),
    TABLE("table", "table"),
    DATABASE("database", "database"),
    COMMENT("comment", "comment"),
    /** @deprecated instead by {@link #TABLE} todo remove this enum */
    @Deprecated
    TABLE_NAME("table_name", "table name");

    private String keyValue;
    private String display;

    private static final String REPLACE_PLACE_HOLDER = "\\$\\{%s\\}";
    private static final String PLACE_HOLDER = "${%s}";

    SaveModePlaceHolder(String keyValue, String display) {
        this.keyValue = keyValue;
        this.display = display;
    }

    public static String getDisplay(String placeholder) {
        Optional<SaveModePlaceHolder> saveModePlaceHolderEnumOptional =
                Arrays.stream(SaveModePlaceHolder.values())
                        .filter(
                                saveModePlaceHolderEnum ->
                                        placeholder.equals(
                                                saveModePlaceHolderEnum.getPlaceHolder()))
                        .findFirst();
        if (saveModePlaceHolderEnumOptional.isPresent()) {
            return saveModePlaceHolderEnumOptional.get().display;
        }
        throw new RuntimeException(String.format("Not support the placeholder: %s", placeholder));
    }

    public String getPlaceHolderKey() {
        return this.keyValue;
    }

    public String getPlaceHolder() {
        return String.format(PLACE_HOLDER, getPlaceHolderKey());
    }

    public String getReplacePlaceHolder() {
        return String.format(REPLACE_PLACE_HOLDER, getPlaceHolderKey());
    }
}
