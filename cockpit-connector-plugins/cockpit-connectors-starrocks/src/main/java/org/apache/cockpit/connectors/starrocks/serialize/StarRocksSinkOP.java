package org.apache.cockpit.connectors.starrocks.serialize;


import org.apache.cockpit.connectors.api.type.RowKind;

/**
 * Reference
 * https://github.com/StarRocks/starrocks/blob/main/docs/loading/Load_to_Primary_Key_tables.md#upsert-and-delete
 */
public enum StarRocksSinkOP {
    UPSERT,
    DELETE;

    public static final String COLUMN_KEY = "__op";

    static StarRocksSinkOP parse(RowKind kind) {
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                return UPSERT;
            case DELETE:
            case UPDATE_BEFORE:
                return DELETE;
            default:
                throw new RuntimeException("Unsupported row kind.");
        }
    }
}
