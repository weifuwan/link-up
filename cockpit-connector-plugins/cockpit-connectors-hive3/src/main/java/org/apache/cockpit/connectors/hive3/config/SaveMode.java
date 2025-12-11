package org.apache.cockpit.connectors.hive3.config;

import lombok.NonNull;

import java.io.Serializable;
import java.util.Locale;

public enum SaveMode implements Serializable {
    APPEND(),
    OVERWRITE(),
    IGNORE(),
    ERROR();

    public static SaveMode fromStr(@NonNull String str) {
        return SaveMode.valueOf(str.toUpperCase(Locale.ROOT));
    }
}
