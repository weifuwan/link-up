package org.apache.cockpit.connectors.api.util;

import java.util.Objects;

public class DefaultValueUtils {
    public static boolean isMysqlSpecialDefaultValue(Object defaultValue) {
        if (Objects.isNull(defaultValue)) {
            return false;
        }
        String defaultValueStr = defaultValue.toString();
        return defaultValueStr.matches(
                        "(?i)^(CURRENT_TIMESTAMP|CURRENT_TIME|CURRENT_DATE)\\(?\\d*\\)?$")
                || defaultValueStr.equalsIgnoreCase("TRUE")
                || defaultValueStr.equalsIgnoreCase("FALSE");
    }
}
