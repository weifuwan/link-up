package org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum;

public enum FieldIdeEnum {
    ORIGINAL("original"), // Original string form
    UPPERCASE("uppercase"), // Convert to uppercase
    LOWERCASE("lowercase"); // Convert to lowercase

    private final String value;

    FieldIdeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
