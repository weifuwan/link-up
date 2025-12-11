package org.apache.cockpit.connectors.doris.util;

public abstract class ErrorMessages {
    public static final String PARSE_NUMBER_FAILED_MESSAGE =
            "Parse '%s' to number failed. Original string is '%s'.";
    public static final String CONNECT_FAILED_MESSAGE = "Connect to doris {} failed.";
    public static final String ILLEGAL_ARGUMENT_MESSAGE =
            "argument '%s' is illegal, value is '%s'.";
    public static final String SHOULD_NOT_HAPPEN_MESSAGE = "Should not come here.";
    public static final String DORIS_INTERNAL_FAIL_MESSAGE =
            "Doris server '{}' internal failed, status is '{}', error message is '{}'";
}
