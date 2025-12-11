package org.apache.cockpit.connectors.clickhouse.config;


import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.clickhouse.exception.ClickhouseConnectorException;

public enum ClickhouseFileCopyMethod {
    SCP("scp"),
    RSYNC("rsync"),
    ;
    private final String name;

    ClickhouseFileCopyMethod(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ClickhouseFileCopyMethod from(String name) {
        for (ClickhouseFileCopyMethod clickhouseFileCopyMethod :
                ClickhouseFileCopyMethod.values()) {
            if (clickhouseFileCopyMethod.getName().equalsIgnoreCase(name)) {
                return clickhouseFileCopyMethod;
            }
        }
        throw new ClickhouseConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Unknown ClickhouseFileCopyMethod: " + name);
    }
}
