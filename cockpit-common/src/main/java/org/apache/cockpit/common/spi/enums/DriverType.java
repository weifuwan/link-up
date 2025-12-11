
package org.apache.cockpit.common.spi.enums;

import lombok.Getter;

@Getter
public enum DriverType {

    POSTGRESQL(0, "PostgreSQL Driver"),
    OPEN_GAUSS(1, "OpenGauss Driver");

    DriverType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    private final int code;
    private final String desc;

}
