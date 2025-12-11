
package org.apache.cockpit.common.constant;

import lombok.extern.slf4j.Slf4j;

import java.util.TimeZone;

@Slf4j
public class SystemConstants {

    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

    static {
        log.info("init timezone: {}", DEFAULT_TIME_ZONE);
    }
}
