package org.apache.cockpit.connectors.api.util;

import java.util.Collections;

public class StringFormatUtils {
    private static final int NUM = 47;

    private StringFormatUtils() {
        // utility class can not be instantiated
    }

    public static String formatTable(Object... objects) {
        String title = objects[0].toString();
        int blankNum = (NUM - title.length()) / 2;
        int kvNum = (objects.length - 1) / 2;
        String template =
                "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(blankNum, " "))
                        + "%s"
                        + "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(kvNum, "%-26s: %19s\n"))
                        + "***********************************************\n";
        return String.format(template, objects);
    }
}
