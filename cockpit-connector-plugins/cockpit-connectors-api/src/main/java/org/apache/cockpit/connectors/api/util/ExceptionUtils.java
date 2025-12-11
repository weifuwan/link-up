package org.apache.cockpit.connectors.api.util;

import lombok.NonNull;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {
    private ExceptionUtils() {}

    public static String getMessage(Throwable e) {
        if (e == null) {
            return "";
        }
        try (StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw)) {
            // Output the error stack information to the printWriter
            e.printStackTrace(pw);
            pw.flush();
            sw.flush();
            return sw.toString();
        } catch (Exception e1) {
            throw new RuntimeException("Failed to print exception logs", e1);
        }
    }

    public static Throwable getRootException(@NonNull Throwable e) {
        Throwable cause = e.getCause();
        if (cause != null) {
            return getRootException(cause);
        } else {
            return e;
        }
    }
}
