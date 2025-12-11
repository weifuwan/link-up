package org.apache.cockpit.connectors.api.util;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TimeUtils {
    private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP =
            new HashMap<Formatter, DateTimeFormatter>();

    static {
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS.value));
        FORMATTER_MAP.put(
                Formatter.HH_MM_SS_SSS, DateTimeFormatter.ofPattern(Formatter.HH_MM_SS_SSS.value));
    }

    public static LocalTime parse(String time, Formatter formatter) {
        return LocalTime.parse(time, FORMATTER_MAP.get(formatter));
    }

    public static LocalTime parse(String dateTime) {
        return LocalTime.parse(dateTime, FORMATTER_MAP.get(matchTimeFormatter(dateTime)));
    }

    public static final Pattern[] PATTERN_ARRAY =
            new Pattern[] {
                Pattern.compile("\\d{2}:\\d{2}:\\d{2}"),
                Pattern.compile("\\d{2}:\\d{2}:\\d{2}.\\d{3}"),
            };

    public static Formatter matchTimeFormatter(String dateTime) {
        for (int j = 0; j < PATTERN_ARRAY.length; j++) {
            if (PATTERN_ARRAY[j].matcher(dateTime).matches()) {
                Formatter dateTimeFormatter = Time_FORMATTER_MAP.get(PATTERN_ARRAY[j]);
                return dateTimeFormatter;
            }
        }
        return null;
    }

    public static final Map<Pattern, Formatter> Time_FORMATTER_MAP = new HashMap();

    static {
        Time_FORMATTER_MAP.put(PATTERN_ARRAY[0], Formatter.parse(Formatter.HH_MM_SS.value));
        Time_FORMATTER_MAP.put(PATTERN_ARRAY[1], Formatter.parse(Formatter.HH_MM_SS_SSS.value));
    }

    public static String toString(LocalTime time, Formatter formatter) {
        return time.format(FORMATTER_MAP.get(formatter));
    }

    public enum Formatter {
        HH_MM_SS("HH:mm:ss"),
        HH_MM_SS_SSS("HH:mm:ss.SSS");
        private final String value;

        Formatter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Formatter parse(String format) {
            Formatter[] formatters = Formatter.values();
            for (Formatter formatter : formatters) {
                if (formatter.getValue().equals(format)) {
                    return formatter;
                }
            }
            String errorMsg = String.format("Illegal format [%s]", format);
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
