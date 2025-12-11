package org.apache.cockpit.plugin.datasource.api.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Slf4j
public class TimeUtil {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String resolveTimeVariable(String varExpression) {
        LocalDateTime now = LocalDateTime.now();

        switch (varExpression) {
            case "0s": // 今天的0点0分0秒
                return now.toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "24s": // 今天的23点59分59秒
                return now.toLocalDate().atTime(23, 59, 59).format(DATE_TIME_FORMATTER);
            case "-1d":
                return now.minusDays(1).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "+1d":
                return now.plusDays(1).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "-7d":
                return now.minusDays(7).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "-30d":
                return now.minusDays(30).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "start_of_month":
                return now.withDayOfMonth(1).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "end_of_month":
                LocalDateTime endOfMonth = now.withDayOfMonth(now.toLocalDate().lengthOfMonth())
                        .toLocalDate().atTime(23, 59, 59);
                return endOfMonth.format(DATE_TIME_FORMATTER);
            case "start_of_year":
                return now.withDayOfYear(1).toLocalDate().atStartOfDay().format(DATE_TIME_FORMATTER);
            case "end_of_year":
                LocalDateTime endOfYear = now.withDayOfYear(now.toLocalDate().lengthOfYear())
                        .toLocalDate().atTime(23, 59, 59);
                return endOfYear.format(DATE_TIME_FORMATTER);
            default:
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(varExpression);
                    return now.format(formatter);
                } catch (Exception e) {
                    log.warn("未知的时间变量表达式: {}, 使用当前时间", varExpression);
                    return now.format(DATE_TIME_FORMATTER);
                }
        }
    }
}
