package org.apache.cockpit.common.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class DateTimeUtils {

    // 默认格式，可以根据需要调整
    public static final String DEFAULT_FORMAT = "yyyyMMddHHmmssSSS";

    /**
     * 将毫秒时间戳转换为指定格式的字符串。
     *
     * @param timestampMillis 毫秒时间戳，通常来自 System.currentTimeMillis()
     * @param pattern 格式化模式，如 "yyyy-MM-dd HH:mm:ss"
     * @return 格式化后的时间字符串
     */
    public static String formatMillis(long timestampMillis, String pattern) {
        Objects.requireNonNull(pattern, "格式化模式不能为空");

        // 1. 将毫秒时间戳转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(timestampMillis);

        // 2. 将 Instant 转换为本地时区的 LocalDateTime
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();

        // 3. 使用 DateTimeFormatter 进行格式化
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

        return localDateTime.format(formatter);
    }

    /**
     * 将毫秒时间戳转换为默认格式的字符串。
     *
     * @param timestampMillis 毫秒时间戳
     * @return 格式化后的时间字符串，格式为 "yyyyMMddHHmmssSSS"
     */
    public static String formatMillis(long timestampMillis) {
        return formatMillis(timestampMillis, DEFAULT_FORMAT);
    }

    public static String formatMillisDefault() {
        return formatMillis(System.currentTimeMillis(), DEFAULT_FORMAT);
    }

}

