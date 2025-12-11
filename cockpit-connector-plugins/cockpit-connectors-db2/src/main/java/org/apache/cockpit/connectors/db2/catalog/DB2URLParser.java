package org.apache.cockpit.connectors.db2.catalog;

import org.apache.cockpit.connectors.api.util.JdbcUrlUtil;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Henry
 * @date 2025/12/4
 */
public class DB2URLParser {
    private static final Pattern DB2_URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:db2://(?<host>[^:]+):(?<port>\\d+)/)(?<database>.+?)((?<suffix>\\?.*)?)$");

    public static JdbcUrlUtil.UrlInfo parse(String url) {
        Matcher matcher = DB2_URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String urlWithoutDatabase = matcher.group("url");
            String host = matcher.group("host");
            Integer port = Integer.valueOf(matcher.group("port"));
            String database = matcher.group("database");
            String suffix = Optional.ofNullable(matcher.group("suffix")).orElse("");
            return new JdbcUrlUtil.UrlInfo(url, urlWithoutDatabase, host, port, database, suffix);
        }
        return new JdbcUrlUtil.UrlInfo(url, url, null, null, "temp", null);
    }
}
