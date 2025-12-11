package org.apache.cockpit.connectors.api.util;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class JdbcUrlUtil {
    private static final Pattern URL_PATTERN =
            Pattern.compile(
                    "^(?<url>jdbc:.+?//(?<host>.+?):(?<port>\\d+?))(/(?<database>.*?))*(?<suffix>\\?.*)*$");

    private JdbcUrlUtil() {}

    public static UrlInfo getUrlInfo(String url) {
        Matcher matcher = URL_PATTERN.matcher(url);
        if (matcher.find()) {
            String urlWithoutDatabase = matcher.group("url");
            String database = matcher.group("database");
            return new UrlInfo(
                    url,
                    urlWithoutDatabase,
                    matcher.group("host"),
                    Integer.valueOf(matcher.group("port")),
                    database,
                    matcher.group("suffix"));
        }
        throw new IllegalArgumentException("The jdbc url format is incorrect: " + url);
    }

    @Data
    public static class UrlInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String origin;
        private final String urlWithoutDatabase;
        private final String host;
        private final Integer port;
        private final String suffix;
        private final String defaultDatabase;

        public UrlInfo(
                String origin,
                String urlWithoutDatabase,
                String host,
                Integer port,
                String defaultDatabase,
                String suffix) {
            this.origin = origin;
            this.urlWithoutDatabase = urlWithoutDatabase;
            this.host = host;
            this.port = port;
            this.defaultDatabase = defaultDatabase;
            this.suffix = suffix == null ? "" : suffix;
        }

        public Optional<String> getUrlWithDatabase() {
            return StringUtils.isBlank(defaultDatabase)
                    ? Optional.empty()
                    : Optional.of(urlWithoutDatabase + "/" + defaultDatabase + suffix);
        }

        public Optional<String> getDefaultDatabase() {
            return StringUtils.isBlank(defaultDatabase)
                    ? Optional.empty()
                    : Optional.of(defaultDatabase);
        }

        public String getUrlWithDatabase(String database) {
            return urlWithoutDatabase + "/" + database + suffix;
        }
    }
}
