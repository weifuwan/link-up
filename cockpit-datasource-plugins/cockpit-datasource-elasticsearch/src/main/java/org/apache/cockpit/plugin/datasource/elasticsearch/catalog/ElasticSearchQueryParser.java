package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ElasticSearchQueryParser {
    public static List<String> splitAndRemoveComment(String sql) {
        // Remove single line comments
        String noSingleLineComments = sql.replaceAll("--.*", "");

        // Remove multi-line comments
        String noMultiLineComments = noSingleLineComments.replaceAll("/\\*.*?\\*/", "");

        // Split by semicolon
        String[] queries = noMultiLineComments.split(";");

        return Arrays.stream(queries)
                .map(String::trim)
                .filter(query -> !query.isEmpty())
                .collect(Collectors.toList());
    }
}
